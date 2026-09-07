#include <catch2/catch_test_macros.hpp>
#include <components/context/context.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_page.hpp>

#include "disk_test_helpers.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// The open path must not let a real failure collapse into the value a legitimate empty state also
// produces (a zero wal id, a '\0' relkind, a `false` create, a `0` append).

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;

namespace {
    std::string refusal_dir() {
        static std::string p = "/tmp/test_otterbrix_open_refusals_" + std::to_string(::getpid());
        return p;
    }
    void cleanup_refusal_dir() { std::filesystem::remove_all(refusal_dir()); }

    std::filesystem::path otbx_at(const std::filesystem::path& base, catalog::oid_t db_oid, catalog::oid_t tbl_oid) {
        return base / std::to_string(static_cast<unsigned>(db_oid)) / std::to_string(static_cast<unsigned>(tbl_oid)) /
               "table.otbx";
    }

    // Does not bootstrap in the ctor — several cases need the pre-bootstrap state, opening the same directory twice.
    struct open_fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit open_fixture(const std::filesystem::path& base)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = base;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}

        ~open_fixture() {
            // Destroy the manager first — its dtor joins the loop thread, which may still enqueue onto the scheduler.
            manager.reset();
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {}};
        }

        void checkpoint(services::wal::id_t wal_id) {
            auto [_, cf] = actor_zeta::otterbrix::send(manager->address(),
                                                       &manager_disk_t::checkpoint_all,
                                                       session_id_t{},
                                                       wal_id,
                                                       std::numeric_limits<uint64_t>::max());
            for (int i = 0; i < 100000 && !cf.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(cf.is_ready());
            auto sealed = std::move(cf).take_ready();
            REQUIRE(sealed <= wal_id);
        }
    };

    // The T3 interposer seam is process-wide; filter by path so only the named table's .otbx is wrapped.
    class path_fault_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        path_fault_scope_t(otterbrix_test::fault_plan_t& plan, std::string path_marker)
            : plan_(plan)
            , marker_(std::move(path_marker)) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~path_fault_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(marker_) == std::string::npos) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan_);
        }

    private:
        otterbrix_test::fault_plan_t& plan_;
        std::string marker_;
    };

    void append_rows(open_fixture& fx, catalog::oid_t table_oid, uint64_t count) {
        std::pmr::vector<components::types::complex_logical_type> types(&fx.resource);
        components::types::complex_logical_type t{components::types::logical_type::BIGINT};
        t.set_alias("value");
        types.push_back(std::move(t));
        components::vector::data_chunk_t chunk(&fx.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.set_value(0, i, static_cast<std::int64_t>(i));
        }
        std::pmr::vector<components::vector::data_chunk_t> batch(&fx.resource);
        batch.emplace_back(std::move(chunk));
        components::execution_context_t append_ctx{session_id_t{},
                                                   components::table::transaction_data{0, 0},
                                                   {},
                                                   table_oid};
        auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
    }

    uint64_t rows_where_value_is(open_fixture& fx, catalog::oid_t table_oid, std::int64_t value) {
        std::pmr::vector<std::uint64_t> key_cols(&fx.resource);
        key_cols.emplace_back(0);
        std::pmr::vector<components::types::logical_value_t> key_vals(&fx.resource);
        key_vals.emplace_back(&fx.resource, value);
        auto batches = test_probe::probe_read(fx, fx.ctx(), table_oid, std::move(key_cols), std::move(key_vals));
        uint64_t total = 0;
        for (const auto& c : batches) {
            total += c.size();
        }
        return total;
    }

    std::int64_t added_at_commit_id_of(open_fixture& fx, catalog::oid_t attoid) {
        std::pmr::vector<std::uint64_t> key_cols(&fx.resource);
        key_cols.emplace_back(catalog::pg_attribute_col::attoid);
        std::pmr::vector<components::types::logical_value_t> key_vals(&fx.resource);
        key_vals.emplace_back(&fx.resource, attoid);
        auto batches = test_probe::probe_read(fx,
                                              fx.ctx(),
                                              catalog::well_known_oid::pg_attribute_table,
                                              std::move(key_cols),
                                              std::move(key_vals));
        for (const auto& c : batches) {
            for (std::uint64_t i = 0; i < c.size(); ++i) {
                auto v = c.value(catalog::pg_attribute_col::added_at_commit_id, i);
                if (!v.is_null()) {
                    return v.value<std::int64_t>();
                }
            }
        }
        return -1; // no row at all — distinct from every honest stamp, including the placeholder
    }

    core::error_t backfill(open_fixture& fx, std::initializer_list<catalog::oid_t> attoids, std::uint64_t commit_id) {
        std::pmr::vector<components::pg_attribute_commit_id_backfill_t> markers(&fx.resource);
        for (auto a : attoids) {
            components::pg_attribute_commit_id_backfill_t m;
            m.attoid = a;
            m.kind = components::pg_attribute_commit_id_backfill_t::kind_t::added_at;
            markers.push_back(std::move(m));
        }
        return fx.invoke(&manager_disk_t::update_pg_attribute_commit_id_fields,
                         disk_test_helpers::auto_ctx(),
                         std::move(markers),
                         commit_id);
    }
} // namespace

// A short sidecar is a crash image, not corruption; refusing the whole database over it is reserved
// for system tables (case 6).
TEST_CASE("services::disk::open::an_unreadable_sidecar_is_not_never_checkpointed") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    constexpr uint64_t kRows = 7;
    constexpr auto kCheckpointId = services::wal::id_t{40};
    catalog::oid_t table_oid = catalog::INVALID_OID;
    catalog::oid_t ns_oid = catalog::INVALID_OID;

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_sidecar");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_oid = test_create_table(fx, ns_oid, "t_sidecar", cols);
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  ns_oid,
                  cols,
                  /*is_computed=*/false);
        append_rows(fx, table_oid, kRows);
        fx.checkpoint(kCheckpointId);
    }

    const auto otbx = otbx_at(base, ns_oid, table_oid);
    const auto sidecar = std::filesystem::path(otbx.string() + ".wal_id");
    REQUIRE(std::filesystem::exists(otbx));
    REQUIRE(std::filesystem::exists(sidecar));
    REQUIRE(std::filesystem::file_size(sidecar) == sizeof(std::uint64_t));

    std::filesystem::resize_file(sidecar, 3);
    REQUIRE(std::filesystem::file_size(sidecar) == 3);

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        REQUIRE_FALSE(fx.manager->has_storage(table_oid));

        auto peeked = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
        INFO("a sidecar that cannot be read must not answer 'never checkpointed'");
        CHECK(peeked.has_error());

        auto load_err = fx.manager->load_storage_for_wal_replay_sync(table_oid, ns_oid);
        INFO("the .otbx opened fine; a sidecar that did not is no reason to leave the table unloaded");
        CHECK_FALSE(load_err.contains_error());
        CHECK(fx.manager->has_storage(table_oid));

        auto after_load = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
        INFO("a loaded table whose floor could not be read must not answer 0 either");
        CHECK(after_load.has_error());
    }

    {
        open_fixture fx(base);
        REQUIRE(std::filesystem::file_size(sidecar) == 3);
        REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fx.manager->restore_oid_generator_sync());
        REQUIRE_NOTHROW(fx.manager->load_user_table_storages_sync());
        auto unclosed = fx.manager->rehydrate_missing_user_storages_sync();
        REQUIRE_FALSE(unclosed.has_error());
        CHECK(unclosed.value() == 0);

        auto ns = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("ns_sidecar"));
        REQUIRE_FALSE(ns.has_error());
        CHECK(ns.value().found);

        CHECK(std::filesystem::exists(otbx));
        CHECK(std::filesystem::file_size(sidecar) == 3);

        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
        auto gone = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t_sidecar"));
        INFO("a table whose sidecar rotted must still be droppable");
        CHECK_FALSE(gone.found);
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::replayed_rows_with_nowhere_to_land_are_refused") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_replay");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    auto table_oid = test_create_table(fx, ns_oid, "t_lost", cols);
    REQUIRE(table_oid >= FIRST_USER_OID);
    REQUIRE_FALSE(fx.manager->has_storage(table_oid));

    std::pmr::vector<components::types::complex_logical_type> types(&fx.resource);
    components::types::complex_logical_type t{components::types::logical_type::BIGINT};
    t.set_alias("value");
    types.push_back(std::move(t));
    components::vector::data_chunk_t chunk(&fx.resource, types, 5);
    chunk.set_cardinality(5);
    for (uint64_t i = 0; i < 5; i++) {
        chunk.set_value(0, i, static_cast<std::int64_t>(i));
    }

    auto appended = fx.manager->direct_append_sync(table_oid, chunk);
    INFO("five committed rows replayed into a table with no storage must be reported, not returned as row 0");
    CHECK(appended.has_error());

    const auto otbx = otbx_at(base, ns_oid, table_oid);
    std::filesystem::create_directories(otbx.parent_path());
    REQUIRE_FALSE(fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false)
                      .contains_error());
    REQUIRE(fx.manager->has_storage(table_oid));
    components::vector::data_chunk_t empty(&fx.resource, types, 1);
    empty.set_cardinality(0);
    auto nothing = fx.manager->direct_append_sync(table_oid, empty);
    CHECK_FALSE(nothing.has_error());

    auto landed = fx.manager->direct_append_sync(table_oid, chunk);
    REQUIRE_FALSE(landed.has_error());
    CHECK(landed.value() == 0);
    auto rows = read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(rows == 5);

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_create_that_failed_is_not_reported_as_a_duplicate") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_create");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "t_create", cols);
    const auto otbx = otbx_at(base, ns_oid, table_oid);
    std::filesystem::create_directories(otbx.parent_path());

    {
        // fail_writes_from compares with >=, so 1 refuses every write on the handle, including the header write.
        otterbrix_test::fault_plan_t plan;
        plan.fail_writes_from = 1;
        path_fault_scope_t scope(plan, "/" + std::to_string(static_cast<unsigned>(table_oid)) + "/");

        auto err = fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false);
        INFO("a create whose first write was refused must not be reported as an already-owned oid");
        CHECK(err.contains_error());
        CHECK_FALSE(fx.manager->has_storage(table_oid));
    }

    if (std::filesystem::exists(otbx)) {
        std::error_code stump_ec;
        INFO("a create that was refused must not leave a stump that refuses the retry");
        CHECK(std::filesystem::file_size(otbx, stump_ec) != 0);
    }
    std::error_code rm_ec;
    std::filesystem::remove(otbx, rm_ec);
    REQUIRE_FALSE(fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false)
                      .contains_error());
    REQUIRE(fx.manager->has_storage(table_oid));

    auto dup = fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false);
    INFO("an oid the owning agent already holds is a legitimate skip, not an error");
    CHECK_FALSE(dup.contains_error());

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::an_unreadable_relkind_is_not_a_regular_table") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t doc_oid = catalog::INVALID_OID;
    catalog::oid_t ns_oid = catalog::INVALID_OID;
    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_doc");
        doc_oid = test_create_computing_table(fx, ns_oid, "t_doc");
        REQUIRE(doc_oid >= FIRST_USER_OID);
        auto kind = fx.manager->relkind_for_oid_sync(doc_oid);
        REQUIRE_FALSE(kind.has_error());
        REQUIRE(kind.value() == catalog::relkind::computed);
        fx.checkpoint(services::wal::id_t{50});
    }

    {
        open_fixture fx(base);
        auto kind = fx.manager->relkind_for_oid_sync(doc_oid);
        INFO("pg_class not being loaded must not answer with a relkind");
        CHECK(kind.has_error());
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        auto unknown = fx.manager->relkind_for_oid_sync(catalog::oid_t{FIRST_USER_OID + 9911});
        REQUIRE_FALSE(unknown.has_error());
        CHECK(unknown.value() == '\0');
        auto kind = fx.manager->relkind_for_oid_sync(doc_oid);
        REQUIRE_FALSE(kind.has_error());
        CHECK(kind.value() == catalog::relkind::computed);
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::rehydrate_states_the_divergence_it_cannot_close") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_rehydrate");

    const auto* cls_def = find_system_table(well_known_oid::pg_class_table);
    REQUIRE(cls_def != nullptr);
    auto orphan_oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
    REQUIRE(orphan_oids.size() == 1);
    const catalog::oid_t orphan = orphan_oids[0];
    {
        std::pmr::vector<components::types::complex_logical_type> types(&fx.resource);
        for (const auto& c : cls_def->columns) {
            types.push_back(c.type());
        }
        components::vector::data_chunk_t row(&fx.resource, types, 1);
        row.set_cardinality(1);
        row.set_value(0, 0, static_cast<std::uint32_t>(orphan));
        row.set_value(1, 0, std::string_view("t_orphan"));
        row.set_value(2, 0, static_cast<std::uint32_t>(ns_oid));
        row.set_value(3, 0, std::string_view("r"));
        row.set_value(4, 0, std::string_view("d"));
        auto rng = append_ok(
            fx.invoke(&manager_disk_t::append_pg_catalog_row, auto_ctx(), well_known_oid::pg_class_table, std::move(row)));
        std::vector<components::pg_catalog_append_range_t> appends{std::move(rng)};
        fx.invoke(&manager_disk_t::storage_publish_commits, rebuild_ctx(), std::uint64_t{1000}, std::move(appends));
    }

    REQUIRE_FALSE(fx.manager->has_storage(orphan));
    {
        auto first = fx.manager->rehydrate_missing_user_storages_sync();
        REQUIRE_FALSE(first.has_error());
        REQUIRE(first.value() == 1);
    }

    CHECK_FALSE(fx.manager->has_storage(orphan));
    CHECK_FALSE(std::filesystem::exists(otbx_at(base, ns_oid, orphan)));

    auto unresolved = fx.manager->rehydrate_missing_user_storages_sync();
    REQUIRE_FALSE(unresolved.has_error());
    INFO("a table the rehydrate walk could not close must be counted, not skipped in silence");
    CHECK(unresolved.value() == 1);

    REQUIRE_NOTHROW(fx.manager->load_user_table_storages_sync());
    REQUIRE_NOTHROW(test_drop_table(fx, orphan));
    auto closed = fx.manager->rehydrate_missing_user_storages_sync();
    REQUIRE_FALSE(closed.has_error());
    CHECK(closed.value() == 0);

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::an_unreadable_system_table_sidecar_is_not_a_brick") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    const auto sys_db = catalog::well_known_oid::main_database;
    const auto short_sidecar =
        std::filesystem::path(otbx_at(base, sys_db, well_known_oid::pg_class_table).string() + ".wal_id");
    const auto zero_sidecar =
        std::filesystem::path(otbx_at(base, sys_db, well_known_oid::pg_namespace_table).string() + ".wal_id");

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fx, "ns_sys");
        REQUIRE(ns_oid != catalog::INVALID_OID);
        fx.checkpoint(services::wal::id_t{60});
    }
    REQUIRE(std::filesystem::file_size(short_sidecar) == sizeof(std::uint64_t));
    REQUIRE(std::filesystem::file_size(zero_sidecar) == sizeof(std::uint64_t));

    std::filesystem::resize_file(short_sidecar, 3);
    std::filesystem::resize_file(zero_sidecar, 0);

    {
        open_fixture fx(base);
        INFO("a system table whose sidecar cannot be read must not cost the database its start");
        REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
        REQUIRE(fx.manager->has_storage(well_known_oid::pg_class_table));
        REQUIRE(fx.manager->has_storage(well_known_oid::pg_namespace_table));

        auto short_floor = fx.manager->peek_checkpoint_wal_id_from_disk(well_known_oid::pg_class_table, sys_db);
        CHECK(short_floor.has_error());
        auto zero_floor = fx.manager->peek_checkpoint_wal_id_from_disk(well_known_oid::pg_namespace_table, sys_db);
        CHECK(zero_floor.has_error());

        auto ns = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("ns_sys"));
        REQUIRE_FALSE(ns.has_error());
        CHECK(ns.value().found);

        CHECK(std::filesystem::file_size(short_sidecar) == 3);
        CHECK(std::filesystem::file_size(zero_sidecar) == 0);
    }

    cleanup_refusal_dir();
}


TEST_CASE("services::disk::open::rehydrate_does_not_create_over_a_file_that_did_not_load") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t table_oid = catalog::INVALID_OID;
    catalog::oid_t ns_oid = catalog::INVALID_OID;
    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_rotten");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_oid = test_create_table(fx, ns_oid, "t_rotten", cols);
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  ns_oid,
                  cols,
                  /*is_computed=*/false);
        append_rows(fx, table_oid, 5);
        fx.checkpoint(services::wal::id_t{70});
    }

    const auto otbx = otbx_at(base, ns_oid, table_oid);
    const auto size_before = std::filesystem::file_size(otbx);
    REQUIRE(size_before > components::table::storage::BLOCK_START);
    {
        std::fstream f(otbx, std::ios::binary | std::ios::in | std::ios::out);
        REQUIRE(f.is_open());
        std::vector<char> junk(512, static_cast<char>(0xAB));
        f.write(junk.data(), static_cast<std::streamsize>(junk.size()));
        f.close();
    }

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    REQUIRE_NOTHROW(fx.manager->load_user_table_storages_sync());
    REQUIRE_FALSE(fx.manager->has_storage(table_oid));

    auto unclosed = fx.manager->rehydrate_missing_user_storages_sync();
    REQUIRE_FALSE(unclosed.has_error());
    INFO("a table whose file is present and did not load must be counted, not rebuilt over");
    CHECK(unclosed.value() == 1);
    CHECK_FALSE(fx.manager->has_storage(table_oid));
    CHECK(std::filesystem::exists(otbx));
    CHECK(std::filesystem::file_size(otbx) == size_before);

    cleanup_refusal_dir();
}


TEST_CASE("services::disk::open::a_rehydrate_walk_that_could_not_run_says_so") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    {
        open_fixture fx(base);
        auto r = fx.manager->rehydrate_missing_user_storages_sync();
        INFO("a walk that could not read pg_class must not report the count of a clean start");
        CHECK(r.has_error());
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fx, "ns_walk");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto table_oid = test_create_table(fx, ns_oid, "t_walk", cols);
        fx.invoke(&manager_disk_t::create_storage_disk, session_id_t{}, table_oid, ns_oid, cols, /*is_computed=*/false);
        REQUIRE(fx.manager->has_storage(table_oid));
        auto r = fx.manager->rehydrate_missing_user_storages_sync();
        REQUIRE_FALSE(r.has_error());
        CHECK(r.value() == 0);
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_sidecar_that_cannot_be_located_is_not_a_sidecar_that_is_absent") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_peek");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "t_peek", cols);

    auto absent = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
    REQUIRE_FALSE(absent.has_error());
    CHECK(absent.value() == services::wal::id_t{0});

    auto no_table = fx.manager->peek_checkpoint_wal_id_from_disk(catalog::INVALID_OID, ns_oid);
    INFO("there is no table to answer about");
    CHECK(no_table.has_error());

    auto no_namespace = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, catalog::INVALID_OID);
    INFO("a namespace the catalog does not name gives no directory to look in");
    CHECK(no_namespace.has_error());

    open_fixture pathless(std::filesystem::path{});
    auto no_path = pathless.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
    INFO("an empty disk path names no directory a sidecar could live in");
    CHECK(no_path.has_error());

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::an_unreadable_relkind_does_not_open_a_document_table_as_regular") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t doc_oid = catalog::INVALID_OID;
    catalog::oid_t ns_oid = catalog::INVALID_OID;
    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_docload");
        doc_oid = test_create_computing_table(fx, ns_oid, "t_docload");
        REQUIRE(doc_oid >= FIRST_USER_OID);
        std::vector<components::table::column_definition_t> no_cols;
        fx.invoke(&manager_disk_t::create_storage_disk, session_id_t{}, doc_oid, ns_oid, no_cols, /*is_computed=*/true);
        REQUIRE(fx.manager->has_storage(doc_oid));
        append_rows(fx, doc_oid, 4);
        fx.checkpoint(services::wal::id_t{80});
    }

    const auto otbx = otbx_at(base, ns_oid, doc_oid);
    REQUIRE(std::filesystem::file_size(otbx) > components::table::storage::BLOCK_START);

    open_fixture fx(base);
    auto err = fx.manager->load_storage_for_wal_replay_sync(doc_oid, ns_oid);
    INFO("a relkind that could not be read must not decide the table is not a document one");
    CHECK(err.contains_error());
    CHECK_FALSE(fx.manager->has_storage(doc_oid));
    CHECK(std::filesystem::exists(otbx));

    cleanup_refusal_dir();
}


TEST_CASE("services::disk::open::a_refused_sidecar_publish_leaves_no_staging_file") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_publish");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "t_publish", cols);
    fx.invoke(&manager_disk_t::create_storage_disk, session_id_t{}, table_oid, ns_oid, cols, /*is_computed=*/false);
    append_rows(fx, table_oid, 3);
    fx.checkpoint(services::wal::id_t{100});

    const auto otbx = otbx_at(base, ns_oid, table_oid);
    const auto sidecar = std::filesystem::path(otbx.string() + ".wal_id");
    const auto staging = std::filesystem::path(sidecar.string() + ".tmp");
    REQUIRE(std::filesystem::file_size(sidecar) == sizeof(std::uint64_t));
    std::filesystem::remove(sidecar);
    REQUIRE(std::filesystem::create_directory(sidecar));

    append_rows(fx, table_oid, 3);
    fx.checkpoint(services::wal::id_t{200});

    INFO("a publish that could not complete must not leave its staging file in the table's namespace");
    CHECK_FALSE(std::filesystem::exists(staging));

    std::error_code ec;
    std::filesystem::remove_all(sidecar, ec);
    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_replayed_update_that_lost_a_value_restores_the_rest_and_reports") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t ns_oid = catalog::INVALID_OID;
    catalog::oid_t table_oid = catalog::INVALID_OID;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_replay_upd");
        table_oid = test_create_table(fx, ns_oid, "t_upd", cols);
        const auto otbx = otbx_at(base, ns_oid, table_oid);
        std::filesystem::create_directories(otbx.parent_path());
        REQUIRE_FALSE(fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false)
                          .contains_error());
        append_rows(fx, table_oid, 3);

        std::pmr::vector<components::types::complex_logical_type> wide_types(&fx.resource);
        components::types::complex_logical_type kept{components::types::logical_type::BIGINT};
        kept.set_alias("value");
        wide_types.push_back(std::move(kept));
        components::types::complex_logical_type ghost{components::types::logical_type::BIGINT};
        ghost.set_alias("not_materialized_yet");
        wide_types.push_back(std::move(ghost));
        components::vector::data_chunk_t wide(&fx.resource, wide_types, 1);
        wide.set_cardinality(1);
        wide.set_value(0, 0, static_cast<std::int64_t>(777));
        wide.set_value(1, 0, static_cast<std::int64_t>(42));

        std::pmr::vector<std::int64_t> ids(&fx.resource);
        ids.push_back(1);

        auto upd = fx.manager->direct_update_sync(table_oid, ids, wide);
        INFO("a replayed update that dropped a journalled value must say so");
        CHECK(upd.contains_error());
        INFO("and the answer must name the column whose value was dropped");
        CHECK(std::string(upd.what.c_str()).find("not_materialized_yet") != std::string::npos);

        INFO("the materialized columns of a partially-lost replayed update must still be restored");
        CHECK(rows_where_value_is(fx, table_oid, 777) == 1);
        CHECK(rows_where_value_is(fx, table_oid, 1) == 0);

        std::pmr::vector<std::int64_t> no_ids(&fx.resource);
        components::vector::data_chunk_t no_rows(&fx.resource, wide_types, 1);
        no_rows.set_cardinality(0);
        CHECK_FALSE(fx.manager->direct_update_sync(table_oid, no_ids, no_rows).contains_error());

        components::vector::data_chunk_t wide_but_empty(&fx.resource, wide_types, 1);
        wide_but_empty.set_cardinality(1);
        wide_but_empty.set_value(0, 0, static_cast<std::int64_t>(778));
        wide_but_empty.data[1].validity().set_invalid(0);
        std::pmr::vector<std::int64_t> one_id(&fx.resource);
        one_id.push_back(1);
        CHECK_FALSE(fx.manager->direct_update_sync(table_oid, one_id, wide_but_empty).contains_error());
        CHECK(rows_where_value_is(fx, table_oid, 778) == 1);
        CHECK(rows_where_value_is(fx, table_oid, 777) == 0);

        fx.checkpoint(services::wal::id_t{100});
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        REQUIRE_FALSE(fx.manager->load_storage_for_wal_replay_sync(table_oid, ns_oid).contains_error());
        REQUIRE(fx.manager->has_storage(table_oid));
        auto rows = read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
        INFO("the database must open over a table whose replayed update reported a lost value");
        CHECK(rows == 3);
        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
        auto gone = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t_upd"));
        CHECK_FALSE(gone.found);
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_replayed_update_with_mismatched_row_ids_is_refused") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t ns_oid = catalog::INVALID_OID;
    catalog::oid_t table_oid = catalog::INVALID_OID;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_replay_mm");
        table_oid = test_create_table(fx, ns_oid, "t_mm", cols);
        const auto otbx = otbx_at(base, ns_oid, table_oid);
        std::filesystem::create_directories(otbx.parent_path());
        REQUIRE_FALSE(fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false)
                          .contains_error());
        append_rows(fx, table_oid, 3);

        std::pmr::vector<components::types::complex_logical_type> types(&fx.resource);
        components::types::complex_logical_type value_t{components::types::logical_type::BIGINT};
        value_t.set_alias("value");
        types.push_back(std::move(value_t));

        auto one_row_chunk = [&](std::int64_t v) {
            components::vector::data_chunk_t c(&fx.resource, types, 1);
            c.set_cardinality(1);
            c.set_value(0, 0, v);
            return c;
        };

        {
            std::pmr::vector<std::int64_t> two_ids(&fx.resource);
            two_ids.push_back(0);
            two_ids.push_back(1);
            auto chunk = one_row_chunk(555);
            auto err = fx.manager->direct_update_sync(table_oid, two_ids, chunk);
            INFO("a record pairing 2 row ids with 1 row must be refused, not half-applied");
            CHECK(err.contains_error());
            CHECK(rows_where_value_is(fx, table_oid, 555) == 0);
        }

        {
            std::pmr::vector<std::int64_t> no_ids(&fx.resource);
            auto chunk = one_row_chunk(556);
            auto err = fx.manager->direct_update_sync(table_oid, no_ids, chunk);
            INFO("an update that names rows but no row ids must be refused, not no-opped");
            CHECK(err.contains_error());
            CHECK(rows_where_value_is(fx, table_oid, 556) == 0);
        }

        {
            std::pmr::vector<std::int64_t> one_id(&fx.resource);
            one_id.push_back(0);
            components::vector::data_chunk_t chunk(&fx.resource, types, 2);
            chunk.set_cardinality(2);
            chunk.set_value(0, 0, static_cast<std::int64_t>(557));
            chunk.set_value(0, 1, static_cast<std::int64_t>(558));
            auto err = fx.manager->direct_update_sync(table_oid, one_id, chunk);
            INFO("a record pairing 1 row id with 2 rows must be refused before anything reads past the ids");
            CHECK(err.contains_error());
        }

        {
            std::pmr::vector<std::int64_t> ghost_id(&fx.resource);
            ghost_id.push_back(std::int64_t{1} << 55); // MAX_ROW_ID (column_data.hpp)
            auto chunk = one_row_chunk(666);
            auto err = fx.manager->direct_update_sync(table_oid, ghost_id, chunk);
            INFO("an update applied to 0 of its 1 row must not report like one applied to all");
            CHECK(err.contains_error());
            CHECK(std::string(err.what.c_str()).find("0 of") != std::string::npos);
            CHECK(rows_where_value_is(fx, table_oid, 666) == 0);
        }

        {
            std::pmr::vector<std::int64_t> id(&fx.resource);
            id.push_back(2);
            auto chunk = one_row_chunk(999);
            CHECK_FALSE(fx.manager->direct_update_sync(table_oid, id, chunk).contains_error());
            CHECK(rows_where_value_is(fx, table_oid, 999) == 1);
        }

        fx.checkpoint(services::wal::id_t{100});
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        REQUIRE_FALSE(fx.manager->load_storage_for_wal_replay_sync(table_oid, ns_oid).contains_error());
        auto rows = read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
        CHECK(rows == 3);
        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_replayed_delete_that_deleted_less_than_named_is_refused") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t ns_oid = catalog::INVALID_OID;
    catalog::oid_t table_oid = catalog::INVALID_OID;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_replay_del");
        table_oid = test_create_table(fx, ns_oid, "t_del", cols);
        const auto otbx = otbx_at(base, ns_oid, table_oid);
        std::filesystem::create_directories(otbx.parent_path());
        REQUIRE_FALSE(fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false)
                          .contains_error());
        append_rows(fx, table_oid, 3);

        {
            std::pmr::vector<std::int64_t> id(&fx.resource);
            id.push_back(1);
            CHECK_FALSE(fx.manager->direct_delete_sync(table_oid, id, 1).contains_error());
            CHECK(rows_where_value_is(fx, table_oid, 1) == 0);
        }

        {
            std::pmr::vector<std::int64_t> id(&fx.resource);
            id.push_back(1);
            auto err = fx.manager->direct_delete_sync(table_oid, id, 1);
            INFO("a replayed delete that deleted 0 of its 1 named row must be refused");
            CHECK(err.contains_error());
            CHECK(std::string(err.what.c_str()).find("0 of") != std::string::npos);
        }

        {
            std::pmr::vector<std::int64_t> id(&fx.resource);
            id.push_back(0);
            auto err = fx.manager->direct_delete_sync(table_oid, id, 2);
            INFO("a record whose count outruns its row ids must be refused before anything reads the tail");
            CHECK(err.contains_error());
            CHECK(rows_where_value_is(fx, table_oid, 0) == 1);
        }

        {
            std::pmr::vector<std::int64_t> ids(&fx.resource);
            ids.push_back(0);
            ids.push_back(2);
            auto err = fx.manager->direct_delete_sync(table_oid, ids, 1);
            INFO("a record naming more row ids than its count must be refused, not half-applied");
            CHECK(err.contains_error());
        }

        {
            std::pmr::vector<std::int64_t> no_ids(&fx.resource);
            CHECK_FALSE(fx.manager->direct_delete_sync(table_oid, no_ids, 0).contains_error());
        }

        CHECK(rows_where_value_is(fx, table_oid, 0) == 1);
        CHECK(rows_where_value_is(fx, table_oid, 2) == 1);

        fx.checkpoint(services::wal::id_t{100});
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        REQUIRE_FALSE(fx.manager->load_storage_for_wal_replay_sync(table_oid, ns_oid).contains_error());
        REQUIRE(fx.manager->has_storage(table_oid));
        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_commit_id_stamp_that_was_not_applied_is_refused") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t ns_oid = catalog::INVALID_OID;
    catalog::oid_t table_oid = catalog::INVALID_OID;
    catalog::oid_t attoid = catalog::INVALID_OID;

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_stamp");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_oid = test_create_table(fx, ns_oid, "t_stamp", cols);

        auto probe = test_probe::probe_table(fx, disk_test_helpers::auto_ctx(), ns_oid, std::string("t_stamp"));
        REQUIRE(probe.found);
        REQUIRE(probe.columns.size() == 1);
        attoid = probe.columns[0].attoid;
        REQUIRE(attoid != catalog::INVALID_OID);
        REQUIRE(added_at_commit_id_of(fx, attoid) == 0);

        CHECK_FALSE(backfill(fx, {}, 4241).contains_error());
        CHECK_FALSE(backfill(fx, {attoid}, 4242).contains_error());
        CHECK(added_at_commit_id_of(fx, attoid) == 4242);

        constexpr catalog::oid_t kGhostAttoid = 4000000000u;
        auto refused = backfill(fx, {kGhostAttoid}, 4243);
        INFO("a commit_id stamp that could not be applied must be reported, not skipped");
        CHECK(refused.contains_error());
        CHECK(added_at_commit_id_of(fx, attoid) == 4242);
        INFO("and the answer must carry the true refusal count");
        CHECK(std::string(refused.what.c_str()).find("1 of 1") != std::string::npos);

        auto mixed = backfill(fx, {kGhostAttoid, attoid}, 4244);
        CHECK(mixed.contains_error());
        INFO("the markers after a refused one must still be stamped");
        CHECK(added_at_commit_id_of(fx, attoid) == 4244);
        INFO("the answer must say 1 of 2 markers was refused — not that the whole batch was");
        CHECK(std::string(mixed.what.c_str()).find("1 of 2") != std::string::npos);

        fx.checkpoint(services::wal::id_t{100});
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        CHECK(added_at_commit_id_of(fx, attoid) == 4244);
        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::a_backfill_on_an_agent_without_pg_attribute_is_refused") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base); // deliberately NOT bootstrapped: the catalog agent holds nothing
    auto err = backfill(fx, {catalog::oid_t{12345}}, 7);
    INFO("an agent that holds no pg_attribute cannot stamp; the marker must not evaporate");
    CHECK(err.contains_error());
    CHECK(std::string(err.what.c_str()).find("pg_attribute") != std::string::npos);

    fx.manager->bootstrap_system_tables_sync();
    CHECK_FALSE(backfill(fx, {}, 8).contains_error());

    cleanup_refusal_dir();
}

namespace {
    class wal_backfill_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_backfill_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_backfill_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_backfill_fault_scope_t(const wal_backfill_fault_scope_t&) = delete;
        wal_backfill_fault_scope_t& operator=(const wal_backfill_fault_scope_t&) = delete;

        bool refuse_open = false;
        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (path.string().find("wal_") == std::string::npos) {
                return inner;
            }
            if (refuse_open) {
                return nullptr;
            }
            if (inner == nullptr) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
        }
    };
} // namespace

TEST_CASE("services::disk::open::a_refused_journal_record_cancels_the_backfill_patch") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    catalog::oid_t ns_oid = catalog::INVALID_OID;
    catalog::oid_t table_oid = catalog::INVALID_OID;
    catalog::oid_t attoid = catalog::INVALID_OID;

    auto wal_dir = base / "wal";
    std::filesystem::create_directories(wal_dir);

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        ns_oid = test_create_namespace(fx, "ns_wal_stamp");
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("a", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        table_oid = test_create_table(fx, ns_oid, "t_wal_stamp", cols);

        auto probe = test_probe::probe_table(fx, disk_test_helpers::auto_ctx(), ns_oid, std::string("t_wal_stamp"));
        REQUIRE(probe.found);
        REQUIRE(probe.columns.size() == 1);
        attoid = probe.columns[0].attoid;
        REQUIRE(attoid != catalog::INVALID_OID);
        REQUIRE(added_at_commit_id_of(fx, attoid) == 0);
        fx.checkpoint(services::wal::id_t{50});
    }

    {
        // Scope order is teardown order in reverse — the seam must outlive the WAL manager's handles.
        wal_backfill_fault_scope_t fault;
        fault.refuse_open = true;

        configuration::config_wal wal_config(wal_dir);
        wal_config.on = true;

        open_fixture fx(base);
        auto wal_manager = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(
            &fx.resource,
            fx.scheduler,
            wal_config,
            fx.log,
            components::pipeline::no_mailbox(),
            components::pipeline::no_mailbox());
        fx.manager->set_manager_wal_sync(wal_manager->address());

        fx.manager->bootstrap_system_tables_sync();
        REQUIRE(added_at_commit_id_of(fx, attoid) == 0);

        auto refused = backfill(fx, {attoid}, 5001);
        INFO("a stamp whose journal record was refused must be reported");
        CHECK(refused.contains_error());
        INFO("and the refusal must be the journal's own, not a routing one");
        CHECK(std::string(refused.what.c_str()).find("wal segment") != std::string::npos);
        INFO("and the storage patch must NOT have been applied over the refused record");
        CHECK(added_at_commit_id_of(fx, attoid) == 0);
    }

    {
        wal_backfill_fault_scope_t fault;

        configuration::config_wal wal_config(wal_dir);
        wal_config.on = true;

        open_fixture fx(base);
        auto wal_manager = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(
            &fx.resource,
            fx.scheduler,
            wal_config,
            fx.log,
            components::pipeline::no_mailbox(),
            components::pipeline::no_mailbox());
        fx.manager->set_manager_wal_sync(wal_manager->address());

        fx.manager->bootstrap_system_tables_sync();
        CHECK_FALSE(backfill(fx, {attoid}, 5002).contains_error());
        CHECK(added_at_commit_id_of(fx, attoid) == 5002);
        INFO("the healthy stamp must really have travelled through the journal seam");
        CHECK(fault.plan.writes_seen > 0);

        fx.checkpoint(services::wal::id_t{100});
    }

    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        CHECK(added_at_commit_id_of(fx, attoid) == 5002);
        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
    }

    cleanup_refusal_dir();
}

TEST_CASE("services::disk::open::an_unknown_indtype_refuses_the_start_instead_of_killing_the_process") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();

    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
    auto bad_row = catalog::build_pg_index_row(&fx.resource, oids[0], oids[1], "1", true, 'z');
    disk_test_helpers::append_ok(fx.invoke(&manager_disk_t::append_pg_catalog_row,
                                           disk_test_helpers::auto_ctx(),
                                           catalog::well_known_oid::pg_index_table,
                                           std::move(bad_row)));

    INFO("an unknown indtype is a start refusal, not a SIGABRT");
    REQUIRE_THROWS_AS(fx.manager->scan_alive_pg_index_sync(), std::runtime_error);

    fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
              disk_test_helpers::auto_ctx(),
              catalog::well_known_oid::pg_index_table,
              std::int64_t{0},
              oids[0]);
    REQUIRE_NOTHROW(fx.manager->scan_alive_pg_index_sync());
}

TEST_CASE("services::disk::open::a_null_indtype_refuses_the_start_instead_of_killing_the_process") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();

    auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{2});
    auto bad_row = catalog::build_pg_index_row(&fx.resource, oids[0], oids[1], "1", true, 'b');
    bad_row.data[4].validity().set_invalid(0);
    disk_test_helpers::append_ok(fx.invoke(&manager_disk_t::append_pg_catalog_row,
                                           disk_test_helpers::auto_ctx(),
                                           catalog::well_known_oid::pg_index_table,
                                           std::move(bad_row)));

    INFO("a NULL indtype is a start refusal, not a SIGABRT");
    REQUIRE_THROWS_AS(fx.manager->scan_alive_pg_index_sync(), std::runtime_error);
}

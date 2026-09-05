#include <catch2/catch_test_macros.hpp>

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

#include "disk_test_helpers.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// THE OPEN PATH ANSWERED "NOTHING TO READ" WHERE IT MEANT "COULD NOT READ".
//
// Every case below is one place on the database-open path — sidecar probe, lazy load, replay
// append, replay synthesis, rehydrate — that used to collapse a failure into the same value a
// legitimately empty state produces: a zero wal id, a '\0' relkind, a `false` create, a `0`
// append. Each test states the ENGINE STATE that collapse produces (a table that silently
// reports it was never checkpointed, replayed rows that vanish, a computed table restored as
// a regular one, a catalog row with no storage behind it), not the return code.
//
// AND EACH ONE IS BOUNDED BY THE SAME GATE. A refusal on the open path that repeats on every
// start is a brick, not loudness, so every case that ends in a refusal also proves the
// database still OPENS afterwards and the affected object can still be DROPPED.

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

    // A manager over `base` that does NOT bootstrap in its constructor: several cases here
    // need the pre-bootstrap state, and all of them need to open the same directory twice.
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
            // Destroy the manager first: its dtor joins the internal loop thread, which may
            // still enqueue children onto the scheduler. Only then stop/delete the scheduler.
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

    // The T3 interposer seam is process-wide; filter by path so only the named table's
    // .otbx is wrapped. Same shape as the scopes in test_system_table_bootstrap.cpp.
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
} // namespace

// --- 1. A SIDECAR THAT CANNOT BE READ IS NOT "NEVER CHECKPOINTED" ------------------------
//
// The `.otbx.wal_id` sidecar is the durable half of a table's checkpoint floor. Both readers
// answered a sidecar they could not read with wal::id_t{0}, which is the exact value that
// means "this table has never been checkpointed":
//
//   * peek_checkpoint_wal_id_from_disk (manager_disk_io.cpp) reads the sidecar with an
//     unchecked stream and returns 0 on any failure — and 0 is what the replay filter in
//     base_spaces reads as "replay every record for this table", so records already absorbed
//     into the checkpointed .otbx are applied a SECOND time;
//   * read_sidecar_wal_id inside load_storage_disk_sync returns 0 the same way, which also
//     disarms the young-file contradiction check right below it (a file with checkpointed
//     content and a sidecar claiming none) and seeds the loaded storage with floor 0.
//
// A SHORT SIDECAR IS A CRASH IMAGE, NOT CORRUPTION, and this case used to say the opposite —
// that the writer was atomic, so a short one could only be corruption. It was not: until
// persist_checkpoint_sidecar (agent_disk.cpp) the writer checked neither its write nor its
// close and fsynced nothing, so a full device during a checkpoint and a plain
// rename-without-fsync each produced one with no corruption anywhere. So the answer is not to
// refuse the table — its .otbx opened perfectly, and for a SYSTEM table a refusal is the whole
// database (case 6 below) — but to come up with the floor marked UNREADABLE, which is neither
// 0 nor a refusal. The phases below pin both halves: the readers still refuse to call it
// "never checkpointed", and the database still opens and the table still drops.
TEST_CASE("services::disk::open::an_unreadable_sidecar_is_not_never_checkpointed") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    constexpr uint64_t kRows = 7;
    constexpr auto kCheckpointId = services::wal::id_t{40};
    catalog::oid_t table_oid = catalog::INVALID_OID;
    catalog::oid_t ns_oid = catalog::INVALID_OID;

    // Phase 1 — a healthy, checkpointed table. Its sidecar records wal id 40.
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

    // Not a hand-laid file: the engine wrote this sidecar, and the only change is that it is
    // now SHORT — the state a checkpoint on a full device used to leave behind, and the one
    // both readers reported as wal::id_t{0}.
    std::filesystem::resize_file(sidecar, 3);
    REQUIRE(std::filesystem::file_size(sidecar) == 3);

    // Phase 2 — the two readers, on a fresh manager that has not loaded the table.
    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        REQUIRE_FALSE(fx.manager->has_storage(table_oid));

        // THE PROBE. RED today: a wal id of 0 for a table checkpointed at 40, which is
        // precisely "replay everything again" to the record filter.
        auto peeked = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
        INFO("a sidecar that cannot be read must not answer 'never checkpointed'");
        CHECK(peeked.has_error());

        // THE LOAD. RED before: void, one warn line, and a storage that came up with
        // checkpoint floor 0 over content checkpointed at 40. The table comes up — the .otbx
        // is not the file that rotted — and what must not survive is the floor of 0.
        auto load_err = fx.manager->load_storage_for_wal_replay_sync(table_oid, ns_oid);
        INFO("the .otbx opened fine; a sidecar that did not is no reason to leave the table unloaded");
        CHECK_FALSE(load_err.contains_error());
        CHECK(fx.manager->has_storage(table_oid));

        // AND THE SAME REFUSAL SURVIVES THE LOAD. This is the stronger form of the probe
        // above: the entry is resident now, so the answer comes from memory rather than the
        // file, and it must still not be "never checkpointed".
        auto after_load = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
        INFO("a loaded table whose floor could not be read must not answer 0 either");
        CHECK(after_load.has_error());
    }

    // Phase 3 — NOT BRICKED. The corrupt sidecar is still there; the database opens, the
    // rest of the catalog answers, and the affected table drops.
    {
        open_fixture fx(base);
        REQUIRE(std::filesystem::file_size(sidecar) == 3);
        REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fx.manager->restore_oid_generator_sync());
        REQUIRE_NOTHROW(fx.manager->load_user_table_storages_sync());
        // The table came up on the walk, so there is no divergence left for rehydrate to
        // close — and the walk ran, which is the other thing 0 has to be able to mean.
        auto unclosed = fx.manager->rehydrate_missing_user_storages_sync();
        REQUIRE_FALSE(unclosed.has_error());
        CHECK(unclosed.value() == 0);

        auto ns = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("ns_sidecar"), std::uint64_t{0});
        REQUIRE_FALSE(ns.has_error());
        CHECK(ns.value().found);

        // NOTHING WAS WRITTEN OVER, AND NOTHING WAS REPAIRED BEHIND THE OPERATOR'S BACK: the
        // .otbx is still there and the sidecar is still exactly as short as it was found.
        CHECK(std::filesystem::exists(otbx));
        CHECK(std::filesystem::file_size(sidecar) == 3);

        REQUIRE_NOTHROW(test_drop_table(fx, table_oid));
        auto gone = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t_sidecar"));
        INFO("a table whose sidecar rotted must still be droppable");
        CHECK_FALSE(gone.found);
    }

    cleanup_refusal_dir();
}

// --- 2. REPLAYED ROWS THAT LAND NOWHERE MUST NOT REPORT LIKE ROWS THAT LANDED -------------
//
// direct_append_sync is the WAL-replay append. It answered with the appended row's START ROW,
// so `0` meant all four of: the table has no storage on its owning agent, the chunk was
// empty, the append failed, and the first row of a fresh table landed at row 0. Its one
// production caller (base_spaces' replay loop) discards the value entirely, so a record of
// COMMITTED rows replayed into a table with no storage disappeared with nothing above the
// storage layer able to notice — the same hole the other three replay routers had closed.
TEST_CASE("services::disk::open::replayed_rows_with_nowhere_to_land_are_refused") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_replay");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    // The catalog helpers write pg_class / pg_attribute only — no .otbx. This is exactly the
    // replay state: a committed table the storage layer does not hold.
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

    // RED today: 0 — byte for byte the answer a successful append of the first row gives.
    auto appended = fx.manager->direct_append_sync(table_oid, chunk);
    INFO("five committed rows replayed into a table with no storage must be reported, not returned as row 0");
    CHECK(appended.has_error());

    // THE ONE LEGITIMATE NO-OP SURVIVES: a record that carries no rows asks for nothing, and
    // the table it names does have storage.
    const auto otbx = otbx_at(base, ns_oid, table_oid);
    std::filesystem::create_directories(otbx.parent_path());
    REQUIRE_FALSE(fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false)
                      .contains_error());
    REQUIRE(fx.manager->has_storage(table_oid));
    components::vector::data_chunk_t empty(&fx.resource, types, 1);
    empty.set_cardinality(0);
    auto nothing = fx.manager->direct_append_sync(table_oid, empty);
    CHECK_FALSE(nothing.has_error());

    // And a real append still answers with its start row through the same door.
    auto landed = fx.manager->direct_append_sync(table_oid, chunk);
    REQUIRE_FALSE(landed.has_error());
    CHECK(landed.value() == 0);
    auto rows = read_ok(fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
    CHECK(rows == 5);

    cleanup_refusal_dir();
}

// --- 3. A CREATE THAT FAILED IS NOT A DUPLICATE ------------------------------------------
//
// bootstrap_create_disk_inner_sync returns `false` for two unrelated outcomes: the agent
// already owns the oid, and the .otbx could not be constructed at all. create_storage_disk_sync
// read both as the first and logged "agent already owns oid" at TRACE — so a create whose very
// first write was refused left no storage, no error, and a log line naming the wrong cause.
// Its callers (WAL-replay synthesis and rehydrate) then walked on.
//
// The two legs are separable from the manager without touching the agent's contract: after a
// `false`, the owning agent either holds the oid (duplicate) or does not (the create failed).
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
        // fail_writes_from is compared with >=, so 1 refuses every write on the wrapped
        // handle — including the header write that creates the database.
        otterbrix_test::fault_plan_t plan;
        plan.fail_writes_from = 1;
        path_fault_scope_t scope(plan, "/" + std::to_string(static_cast<unsigned>(table_oid)) + "/");

        // RED today: void. The engine walks on with no storage for a table the catalog has.
        auto err = fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false);
        INFO("a create whose first write was refused must not be reported as an already-owned oid");
        CHECK(err.contains_error());
        CHECK_FALSE(fx.manager->has_storage(table_oid));
    }

    // A refusal must be retryable: the same call with the device healthy succeeds. The
    // zero-byte stump the refused create left behind is the thing that would block that
    // retry, so it must not survive the refusal.
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

    // AND THE LEGITIMATE SKIP STAYS A SKIP. Creating the same oid twice is a duplicate, not a
    // failure: the agent owns it, which is exactly the post-condition the caller wanted.
    auto dup = fx.manager->create_storage_disk_sync(table_oid, ns_oid, cols, otbx, /*is_computed=*/false);
    INFO("an oid the owning agent already holds is a legitimate skip, not an error");
    CHECK_FALSE(dup.contains_error());

    cleanup_refusal_dir();
}

// --- 4. A RELKIND NOBODY COULD READ IS NOT "REGULAR" --------------------------------------
//
// relkind_for_oid_sync answered '\0' both for "pg_class carries no row for this oid" (honest:
// the catalog does not know it) and for "pg_class is not loaded, or is too short to carry a
// relkind column" (a broken read). Its consumers turn '\0' into `is_computed = false`, so a
// DOCUMENT (relkind 'g') table restored through a path that could not read pg_class comes back
// as an ordinary row-storage table — the dynamic-schema semantics silently gone.
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
        // The catalog knows it is 'g' while pg_class is loaded.
        auto kind = fx.manager->relkind_for_oid_sync(doc_oid);
        REQUIRE_FALSE(kind.has_error());
        REQUIRE(kind.value() == catalog::relkind::computed);
        fx.checkpoint(services::wal::id_t{50});
    }

    {
        // A manager that has NOT bootstrapped: pg_class exists on disk and is not loaded.
        // This is the read that cannot be performed, as opposed to the read that came back
        // empty.
        open_fixture fx(base);
        auto kind = fx.manager->relkind_for_oid_sync(doc_oid);
        INFO("pg_class not being loaded must not answer with a relkind");
        CHECK(kind.has_error());
    }

    {
        // And the honest empty answer stays in band: pg_class IS loaded and simply carries no
        // row for this oid. That is not an error, and callers read it as "not computed".
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        auto unknown = fx.manager->relkind_for_oid_sync(catalog::oid_t{FIRST_USER_OID + 9911});
        REQUIRE_FALSE(unknown.has_error());
        CHECK(unknown.value() == '\0');
        // The document table itself still reads back as 'g' across the restart.
        auto kind = fx.manager->relkind_for_oid_sync(doc_oid);
        REQUIRE_FALSE(kind.has_error());
        CHECK(kind.value() == catalog::relkind::computed);
    }

    cleanup_refusal_dir();
}

// --- 5. THE ONE DIVERGENCE REHYDRATE EXISTS TO CLOSE, LEFT OPEN WITHOUT A WORD ------------
//
// rehydrate_missing_user_storages_sync recreates the .otbx of every alive user table the
// storage layer does not hold. When the table's pg_attribute columns do not resolve it
// `continue`d with NO log line at all — leaving exactly the catalog/storage divergence the
// walk was written to close, in the one shape where nothing downstream re-derives it.
//
// The SKIP itself is right: creating a zero-column storage is worse, and refusing the start
// would repeat forever over a catalog nobody can repair from inside the process. What is
// wrong is that it is silent — and that the divergence it leaves is not observable anywhere.
TEST_CASE("services::disk::open::rehydrate_states_the_divergence_it_cannot_close") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    open_fixture fx(base);
    fx.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fx, "ns_rehydrate");

    // A pg_class row with no pg_attribute rows behind it: alive, relkind 'r', no columns.
    // Written through the ordinary catalog append, not laid out by hand.
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

    // THE LEGITIMATE SKIP, PINNED. No zero-column storage was manufactured, and no file was
    // put down under a schema nobody could resolve.
    CHECK_FALSE(fx.manager->has_storage(orphan));
    CHECK_FALSE(std::filesystem::exists(otbx_at(base, ns_oid, orphan)));

    // AND THE DIVERGENCE IS OBSERVABLE. RED before: the walk answered void and said nothing
    // anywhere, so the only record of "the catalog names a table this engine cannot serve" was
    // a log line that did not exist. The count it reports is that record.
    auto unresolved = fx.manager->rehydrate_missing_user_storages_sync();
    REQUIRE_FALSE(unresolved.has_error());
    INFO("a table the rehydrate walk could not close must be counted, not skipped in silence");
    CHECK(unresolved.value() == 1);

    // NOT BRICKED: the start goes on, and the orphan row drops.
    REQUIRE_NOTHROW(fx.manager->load_user_table_storages_sync());
    REQUIRE_NOTHROW(test_drop_table(fx, orphan));
    auto closed = fx.manager->rehydrate_missing_user_storages_sync();
    REQUIRE_FALSE(closed.has_error());
    CHECK(closed.value() == 0);

    cleanup_refusal_dir();
}

// --- 6. A SIDECAR THAT CANNOT BE READ MUST NOT COST THE DATABASE ITS SYSTEM TABLES --------
//
// THE PREMISE THE REFUSAL WAS BUILT ON IS FALSE IN THIS TREE. Case 1 above (and three code
// comments with it) justified refusing an unreadable sidecar by "it is written atomically
// (tmp + rename), so a short one is corruption, not a crash image". The writer in
// agent_disk_t::checkpoint_inner did not check write(), did not check close(), fsynced
// neither the file nor its directory, and renamed unconditionally on is_open(). Two ROUTINE
// roads therefore led to a short or zero-length sidecar with no corruption anywhere: a full
// device or an I/O error during a checkpoint (no crash at all), and the classic
// rename-without-fsync, whose crash image is a zero-length file under the new name.
// manager_disk_io.cpp:560 says as much one function away — a crash between the tmp write and
// the rename legitimately leaves the staging file behind.
//
// AND FOR A SYSTEM TABLE THE REFUSAL IS NOT PER-TABLE, IT IS THE WHOLE DATABASE. It travels
// load_storage_disk_sync -> bootstrap_one -> throw -> base_spaces.cpp:207, which has no
// try/catch: the database never opens again, and nothing inside the process can repair the
// file. Case 1's "not bricked" phase only ever proved it for a USER table.
//
// The floor a sidecar could not give up is a fact about REPLAY, not about the .otbx: the file
// itself opened perfectly. So the table comes up with its checkpoint floor marked UNREADABLE,
// which is neither 0 (replay everything, duplicating absorbed rows) nor a refusal, and the
// replay filter's existing third answer drops that table's records loudly instead.
TEST_CASE("services::disk::open::an_unreadable_system_table_sidecar_is_not_a_brick") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    const auto sys_db = catalog::well_known_oid::main_database;
    const auto short_sidecar =
        std::filesystem::path(otbx_at(base, sys_db, well_known_oid::pg_class_table).string() + ".wal_id");
    const auto zero_sidecar =
        std::filesystem::path(otbx_at(base, sys_db, well_known_oid::pg_namespace_table).string() + ".wal_id");

    // Phase 1 — a healthy catalog with a committed checkpoint behind it. Both sidecars are
    // written by the engine, not laid out by hand.
    {
        open_fixture fx(base);
        fx.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fx, "ns_sys");
        REQUIRE(ns_oid != catalog::INVALID_OID);
        fx.checkpoint(services::wal::id_t{60});
    }
    REQUIRE(std::filesystem::file_size(short_sidecar) == sizeof(std::uint64_t));
    REQUIRE(std::filesystem::file_size(zero_sidecar) == sizeof(std::uint64_t));

    // The two states the writer above could produce on its own: the ENOSPC stump and the
    // rename-without-fsync zero.
    std::filesystem::resize_file(short_sidecar, 3);
    std::filesystem::resize_file(zero_sidecar, 0);

    // Phase 2 — RED today: bootstrap_one throws and base_spaces has no catch, so this
    // database is unopenable for good.
    {
        open_fixture fx(base);
        INFO("a system table whose sidecar cannot be read must not cost the database its start");
        REQUIRE_NOTHROW(fx.manager->bootstrap_system_tables_sync());
        REQUIRE(fx.manager->has_storage(well_known_oid::pg_class_table));
        REQUIRE(fx.manager->has_storage(well_known_oid::pg_namespace_table));

        // THE CONFLATION STAYS CLOSED. The tables came up, and neither of them claims to have
        // never been checkpointed: the floor is reported as unreadable through the loaded
        // entry too, which is what makes the replay filter drop their records instead of
        // re-applying rows the checkpointed .otbx already holds.
        auto short_floor = fx.manager->peek_checkpoint_wal_id_from_disk(well_known_oid::pg_class_table, sys_db);
        CHECK(short_floor.has_error());
        auto zero_floor = fx.manager->peek_checkpoint_wal_id_from_disk(well_known_oid::pg_namespace_table, sys_db);
        CHECK(zero_floor.has_error());

        // And the catalog answers: the namespace written before the corruption is still there,
        // which is the difference between a degraded start and an empty pg_catalog over live
        // storage.
        auto ns = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("ns_sys"), std::uint64_t{0});
        REQUIRE_FALSE(ns.has_error());
        CHECK(ns.value().found);

        // NOTHING WAS REPAIRED BEHIND THE OPERATOR'S BACK: both files are still exactly as
        // corrupt as they were found.
        CHECK(std::filesystem::file_size(short_sidecar) == 3);
        CHECK(std::filesystem::file_size(zero_sidecar) == 0);
    }

    cleanup_refusal_dir();
}


// --- 7. A FILE THAT IS PRESENT AND DID NOT LOAD IS NOT A LOST FILE ------------------------
//
// rehydrate_missing_user_storages_sync exists for a table whose .otbx was LOST — a freshly
// created file's directory entry is not fsynced, so a crash can keep the catalog row and lose
// the file. A table whose .otbx is PRESENT but was refused by the loader arrives at the same
// place ("no storage for this oid") and is the opposite state: the file is every byte the
// operator has, and creating over it answers a refusal by destroying the thing that was
// refused. Case 1 used to pin this leg with a rotten SIDECAR; a rotten sidecar no longer
// stops the open (it is a separate, derived file), so the state is reached here the only way
// left — an .otbx whose own header does not read.
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
        // Scribble over the first header sector. The size does not move, so this is not the
        // never-checkpointed signature — it is a file whose root cannot be read.
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


// --- 8. A WALK THAT NEVER RAN MUST NOT ANSWER LIKE A WALK THAT FOUND NOTHING --------------
//
// rehydrate_missing_user_storages_sync got a count so a start that came up with tables it
// cannot serve could say so. Four of its returns come BEFORE a single table is examined — no
// catalog agent, an empty config path, pg_class not loaded, pg_class too short to scan — and
// every one of them answered `0`, which is byte for byte what a healthy start answers. Its one
// production caller reads `> 0`. So the walk not running at all was the quietest outcome
// available: exactly the collapse this whole file exists to remove, re-committed in the new
// channel the fix itself introduced.
TEST_CASE("services::disk::open::a_rehydrate_walk_that_could_not_run_says_so") {
    cleanup_refusal_dir();
    auto base = std::filesystem::path(refusal_dir());
    std::filesystem::create_directories(base);

    {
        // pg_class is not loaded: this manager has not bootstrapped, so no alive table can be
        // named and not one was looked at. RED before: 0.
        open_fixture fx(base);
        auto r = fx.manager->rehydrate_missing_user_storages_sync();
        INFO("a walk that could not read pg_class must not report the count of a clean start");
        CHECK(r.has_error());
    }

    {
        // AND THE HEALTHY ZERO STAYS A ZERO. Every alive table has its storage, so there is
        // nothing to close — and the walk did run, which is the other half `0` has to mean.
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

// --- 9. A SIDECAR THIS FUNCTION COULD NOT GO LOOKING FOR IS NOT A SIDECAR THAT IS ABSENT ---
//
// peek_checkpoint_wal_id_from_disk's own comment says it: "Only 'no sidecar exists' may answer
// 0". Three conditions above that line answered 0 all the same — an empty config path, an
// INVALID table oid, and a namespace the catalog does not name — and none of them establishes
// that no sidecar exists. They establish that the function was not given enough to go looking
// for one, and 0 is the replay filter's instruction to apply every record this table has.
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

    // THE HONEST ZERO, PINNED FIRST so the refusals below cannot be mistaken for a blanket
    // one: this table has no .otbx and therefore no sidecar. Nothing was read wrong; there was
    // nothing to read.
    auto absent = fx.manager->peek_checkpoint_wal_id_from_disk(table_oid, ns_oid);
    REQUIRE_FALSE(absent.has_error());
    CHECK(absent.value() == services::wal::id_t{0});

    // RED before, all three: 0.
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

// --- 10. A RELKIND NOBODY COULD READ MUST NOT OPEN A DOCUMENT TABLE AS A REGULAR ONE -------
//
// Case 4 closed the value channel and base_spaces' replay-synthesis leg acts on it, but the
// DISK LOAD did not: it logged the failed read and carried on with `is_computed` left false.
// The comment there claimed the file would then be DEFERRED — which needs the .otbx to be
// exactly BLOCK_START bytes. A table that HAS been checkpointed is past that size, so the
// identical state opened it as an ordinary row-storage table with its dynamic-schema
// semantics gone, and the only thing the fix had changed was that a log line now appeared.
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

    // A manager that has NOT bootstrapped: pg_class is on disk and not loaded, so neither the
    // columns nor the relkind can be resolved. RED before: no_error, and a storage that came
    // up as a regular table.
    open_fixture fx(base);
    auto err = fx.manager->load_storage_for_wal_replay_sync(doc_oid, ns_oid);
    INFO("a relkind that could not be read must not decide the table is not a document one");
    CHECK(err.contains_error());
    CHECK_FALSE(fx.manager->has_storage(doc_oid));
    // Nothing was written over: the refusal leaves the file exactly as it found it.
    CHECK(std::filesystem::exists(otbx));

    cleanup_refusal_dir();
}


// --- 11. A SIDECAR PUBLISH THAT WAS REFUSED MUST LEAVE NOTHING BEHIND ---------------------
//
// The writer this file's other cases were justified by ("written atomically, tmp + rename")
// did not check its write, did not check its close, fsynced neither the file nor the
// directory, and, when the rename was refused, warned and LEFT THE STAGING FILE SITTING IN
// THE ENGINE-OWNED `table.otbx.*` namespace — where the next round writes over a file it did
// not create, and where verify_otbx_sidecars has to keep a permanent exemption for it.
//
// persist_checkpoint_sidecar states the contract instead: either the new id is published, or
// the sidecar already on disk is left byte-identical and nothing else is added next to it.
// The refusal is forced here the one way a test can force it without a device — the rename
// target is made un-renameable-over — and what is asserted is the invariant, not the cause.
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

    // RED before: the staging file the refused rename left behind, in the namespace the
    // engine polices for exactly this kind of leftover.
    INFO("a publish that could not complete must not leave its staging file in the table's namespace");
    CHECK_FALSE(std::filesystem::exists(staging));

    std::error_code ec;
    std::filesystem::remove_all(sidecar, ec);
    cleanup_refusal_dir();
}

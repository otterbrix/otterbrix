#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <services/disk/manager_disk.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/table/data_table.hpp>
#include <components/table/row_version_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <core/result_wrapper.hpp>

#include "disk_test_helpers.hpp"

#include "../../../components/table/test/fault_injection_file.hpp"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

// column_data_checkpointer_t::checkpoint walks segments only; the committed-update overlay
// (filled via the WAL-replay PHYSICAL_UPDATE path) is not a segment, so a checkpoint serialises
// pre-update bytes. Invisible in a normal round (compaction folds the overlay into fresh
// segments); the hole is the failed-round retry, which skips compaction and drops the overlay
// while the `.otbx.wal_id` sidecar advances past the WAL record that could restore it.

using namespace services::disk;
using namespace components::table;
using namespace components::types;
using namespace components::vector;
namespace catalog = components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;

namespace {

    std::string overlay_dir() { return "/tmp/test_otterbrix_cp_overlay_" + std::to_string(::getpid()); }

    void append_one_int(data_table_t& table, std::pmr::memory_resource* res, int64_t value) {
        auto types = table.copy_types();
        data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, value);
        table_append_state state(res);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    // The shape agent_disk_t::direct_update_sync uses -- NOT the statement path, which never builds an overlay.
    void overlay_update_one(data_table_t& table, std::pmr::memory_resource* res, int64_t row_id, int64_t new_value) {
        auto types = table.copy_types();
        data_chunk_t chunk(res, types, 1);
        chunk.set_cardinality(1);
        chunk.set_value(0, 0, new_value);
        vector_t ids(res, complex_logical_type(logical_type::BIGINT), 1);
        ids.set_value(0, row_id);
        auto state = table.initialize_update({});
        auto updated = table.update(*state, ids, chunk);
        REQUIRE_FALSE(updated.has_error());
        REQUIRE(updated.value().second == 1);
    }

    std::vector<int64_t> read_all_ints(table_storage_t& ts, std::pmr::memory_resource* res) {
        std::vector<int64_t> out;
        std::vector<storage_index_t> column_ids{storage_index_t(0)};
        table_scan_state state(res);
        ts.table().initialize_scan(state, column_ids, nullptr);
        auto types = ts.table().copy_types();
        data_chunk_t chunk(res, types, DEFAULT_VECTOR_CAPACITY);
        while (true) {
            chunk.reset();
            ts.table().scan(chunk, state);
            REQUIRE_FALSE(state.table_state.has_error());
            if (chunk.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < chunk.size(); i++) {
                auto cell = chunk.value(0, i);
                out.push_back(cell.value<int64_t>());
            }
        }
        return out;
    }

    // Sidecars and the WAL stay out of this by never going through a block manager, not by the `.otbx` filter.
    class otbx_fault_scope_t final
        : public components::table::storage::single_file_block_manager_t::file_handle_interposer_t {
    public:
        explicit otbx_fault_scope_t(otterbrix_test::fault_plan_t& plan)
            : plan_(plan) {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(this);
        }
        ~otbx_fault_scope_t() override {
            components::table::storage::single_file_block_manager_t::dev_set_file_interposer(nullptr);
        }

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner == nullptr || inner->path().string().find(".otbx") == std::string::npos) {
                return inner;
            }
            return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan_);
        }

    private:
        otterbrix_test::fault_plan_t& plan_;
    };

    struct overlay_disk {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit overlay_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~overlay_disk() {
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

        services::wal::id_t checkpoint_round(services::wal::id_t wal_id) {
            return checkpoint_round(wal_id, std::numeric_limits<uint64_t>::max());
        }

        // UINT64_MAX means "nothing is in flight".
        services::wal::id_t checkpoint_round(services::wal::id_t wal_id, uint64_t compact_watermark) {
            return invoke(&manager_disk_t::checkpoint_all, session_id_t{}, wal_id, compact_watermark);
        }
    };

    std::pair<uint64_t, uint64_t>
    append_rows_reporting(overlay_disk& fd, catalog::oid_t table_oid, uint64_t first, uint64_t count) {
        std::pmr::vector<complex_logical_type> types(&fd.resource);
        complex_logical_type t{logical_type::BIGINT};
        t.set_alias("value");
        types.push_back(std::move(t));
        data_chunk_t chunk(&fd.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.set_value(0, i, static_cast<std::int64_t>(first + i));
        }
        std::pmr::vector<data_chunk_t> batch(&fd.resource);
        batch.emplace_back(std::move(chunk));
        components::execution_context_t append_ctx{session_id_t{}, transaction_data{0, 0}, {}, table_oid};
        auto r = fd.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
        return r.value();
    }

    // A .otbx keeps no version metadata: every row it holds reads back as committed.
    void append_rows_uncommitted(overlay_disk& fd,
                                 catalog::oid_t table_oid,
                                 uint64_t first,
                                 uint64_t count,
                                 uint64_t txn_id) {
        std::pmr::vector<complex_logical_type> types(&fd.resource);
        complex_logical_type t{logical_type::BIGINT};
        t.set_alias("value");
        types.push_back(std::move(t));
        data_chunk_t chunk(&fd.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.set_value(0, i, static_cast<std::int64_t>(first + i));
        }
        std::pmr::vector<data_chunk_t> batch(&fd.resource);
        batch.emplace_back(std::move(chunk));
        components::execution_context_t append_ctx{session_id_t{}, transaction_data{txn_id, txn_id}, {}, table_oid};
        auto r = fd.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
    }

    void append_rows(overlay_disk& fd, catalog::oid_t table_oid, uint64_t first, uint64_t count) {
        std::pmr::vector<complex_logical_type> types(&fd.resource);
        complex_logical_type t{logical_type::BIGINT};
        t.set_alias("value");
        types.push_back(std::move(t));
        data_chunk_t chunk(&fd.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.set_value(0, i, static_cast<std::int64_t>(first + i));
        }
        std::pmr::vector<data_chunk_t> batch(&fd.resource);
        batch.emplace_back(std::move(chunk));
        components::execution_context_t append_ctx{session_id_t{}, transaction_data{0, 0}, {}, table_oid};
        auto r = fd.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
        REQUIRE_FALSE(r.has_error());
    }

    // The DURABLE half of checkpoint_wal_id_, read off the device, not the agent's memory.
    std::uint64_t sidecar_value(const std::filesystem::path& sidecar) {
        std::ifstream in(sidecar, std::ios::binary);
        REQUIRE(in.is_open());
        std::uint64_t v = 0;
        in.read(reinterpret_cast<char*>(&v), sizeof(v));
        REQUIRE(in.gcount() == static_cast<std::streamsize>(sizeof(v)));
        return v;
    }

    // Shadow paging allocates FRESH blocks, so a round that touched the table cannot leave the bytes equal.
    std::uint64_t file_digest(const std::filesystem::path& p) {
        std::ifstream in(p, std::ios::binary);
        REQUIRE(in.is_open());
        std::uint64_t h = 1469598103934665603ULL;
        std::vector<char> buf(64 * 1024);
        while (in) {
            in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
            const auto got = static_cast<std::size_t>(in.gcount());
            for (std::size_t i = 0; i < got; i++) {
                h ^= static_cast<std::uint64_t>(static_cast<unsigned char>(buf[i]));
                h *= 1099511628211ULL;
            }
        }
        return h;
    }

    std::filesystem::path otbx_of(const std::filesystem::path& root, catalog::oid_t table_oid) {
        return root / std::to_string(static_cast<unsigned>(catalog::well_known_oid::main_database)) /
               std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
    }

} // namespace

// Advancing checkpoint_wal_id_ over pre-update bytes would seal the only remaining copy of the value.
TEST_CASE("services::disk::update_overlay::a_checkpoint_that_cannot_fold_the_overlay_refuses") {
    auto dir = overlay_dir() + "/refuses";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    core::pmr::otterbrix_resource resource;

    auto otbx = std::filesystem::path(dir) / "overlay.otbx";
    constexpr int64_t N = 5;

    {
        std::vector<column_definition_t> cols;
        cols.emplace_back("value", logical_type::BIGINT);
        table_storage_t ts(&resource, std::move(cols), otbx);
        REQUIRE_FALSE(ts.construction_failed());

        for (int64_t i = 0; i < N; i++) {
            append_one_int(ts.table(), &resource, i);
        }
        REQUIRE_FALSE(ts.checkpoint(services::wal::id_t{100}).has_error());
        REQUIRE(ts.checkpoint_wal_id() == 100);

        overlay_update_one(ts.table(), &resource, /*row_id=*/2, /*new_value=*/999);
        CHECK(read_all_ints(ts, &resource) == std::vector<int64_t>{0, 1, 999, 3, 4});

        auto retried = ts.checkpoint(services::wal::id_t{200});
        INFO("a checkpoint that serialises only segments cannot report success over an overlay it drops");
        REQUIRE(retried.has_error());
        CHECK(ts.checkpoint_wal_id() == 100);
        CHECK(ts.prev_checkpoint_wal_id() == 0);
    }

    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == std::vector<int64_t>{0, 1, 2, 3, 4});
    }

    std::filesystem::remove_all(dir);
}

// The space argument behind the un-compacted retry doesn't reach as far as dropping a committed value.
TEST_CASE("services::disk::update_overlay::the_retry_round_still_lands_a_replayed_update") {
    auto dir = overlay_dir() + "/retry_round";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    core::pmr::otterbrix_resource probe_resource;
    catalog::oid_t table_oid = 0;

    {
        otterbrix_test::fault_plan_t plan;
        otbx_fault_scope_t scope(plan); // installed BEFORE the manager opens anything

        overlay_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "overlay_ns");

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
        table_oid = test_create_table(fd, ns_oid, "overlay_t", columns);
        REQUIRE(table_oid >= catalog::FIRST_USER_OID);
        fd.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  columns,
                  /*is_computed=*/false);
        append_rows(fd, table_oid, 0, 5);

        fd.checkpoint_round(services::wal::id_t{100});

        append_rows(fd, table_oid, 5, 1);
        plan.fail_writes_at_header_slots = true;
        fd.checkpoint_round(services::wal::id_t{200});
        REQUIRE(plan.header_writes_failed > 0);
        plan.fail_writes_at_header_slots = false;

        {
            std::pmr::vector<complex_logical_type> types(&fd.resource);
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias("value");
            types.push_back(std::move(t));
            data_chunk_t chunk(&fd.resource, types, 1);
            chunk.set_cardinality(1);
            chunk.set_value(0, 0, static_cast<std::int64_t>(999));
            std::pmr::vector<std::int64_t> ids(&fd.resource);
            ids.push_back(2);
            REQUIRE_FALSE(fd.manager->direct_update_sync(table_oid, ids, chunk).contains_error());
        }

        fd.checkpoint_round(services::wal::id_t{300});
    }

    {
        table_storage_t ts(&probe_resource, otbx_of(dir, table_oid), {});
        REQUIRE_FALSE(ts.construction_failed());
        auto rows = read_all_ints(ts, &probe_resource);
        INFO("the replayed update must be in the checkpointed file, not only in the overlay");
        CHECK(std::find(rows.begin(), rows.end(), static_cast<int64_t>(999)) != rows.end());
        INFO("and the value it replaced must be gone from it");
        CHECK(std::find(rows.begin(), rows.end(), static_cast<int64_t>(2)) == rows.end());
    }

    std::filesystem::remove_all(dir);
}

// A sentinel, not a reproduction: reservation and append share one mailbox-atomic handler with
// no seam to stage the divergence from out here. Sensitivity proven by injection: with
// `const uint64_t start_row = s->total_rows() + 1;` in agent_disk_t::storage_append_inner, this
// case failed at the first append with io_error "journalled start_row 1 but the rows
// materialized at 0"; the injection was reverted.
TEST_CASE("services::disk::checkpoint_round::an_append_answers_with_the_start_row_it_journalled") {
    auto dir = overlay_dir() + "/start_row";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    overlay_disk fd(dir);
    fd.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fd, "start_row_ns");

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
    auto table_oid = test_create_table(fd, ns_oid, "start_row_t", columns);
    fd.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              columns,
              /*is_computed=*/false);

    const auto first = append_rows_reporting(fd, table_oid, 0, 4);
    CHECK(first.first == 0);
    CHECK(first.second == 4);

    const auto second = append_rows_reporting(fd, table_oid, 4, 3);
    INFO("the second append must be answered with the row count that preceded it");
    CHECK(second.first == 4);
    CHECK(second.second == 3);

    const auto third = append_rows_reporting(fd, table_oid, 7, 1);
    CHECK(third.first == 7);
    CHECK(third.second == 1);

    std::filesystem::remove_all(dir);
}

// A DROP removing only the published sidecar left the staging file behind, and with the
// directory non-empty the per-oid directory removal failed too.
TEST_CASE("services::disk::checkpoint_round::drop_removes_the_sidecar_staging_file_and_the_oid_directory") {
    auto dir = overlay_dir() + "/drop_staging";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    overlay_disk fd(dir);
    fd.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fd, "drop_ns");

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
    auto table_oid = test_create_table(fd, ns_oid, "drop_t", columns);
    fd.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              columns,
              /*is_computed=*/false);
    append_rows(fd, table_oid, 0, 4);
    fd.checkpoint_round(services::wal::id_t{100});

    const auto otbx = otbx_of(dir, table_oid);
    auto sidecar = otbx;
    sidecar += ".wal_id";
    auto staging = sidecar;
    staging += ".tmp";
    REQUIRE(std::filesystem::exists(otbx));
    REQUIRE(std::filesystem::exists(sidecar));
    {
        std::ofstream f(staging, std::ios::binary | std::ios::trunc);
        REQUIRE(f.is_open());
        std::uint64_t id = 100;
        f.write(reinterpret_cast<const char*>(&id), sizeof(id));
    }
    REQUIRE(std::filesystem::exists(staging));

    std::pmr::vector<catalog::oid_t> victims(&fd.resource);
    victims.push_back(table_oid);
    fd.invoke(&manager_disk_t::drop_storage_many, session_id_t{}, std::move(victims));

    CHECK_FALSE(std::filesystem::exists(otbx));
    CHECK_FALSE(std::filesystem::exists(sidecar));
    INFO("the staging file is the other name this engine owns next to the .otbx");
    CHECK_FALSE(std::filesystem::exists(staging));
    INFO("and with the directory empty, the per-oid directory goes with it");
    CHECK_FALSE(std::filesystem::exists(otbx.parent_path()));

    std::filesystem::remove_all(dir);
}

// write_header is the atomic point of a checkpoint; a sidecar that couldn't publish would leave
// the .otbx at root N+1 while the durable floor still said N, so a restart re-applies every WAL
// record this round already absorbed.
TEST_CASE("services::disk::checkpoint_round::a_sidecar_that_cannot_be_written_does_not_split_the_floor") {
    auto dir = overlay_dir() + "/sidecar_split";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    overlay_disk fd(dir);
    fd.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fd, "sidecar_ns");

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
    auto table_oid = test_create_table(fd, ns_oid, "sidecar_t", columns);
    fd.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              columns,
              /*is_computed=*/false);
    append_rows(fd, table_oid, 0, 4);
    fd.checkpoint_round(services::wal::id_t{100});

    const auto otbx = otbx_of(dir, table_oid);
    auto sidecar = otbx;
    sidecar += ".wal_id";
    auto staging = sidecar;
    staging += ".tmp";
    REQUIRE(sidecar_value(sidecar) == 100);
    const auto otbx_before = file_digest(otbx);

    std::filesystem::create_directories(staging);
    {
        std::ofstream blocker(staging / "blocker", std::ios::binary | std::ios::trunc);
        REQUIRE(blocker.is_open());
        blocker << "occupied";
    }

    append_rows(fd, table_oid, 4, 2);
    fd.checkpoint_round(services::wal::id_t{200});

    const auto durable = sidecar_value(sidecar);
    auto engine = fd.manager->peek_checkpoint_wal_id_from_disk(table_oid, catalog::well_known_oid::main_database);
    REQUIRE_FALSE(engine.has_error());
    INFO("durable sidecar says " << static_cast<std::uint64_t>(durable) << ", the engine says "
                                 << static_cast<std::uint64_t>(engine.value()));
    CHECK(static_cast<std::uint64_t>(engine.value()) == static_cast<std::uint64_t>(durable));

    CHECK(file_digest(otbx) == otbx_before);

    std::filesystem::remove_all(dir);
}

// The failed-round retry skips compact(), and the MVCC-watermark gate lived inside that call:
// `!skip_compact_this_round && !compact(watermark)` never evaluates its right half on retry.
TEST_CASE("services::disk::checkpoint_round::a_retry_round_does_not_checkpoint_uncommitted_rows") {
    auto dir = overlay_dir() + "/uncommitted_retry";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    core::pmr::otterbrix_resource probe_resource;
    catalog::oid_t table_oid = 0;

    constexpr uint64_t watermark = TRANSACTION_ID_START - 1;
    constexpr uint64_t pending_txn = TRANSACTION_ID_START + 7;

    {
        otterbrix_test::fault_plan_t plan;
        otbx_fault_scope_t scope(plan); // installed BEFORE the manager opens anything

        overlay_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        auto ns_oid = test_create_namespace(fd, "uncommitted_ns");

        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
        table_oid = test_create_table(fd, ns_oid, "uncommitted_t", columns);
        REQUIRE(table_oid >= catalog::FIRST_USER_OID);
        fd.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  columns,
                  /*is_computed=*/false);
        append_rows(fd, table_oid, 0, 5);
        fd.checkpoint_round(services::wal::id_t{100}, watermark);

        append_rows(fd, table_oid, 5, 1);
        plan.fail_writes_at_header_slots = true;
        fd.checkpoint_round(services::wal::id_t{200}, watermark);
        REQUIRE(plan.header_writes_failed > 0);
        plan.fail_writes_at_header_slots = false;

        append_rows_uncommitted(fd, table_oid, 777, 1, pending_txn);

        fd.checkpoint_round(services::wal::id_t{300}, watermark);
    }

    {
        table_storage_t ts(&probe_resource, otbx_of(dir, table_oid), {});
        REQUIRE_FALSE(ts.construction_failed());
        auto rows = read_all_ints(ts, &probe_resource);
        INFO("an uncommitted row must not reach a file that cannot say it is uncommitted");
        CHECK(std::find(rows.begin(), rows.end(), static_cast<int64_t>(777)) == rows.end());
        INFO("and the committed rows the last good round wrote are still there");
        CHECK(std::find(rows.begin(), rows.end(), static_cast<int64_t>(0)) != rows.end());
        CHECK(std::find(rows.begin(), rows.end(), static_cast<int64_t>(4)) != rows.end());
    }

    std::filesystem::remove_all(dir);
}

// The GC sweep removed the files but left the per-oid directory behind, forever.
TEST_CASE("services::disk::checkpoint_round::the_gc_sweep_takes_the_oid_directory_with_the_file") {
    auto dir = overlay_dir() + "/gc_dir";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    overlay_disk fd(dir);
    fd.manager->bootstrap_system_tables_sync();
    auto ns_oid = test_create_namespace(fd, "gc_ns");

    std::vector<column_definition_t> columns;
    columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
    auto table_oid = test_create_table(fd, ns_oid, "gc_t", columns);
    fd.invoke(&manager_disk_t::create_storage_disk,
              session_id_t{},
              table_oid,
              catalog::well_known_oid::main_database,
              columns,
              /*is_computed=*/false);
    append_rows(fd, table_oid, 0, 4);
    fd.checkpoint_round(services::wal::id_t{100});

    const auto otbx = otbx_of(dir, table_oid);
    auto sidecar = otbx;
    sidecar += ".wal_id";
    REQUIRE(std::filesystem::exists(otbx));
    REQUIRE(std::filesystem::exists(sidecar));

    constexpr std::uint64_t dropped_at = 5000;
    {
        std::pmr::vector<catalog::oid_t> victims{&fd.resource};
        victims.push_back(table_oid);
        fd.invoke(&manager_disk_t::mark_storage_dropped_many, session_id_t{}, std::move(victims), dropped_at);
    }
    // The mark alone reclaims nothing: GC is horizon-driven.
    REQUIRE(std::filesystem::exists(otbx));

    fd.invoke(&manager_disk_t::on_horizon_advanced, dropped_at + 1);

    CHECK_FALSE(std::filesystem::exists(otbx));
    CHECK_FALSE(std::filesystem::exists(sidecar));
    INFO("the transactional DROP must leave the same tree behind as the immediate one");
    CHECK_FALSE(std::filesystem::exists(otbx.parent_path()));

    std::filesystem::remove_all(dir);
}

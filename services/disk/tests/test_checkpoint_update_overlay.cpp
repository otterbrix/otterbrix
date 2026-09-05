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

// ---------------------------------------------------------------------------------------
// THE COMMITTED-UPDATE OVERLAY AND THE CHECKPOINT THAT CANNOT SEE IT.
//
// column_data_checkpointer_t::checkpoint walks column_data_.data_.segments() and nothing
// else: the committed-update overlay (column_data_t::updates_, filled by
// data_table_t::update — the WAL-replay PHYSICAL_UPDATE path through
// agent_disk_t::direct_update_sync) is not a segment and is not read there. A checkpoint
// therefore serialises the PRE-update bytes.
//
// In a normal round that is invisible, because agent_disk_t::checkpoint_inner compacts
// every entry first and the rebuild scan folds the overlay into the fresh segments. The
// mask has a hole: the failed-round retry deliberately checkpoints WITHOUT compacting
// (skip_compact_this_round = last_checkpoint_failed()), and there the overlay is dropped
// while the `.otbx.wal_id` sidecar advances past the journal record that carried it. The
// value is silently rolled back and the record that could restore it is sealed away.
//
// The first two cases drive that, one at each layer:
//   1. table_storage_t::checkpoint straight — the retry's shape, minus the round;
//   2. the production round through manager_disk_t::checkpoint_all, with the retry reached
//      the way it is reached in the field: a header-slot write failure, which is the ONE
//      checkpoint failure the block manager treats as recoverable (write_header's case 2:
//      both slots read back as the iteration this manager already believed, so nothing
//      latches) and therefore the one that leads to a RETRY rather than to the degraded
//      entry's deferral.
//
// The rest of the file is the round's other durability bookkeeping, which the same round
// owns and which the same kind of silence used to hide:
//   3. the start_row a WAL-first append journals is the one it answers with;
//   4. a DROP takes the whole `table.otbx.*` namespace, staging file and directory included;
//   5. a sidecar that cannot be written does not split the floor in two;
//   6. the retry round still asks the MVCC question the compact it skips used to ask;
//   7. the transactional DROP's GC sweep leaves the same tree behind as the immediate DROP.
// Cases 3-7 carry the `checkpoint_round` tag: they are the round's own bookkeeping, not the
// overlay.
// ---------------------------------------------------------------------------------------

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

    // The overlay-producing write, in exactly the shape agent_disk_t::direct_update_sync
    // hands to table_storage_adapter_t::update(row_ids, data): one id vector, one chunk at
    // the table's own width, straight into data_table_t::update. NOT the statement path
    // (delete-old + append-new), which never builds an overlay.
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

    // Every row of column 0, read back out of a storage. The judgement is the DATA, never
    // "the open succeeded".
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

    // The T3 interposer seam is process-wide and wraps at OPEN time, so it must be installed
    // before the manager opens anything; the plan is armed later, around the one round that
    // must fail.
    //
    // WHAT KEEPS THE SIDECARS AND THE WAL OUT OF IT IS THE SEAM, NOT THE `.otbx` FILTER: the
    // interposer is applied only to single_file_block_manager_t's OWN handle (it wraps at both
    // of its open sites and nowhere else), and neither the sidecar writer nor the WAL goes
    // through a block manager. The filter is kept because it says which file this scope means —
    // it is not what makes the scope narrow, and `table.otbx.wal_id` would pass it.
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

        // The watermark the dispatcher would supply: every stamp at or below it is visible to
        // all. UINT64_MAX above means "nothing is in flight", which is what most of this file
        // wants; a round that has to see an uncommitted write passes a real one.
        services::wal::id_t checkpoint_round(services::wal::id_t wal_id, uint64_t compact_watermark) {
            return invoke(&manager_disk_t::checkpoint_all, session_id_t{}, wal_id, compact_watermark);
        }
    };

    // Appends `count` rows and hands back the whole answer, so a caller can judge the
    // start_row the handler reports and not only whether it refused.
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

    // AN APPEND THAT IS MATERIALIZED BUT NOT COMMITTED: the rows land in the segments now and
    // are stamped with the PENDING transaction id, which no commit ever publishes here. That is
    // the ordinary state of a table between a statement and its COMMIT, and it is the state a
    // checkpoint must not serialize — a .otbx keeps no version metadata, so every row it holds
    // reads back as committed.
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

    // The DURABLE half of checkpoint_wal_id_, read off the device rather than out of the
    // agent's memory.
    std::uint64_t sidecar_value(const std::filesystem::path& sidecar) {
        std::ifstream in(sidecar, std::ios::binary);
        REQUIRE(in.is_open());
        std::uint64_t v = 0;
        in.read(reinterpret_cast<char*>(&v), sizeof(v));
        REQUIRE(in.gcount() == static_cast<std::streamsize>(sizeof(v)));
        return v;
    }

    // FNV-1a over a whole file: shadow paging allocates FRESH blocks and commits the OTHER
    // header slot, so a round that touched the table cannot leave the bytes equal.
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

// 1. THE RETRY'S SHAPE, ONE LAYER DOWN. A committed-update overlay is outstanding and the
//    checkpoint is taken WITHOUT the rebuild that folds it — exactly what the failed-round
//    gate does. The checkpoint must not report a success it cannot back: the bytes it
//    writes are the PRE-update ones, and advancing checkpoint_wal_id_ over them seals the
//    journal record that is the only remaining copy of the value.
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

        // The replayed PHYSICAL_UPDATE: row 2 becomes 999, in the overlay.
        overlay_update_one(ts.table(), &resource, /*row_id=*/2, /*new_value=*/999);
        // In memory the update is there — the overlay is what the read path layers on.
        CHECK(read_all_ints(ts, &resource) == std::vector<int64_t>{0, 1, 999, 3, 4});

        // The retry: checkpoint with no compact() in between.
        auto retried = ts.checkpoint(services::wal::id_t{200});
        INFO("a checkpoint that serialises only segments cannot report success over an overlay it drops");
        REQUIRE(retried.has_error());
        // The durable floor must not move over a value that did not reach the file: the WAL
        // record carrying the 999 is the only remaining copy of it.
        CHECK(ts.checkpoint_wal_id() == 100);
        CHECK(ts.prev_checkpoint_wal_id() == 0);
    }

    // And the file itself is the previous root, whole: the refusal costs the round, not the
    // table.
    {
        table_storage_t ts(&resource, otbx, {});
        REQUIRE_FALSE(ts.construction_failed());
        CHECK(read_all_ints(ts, &resource) == std::vector<int64_t>{0, 1, 2, 3, 4});
    }

    std::filesystem::remove_all(dir);
}

// 2. THE PRODUCTION ROUND. A header-slot write failure puts the entry into the failed-round
//    state, and a replayed update then lands in the overlay. The NEXT round must still get
//    that value onto the device: the space argument behind the un-compacted retry does not
//    reach as far as dropping a committed value.
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

        // A round every entry FAILS. Only the two header slots are refused, so no data write
        // latches durability_error_ and no block manager goes degraded — which is what keeps
        // the entries on the RETRY path instead of the deferral path.
        append_rows(fd, table_oid, 5, 1);
        plan.fail_writes_at_header_slots = true;
        fd.checkpoint_round(services::wal::id_t{200});
        REQUIRE(plan.header_writes_failed > 0);
        plan.fail_writes_at_header_slots = false;

        // The replayed PHYSICAL_UPDATE, straight at the agent's replay router: row 2 becomes
        // 999, in the overlay.
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

        // The retry round. It must get the 999 onto the device.
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

// 3. THE START_ROW THE JOURNAL NAMES IS THE START_ROW THE ROWS LAND AT.
//
// storage_append_inner reserves start_row from total_rows(), writes it into the
// PHYSICAL_INSERT record, and only then materializes. The two must agree — CREATE INDEX's
// backfill-from-WAL uses the journalled value as the row-id base of the replayed chunk. The
// agreement used to be an assert, which NDEBUG deletes: the release build answered with the
// materialized base while the record on disk named the reserved one, and nothing said so.
//
// THE DIVERGENCE ITSELF CANNOT BE STAGED FROM OUT HERE: the reservation and the append sit
// in one mailbox-atomic handler with no seam between them, so this is a SENTINEL on the
// success path rather than a reproduction. Its sensitivity was proven by injection, not
// assumed: with `const uint64_t start_row = s->total_rows() + 1;` in
// agent_disk_t::storage_append_inner (agent_disk.cpp, step 5) this case fails at the first
// append with io_error "journalled start_row 1 but the rows materialized at 0"; the
// injection was reverted.
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

// 4. DROP TAKES THE WHOLE `table.otbx.*` NAMESPACE WITH IT — THE STAGING FILE INCLUDED.
//
// The sidecar is published through `<table>.otbx.wal_id.tmp` + rename, and a
// crash between its staging fsync and that rename legitimately leaves the .tmp behind —
// verify_otbx_sidecars names it as one of the two files this build owns, which is precisely
// why no open path deletes it. A DROP that removed only the published sidecar therefore left
// the staging file behind, and with the directory not empty the per-oid directory removal
// failed too: an oid's directory outliving the table it belonged to.
//
// The .tmp is written here by hand, deliberately: in-process it can never survive a failure
// (stage_checkpoint_sidecar's refuse() removes it on every leg, and so does the round's own
// discard on a deferral), so the only producer is a
// crash, and the file's own contract already declares this exact name legitimate.
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

// 5. THE ROUND'S TWO HALVES COMMIT IN ONE ORDER, AND THE ORDER MUST NOT BE ABLE TO SPLIT THEM.
//
// The atomic point of a table's checkpoint is write_header (its own fsync IS the commit), and
// the durable half of checkpoint_wal_id_ — the `.otbx.wal_id` sidecar — used to be written
// AFTER it. So a sidecar that could not be published left the .otbx at root N+1 while the
// durable floor still said N: a restart then re-applies every WAL record this round already
// absorbed. That is not a crash scenario — a full device or a refused create is enough.
//
// Staged here without a crash and without laying out any database state by hand: the staging
// NAME is occupied by a non-empty DIRECTORY, so stage_checkpoint_sidecar's own stale-tmp
// remove and its FILE_CREATE_NEW open both fail, exactly as they would on ENOSPC.
//
// What must hold is that the two halves agree afterwards, whichever way the round went.
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

    // Occupy the staging name with something neither remove() nor create-new can get past.
    std::filesystem::create_directories(staging);
    {
        std::ofstream blocker(staging / "blocker", std::ios::binary | std::ios::trunc);
        REQUIRE(blocker.is_open());
        blocker << "occupied";
    }

    append_rows(fd, table_oid, 4, 2);
    fd.checkpoint_round(services::wal::id_t{200});

    // The durable floor is whatever the file says, and the engine must not believe anything
    // else about this table: a floor that moved only in memory is a floor a restart cannot
    // read, and every record between the two ids gets replayed into a root that already has it.
    const auto durable = sidecar_value(sidecar);
    auto engine = fd.manager->peek_checkpoint_wal_id_from_disk(table_oid, catalog::well_known_oid::main_database);
    REQUIRE_FALSE(engine.has_error());
    INFO("durable sidecar says " << static_cast<std::uint64_t>(durable) << ", the engine says "
                                 << static_cast<std::uint64_t>(engine.value()));
    CHECK(static_cast<std::uint64_t>(engine.value()) == static_cast<std::uint64_t>(durable));

    // And the half that could not be published must not have been committed either: with the
    // sidecar unwritable the entry is deferred, so its .otbx is still root N, byte for byte.
    CHECK(file_digest(otbx) == otbx_before);

    std::filesystem::remove_all(dir);
}

// 6. THE RETRY ROUND STILL ASKS THE MVCC QUESTION.
//
// A .otbx stores no version metadata: every row it holds reads back as plain committed. So a
// checkpoint may only serialize a table whose every stamp is already visible to all — that is
// what data_table_t::compact gates itself on, and while every round compacted, asking compact()
// was asking the gate.
//
// The failed-round retry does NOT compact (see the space argument on that gate), and the gate
// lived INSIDE the call it skips: `!skip_compact_this_round && !compact(watermark)` never
// evaluates its right half on the retry. An uncommitted append landing between a failed round
// and its retry was therefore written into the .otbx as committed data and resurrected at the
// next start — a row no transaction ever published, visible forever.
//
// Staged here exactly that way: a header-slot write failure puts the entry on the retry path
// (the ONE checkpoint failure the block manager treats as recoverable), an append stamped with a
// pending transaction id follows, and the retry round runs with a watermark below that stamp.
TEST_CASE("services::disk::checkpoint_round::a_retry_round_does_not_checkpoint_uncommitted_rows") {
    auto dir = overlay_dir() + "/uncommitted_retry";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    core::pmr::otterbrix_resource probe_resource;
    catalog::oid_t table_oid = 0;

    // Below every pending txn id (those start at TRANSACTION_ID_START) and above every committed
    // stamp this test makes — the shape of a real dispatcher watermark.
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

        // A round the entry FAILS, which is what arms the un-compacted retry. Only the two
        // header slots are refused, so nothing latches durability_error_ and the entry stays on
        // the RETRY path instead of the degraded-entry deferral.
        append_rows(fd, table_oid, 5, 1);
        plan.fail_writes_at_header_slots = true;
        fd.checkpoint_round(services::wal::id_t{200}, watermark);
        REQUIRE(plan.header_writes_failed > 0);
        plan.fail_writes_at_header_slots = false;

        // The uncommitted write. It is in the segments from this moment on; only its version
        // stamp says it must not be seen.
        append_rows_uncommitted(fd, table_oid, 777, 1, pending_txn);

        // The retry round. It must NOT put row 777 on the device: nothing has committed it, and
        // the file it would be written into cannot record that.
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

// 7. THE TRANSACTIONAL DROP TAKES THE PER-OID DIRECTORY TOO.
//
// There are two DROP paths and they used to disagree about the same tree. The immediate one
// (drop_storage_many -> drop_storage_one_local) removes the .otbx, both sidecar names and then
// the per-oid directory. The transactional one records a GC entry (mark_storage_dropped_many)
// and reclaims it when the horizon passes (on_horizon_advanced -> on_horizon_advanced_inner),
// and that path removed the files and left the directory standing — a dropped table's oid
// outliving the table in the layout, one empty directory per transactional DROP, forever.
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

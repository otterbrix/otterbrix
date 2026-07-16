// Checkpoint fences — agent-level tests for two checkpoint/replay hazards.
//
// 1. The sidecar wal_id must cover every WAL record the agent already applied to
//    the table it folds. run_auto_checkpoint snapshots the global wal id BEFORE
//    two cross-actor suspensions, so DML can land — and be folded into the
//    .otbx — between the snapshot and the fold. Stamping the stale snapshot
//    makes replay re-apply those records onto a base that embodies them
//    (positional DELETEs then tombstone the wrong survivors).
//
// 2. checkpoint_inner's compact is a renumber epoch, same as the maybe_cleanup
//    and VACUUM compaction sites. When the subsequent fold fails (deferred
//    round), the renumber survives in memory while the durable state stays
//    pre-compact — so it must be fenced by a PHYSICAL_COMPACT marker, or every
//    later positional record replays against the wrong numbering.

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/wal_reader.hpp>

#include <filesystem>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace services::disk;
namespace catalog = components::catalog;
namespace types = components::types;
using session_id_t = components::session::session_id_t;

namespace {

    constexpr uint64_t kMaxWatermark = std::numeric_limits<uint64_t>::max();
    constexpr catalog::oid_t kMainDb = catalog::well_known_oid::main_database;

    std::string fence_test_dir() {
        static std::string p = "/tmp/test_otterbrix_cpfence_" + std::to_string(::getpid());
        return p;
    }

    // Disk + WAL fixture (mirrors test_recovery.cpp's): the WAL manager must be
    // wired so the agent's in-turn PHYSICAL_* emits actually run.
    struct fence_fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_wal wal_config;
        configuration::config_disk disk_config;
        std::unique_ptr<services::wal::manager_wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> disk;

        explicit fence_fixture(const std::string& dir)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , wal_config([&]() {
                configuration::config_wal c;
                c.path = dir;
                c.on = true;
                return c;
            }())
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = dir;
                return c;
            }())
            , wal(actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource, scheduler, wal_config, log))
            , disk(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            std::filesystem::create_directories(dir);
            wal->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(disk->address()),
                                                     actor_zeta::address_t::empty_address(),
                                                     actor_zeta::address_t::empty_address()});
            disk->sync(services::disk::manager_disk_t::disk_sync_pack_t{wal->address()});
            disk->bootstrap_system_tables_sync();
        }
        ~fence_fixture() {
            disk.reset();
            wal.reset();
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(disk->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }

        template<typename Fn, typename... Args>
        auto invoke_wal(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(wal->address(), fn, std::forward<Args>(args)...);
            for (int i = 0; i < 100000 && !future.is_ready(); ++i) {
                scheduler->run(1000);
                std::this_thread::yield();
            }
            REQUIRE(future.is_ready());
            return std::move(future).take_ready();
        }
    };

    components::table::transaction_data sys_txn() { return components::table::transaction_data{0, 0}; }

    // A real (non-zero) txn: the agent's in-turn PHYSICAL_* WAL emits are gated on
    // it (txn 0 is the replay path and writes no WAL).
    components::table::transaction_data live_txn() {
        components::table::transaction_data td(88, 0);
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        return td;
    }

    void make_disk_table(fence_fixture& fx, catalog::oid_t oid) {
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("v", types::complex_logical_type{types::logical_type::BIGINT});
        fx.invoke(&manager_disk_t::create_storage_disk, session_id_t{}, oid, kMainDb, cols);
    }

    // Txn-append n rows starting at `start`, then publish them committed so the
    // later checkpoint compact sees plain committed stamps.
    void append_committed(fence_fixture& fx, catalog::oid_t oid, int64_t start, int64_t n) {
        std::pmr::vector<types::complex_logical_type> ct{&fx.resource};
        ct.emplace_back(types::logical_type::BIGINT);
        components::vector::data_chunk_t chunk{&fx.resource, ct, static_cast<size_t>(n)};
        chunk.set_cardinality(static_cast<size_t>(n));
        for (int64_t i = 0; i < n; ++i) {
            chunk.set_value(0, static_cast<size_t>(i), types::logical_value_t{&fx.resource, i});
        }
        std::pmr::vector<components::vector::data_chunk_t> chunks{&fx.resource};
        chunks.emplace_back(std::move(chunk));
        components::execution_context_t ctx{session_id_t{}, live_txn(), {}};
        ctx.table_oid = oid;
        auto r = fx.invoke(&manager_disk_t::storage_append, ctx, oid, std::move(chunks));
        REQUIRE_FALSE(r.has_error());

        std::vector<components::pg_catalog_append_range_t> ranges;
        ranges.push_back(components::pg_catalog_append_range_t{oid, start, static_cast<uint64_t>(n)});
        components::execution_context_t pub_ctx{session_id_t{}, sys_txn(), {}};
        fx.invoke(&manager_disk_t::storage_publish_commits, pub_ctx, uint64_t{5}, std::move(ranges));

        // COMMIT marker with FULL sync: flushes the wal worker's buffer to the
        // segment file (a live reader sees nothing before the first flush) and
        // puts txn 88 in the committed set.
        fx.invoke_wal(&services::wal::manager_wal_replicate_t::commit_txn,
                      session_id_t{},
                      uint64_t{88},
                      services::wal::wal_sync_mode::FULL,
                      kMainDb,
                      uint64_t{5});
    }

    uint64_t delete_ids(fence_fixture& fx, catalog::oid_t oid, const std::vector<int64_t>& ids) {
        components::vector::vector_t v(&fx.resource, types::logical_type::BIGINT, ids.size());
        for (size_t i = 0; i < ids.size(); ++i) {
            v.data<int64_t>()[i] = ids[i];
        }
        components::execution_context_t ctx{session_id_t{}, sys_txn(), {}};
        ctx.table_oid = oid;
        return fx.invoke(&manager_disk_t::storage_delete_rows,
                         ctx,
                         oid,
                         std::move(v),
                         static_cast<uint64_t>(ids.size()));
    }

    uint64_t total_rows(fence_fixture& fx, catalog::oid_t oid) {
        return fx.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, oid);
    }

    // Scan the WAL directory for this oid's records (system txn 0 records are
    // always in the committed view).
    std::vector<services::wal::record_t> physical_records_for(fence_fixture& fx, catalog::oid_t oid) {
        services::wal::wal_reader_t reader(fx.wal_config, fx.log);
        auto records = reader.read_committed_records(services::wal::id_t{0});
        std::vector<services::wal::record_t> out;
        for (auto& r : records) {
            if (r.is_physical() && r.table_oid == oid) {
                out.push_back(std::move(r));
            }
        }
        return out;
    }

} // namespace

// Hazard 1: DML applied by the agent AFTER the checkpoint's wal-id snapshot is
// folded into the .otbx, so the sidecar must be stamped with the highest wal id
// the agent applied for the table, not the stale snapshot — otherwise replay
// re-applies those records onto a base that already embodies them.
TEST_CASE("services::disk::checkpoint::sidecar_covers_agent_applied_wal_ids") {
    auto dir = fence_test_dir() + "/stale_sidecar";
    std::filesystem::remove_all(dir);
    {
        fence_fixture fx(dir);
        const catalog::oid_t oid = catalog::FIRST_USER_OID + 7301;
        make_disk_table(fx, oid);
        // Two committed appends: the second INSERT's wal id sits above the stale
        // snapshot taken "before" it, modeling DML applied during the checkpoint's
        // suspension window.
        append_committed(fx, oid, 0, 5);
        append_committed(fx, oid, 5, 5);
        // System (txn 0) deletes: committed-dead in place, no WAL record — the
        // INSERTs' wal ids alone are the floor the sidecar must reach.
        REQUIRE(delete_ids(fx, oid, {0, 1, 2, 3}) == 4);

        // The table's own records give the floor the sidecar must reach.
        uint64_t applied_max = 0;
        for (const auto& r : physical_records_for(fx, oid)) {
            applied_max = std::max(applied_max, static_cast<uint64_t>(r.id));
        }
        REQUIRE(applied_max > 0);

        // A deliberately stale snapshot id — below everything this table wrote —
        // models the auto-checkpoint's capture-before-suspension window.
        const services::wal::id_t stale_snapshot{1};
        REQUIRE(static_cast<uint64_t>(stale_snapshot) < applied_max);
        fx.invoke(&manager_disk_t::checkpoint_all, session_id_t{}, stale_snapshot, kMaxWatermark);

        const auto sidecar = fx.disk->peek_checkpoint_wal_id_from_disk(oid, kMainDb);
        REQUIRE(static_cast<uint64_t>(sidecar) >= applied_max);
    }
    std::filesystem::remove_all(dir);
}

// Hazard 2: checkpoint_inner's compact renumbers, then the fold fails and the
// round is deferred. The renumber survives in memory with the OLD durable
// state, so it must be fenced by a PHYSICAL_COMPACT marker (same discipline as
// the maybe_cleanup / VACUUM sites) for later positional records to replay
// against the numbering the live run actually used.
TEST_CASE("services::disk::checkpoint::failed_fold_after_compact_emits_epoch_marker") {
    auto dir = fence_test_dir() + "/failed_fold";
    std::filesystem::remove_all(dir);
    {
        fence_fixture fx(dir);
        const catalog::oid_t oid = catalog::FIRST_USER_OID + 7302;
        make_disk_table(fx, oid);
        append_committed(fx, oid, 0, 10);
        // 2/10 dead: below the commit-time gc threshold, so only the checkpoint's
        // unconditional compact reclaims them.
        REQUIRE(delete_ids(fx, oid, {0, 1}) == 2);
        REQUIRE(total_rows(fx, oid) == 10);

        arm_checkpoint_fold_failure(oid);
        fx.invoke(&manager_disk_t::checkpoint_all, session_id_t{}, services::wal::id_t{1000}, kMaxWatermark);

        // The compact ran (in-memory renumber) but the fold was deferred: no sidecar.
        REQUIRE(total_rows(fx, oid) == 8);
        REQUIRE(fx.disk->peek_checkpoint_wal_id_from_disk(oid, kMainDb) == services::wal::id_t{0});

        // WAL barrier: the marker emit is fire-and-forget on the table's DML
        // stream; this awaited txn-append is FIFO-after it.
        append_committed(fx, oid, 8, 1);

        bool marker_found = false;
        for (const auto& r : physical_records_for(fx, oid)) {
            if (r.record_type == services::wal::wal_record_type::PHYSICAL_COMPACT) {
                marker_found = true;
            }
        }
        REQUIRE(marker_found);
    }
    std::filesystem::remove_all(dir);
}

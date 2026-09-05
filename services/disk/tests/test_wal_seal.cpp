#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include "disk_test_helpers.hpp"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>

// ---------------------------------------------------------------------------
// B2 — WAL SEALING. checkpoint_all's return value is the floor handed to
// wal_worker_t::truncate_before, which DELETES every WAL segment lying entirely at or
// below it. The one invariant that matters here:
//
//     truncating the WAL must never discard a record a restart would still need.
//
// The floor that satisfies it is min(prev_checkpoint_wal_id) over EVERY entry the agents
// own — not over the entries that were checkpointed this round. checkpoint_inner defers
// entries for four documented reasons (degraded block storage, an open scan cursor,
// version stamps above the compact watermark, a checkpoint that returned an error) and a
// deferred entry keeps its old file, its old sidecar and its UNCHANGED
// prev_checkpoint_wal_id — which it still feeds into the min. That contribution is what
// pins the floor below the records the deferred table has not persisted yet.
//
// These tests own the two ends of that contract:
//   * floor_pinned_by_deferred_table — a table that does NOT get checkpointed holds the
//     floor down while every other table races ahead, across consecutive rounds, and a
//     restart proves its rows really never reached the file.
//   * no_seal_when_no_entry_reports_a_floor — the min_prev_id == max() edge case: no entry
//     reported anything, so there is no floor to seal at and checkpoint_all must answer 0
//     ("do not truncate") rather than pass the sentinel on as a truncation boundary.
// ---------------------------------------------------------------------------

using namespace services::disk;
namespace catalog = components::catalog;
using session_id_t = components::session::session_id_t;
using namespace disk_test_helpers;

namespace {
    std::string seal_dir() { return "/tmp/test_otterbrix_wal_seal_" + std::to_string(::getpid()); }

    // More than one batch, so the cursor opened below cannot drain itself on the first fetch.
    constexpr uint64_t kRowsBeforeSeal = 3 * components::vector::DEFAULT_VECTOR_CAPACITY;
    // Appended after the last checkpoint the table takes part in.
    constexpr uint64_t kRowsAfterSeal = 64;

    struct fresh_disk {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit fresh_disk(const std::filesystem::path& path)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = path;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {}
        ~fresh_disk() {
            // Destroy the manager first: its dtor joins the internal loop thread, which may
            // still enqueue children onto the scheduler.
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

        // One checkpoint round; returns the WAL floor checkpoint_all reports, i.e. the exact
        // value that would be passed to truncate_before. The watermark is always
        // "everything is visible to all" — no snapshot is open in this fixture, so the MVCC
        // gate never fires and the only deferral in play is the one a test sets up.
        services::wal::id_t checkpoint_round(services::wal::id_t wal_id) {
            return invoke(&manager_disk_t::checkpoint_all,
                          session_id_t{},
                          wal_id,
                          std::numeric_limits<uint64_t>::max());
        }
    };

    // Append `count` BIGINT rows numbered [first, first + count) to an existing table.
    void append_rows(fresh_disk& fd, catalog::oid_t table_oid, uint64_t first, uint64_t count) {
        uint64_t written = 0;
        while (written < count) {
            const uint64_t rows = std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, count - written);
            std::pmr::vector<components::types::complex_logical_type> types(&fd.resource);
            components::types::complex_logical_type t{components::types::logical_type::BIGINT};
            t.set_alias("value");
            types.push_back(std::move(t));
            components::vector::data_chunk_t chunk(&fd.resource, types, rows);
            chunk.set_cardinality(rows);
            for (uint64_t i = 0; i < rows; i++) {
                chunk.set_value(0, i, static_cast<std::int64_t>(first + written + i));
            }
            std::pmr::vector<components::vector::data_chunk_t> batch(&fd.resource);
            batch.emplace_back(std::move(chunk));
            components::execution_context_t append_ctx{session_id_t{},
                                                       components::table::transaction_data{0, 0},
                                                       {},
                                                       table_oid};
            auto r = fd.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
            REQUIRE_FALSE(r.has_error());
            written += rows;
        }
    }

    // A disk-backed user table carrying `rows` rows.
    catalog::oid_t make_seeded_table(fresh_disk& fd, uint64_t rows) {
        auto ns_oid = test_create_namespace(fd, "seal_ns");
        std::vector<components::table::column_definition_t> columns;
        columns.emplace_back("value", components::types::complex_logical_type{components::types::logical_type::BIGINT});
        auto table_oid = test_create_table(fd, ns_oid, "sealed", columns);
        REQUIRE(table_oid >= catalog::FIRST_USER_OID);
        fd.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  columns,
                  /*is_computed=*/false);
        append_rows(fd, table_oid, 0, rows);
        return table_oid;
    }

    // OPEN a cursor and read exactly ONE batch, leaving it un-drained: the agent erases the
    // active_scans_ entry only on a drain, so this is the state that gates the oid.
    uint64_t open_undrained_cursor(fresh_disk& fd, catalog::oid_t table_oid) {
        auto reply = fd.invoke(&manager_disk_t::storage_fetch_next_batch,
                               session_id_t{},
                               table_oid,
                               uint64_t{0}, // 0 == OPEN
                               std::unique_ptr<components::table::table_filter_t>(nullptr),
                               int64_t{-1},
                               std::vector<size_t>{},
                               with_open_snapshot(0, 0));
        REQUIRE_FALSE(reply.has_error());
        auto batch = std::move(reply.value());
        REQUIRE(batch.batch != nullptr);
        REQUIRE(batch.batch->size() > 0);
        return batch.cursor_id;
    }
} // namespace

// 1. SEALING INVARIANT. A table whose checkpoint did NOT happen pins the WAL floor at its
//    own number, no matter how far the tables around it advance.
//
//    The deferral lever here is the production one that is fully deterministic from the
//    outside: an un-drained fetch-next cursor. checkpoint_inner refuses to touch an oid with
//    a live cursor (the cursor holds an absolute row position into the un-swapped
//    collection), so that entry keeps its file, its sidecar and its prev_checkpoint_wal_id
//    while every other table in the round moves on.
//
//    Timeline (the wal ids are what checkpoint_all is told the WAL has reached):
//      round 1 @ 100 -> every table: prev 0,   current 100.  floor 0 ("do not truncate").
//      round 2 @ 200 -> every table: prev 100, current 200.  floor 100.
//      rows are appended to the user table and a cursor is opened on it and abandoned.
//      round 3 @ 300 -> user table DEFERRED (prev 100, current 200);
//                       everything else: prev 200, current 300.  floor must be 100.
//      round 4 @ 400 -> user table DEFERRED again;
//                       everything else: prev 300, current 400.  floor must STILL be 100.
//
//    100 is the deferred table's own contribution, and dropping it from the min is what
//    "sealing advanced too far" looks like: the round would report 200, then 300, and
//    truncate_before would delete the segments carrying the appends the deferred table has
//    not persisted. The restart at the end proves those appends really are only in the WAL.
TEST_CASE("services::disk::wal_seal::floor_pinned_by_deferred_table") {
    auto dir = seal_dir() + "/deferred";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    catalog::oid_t table_oid = catalog::INVALID_OID;

    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        table_oid = make_seeded_table(fd, kRowsBeforeSeal);

        // Round 1. No table has a superseded root yet, so prev is 0 everywhere and the floor
        // is 0 — which the WAL side reads as "do not truncate". A first checkpoint never seals.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{100}) == services::wal::id_t{0});

        // Round 2. Now every table has a superseded root taken at 100, so 100 is the floor.
        // It is NOT 200: the records between 100 and 200 are still live for any table whose
        // next round dies before its header commit and reopens the superseded root.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{200}) == services::wal::id_t{100});

        // Rows that no checkpoint has folded into the file yet.
        append_rows(fd, table_oid, kRowsBeforeSeal, kRowsAfterSeal);

        // Abandon a cursor on the table: from here on checkpoint_inner defers this oid.
        const auto cursor_id = open_undrained_cursor(fd, table_oid);
        REQUIRE(cursor_id != 0);
        REQUIRE(fd.manager->has_active_scan_for_oid_sync(table_oid));

        // Round 3: the deferred table holds the floor at its own prev while the rest of the
        // catalog moves to prev 200.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{300}) == services::wal::id_t{100});

        // Round 4: the deferred entry still has not moved, so neither may the floor — even
        // though the tables around it have now reached prev 300.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{400}) == services::wal::id_t{100});

        // The premise, stated against the entry itself: its durable root is still the one
        // committed in round 2. Rounds 3 and 4 wrote nothing for it.
        REQUIRE(fd.manager->peek_checkpoint_wal_id_from_disk(table_oid,
                                                             catalog::well_known_oid::main_database) ==
                services::wal::id_t{200});
    }

    // And the premise proven the hard way: a fresh manager over the same directory reopens
    // the table from that round-2 root, and the rows appended afterwards are simply not
    // there. They exist only in the WAL — which is exactly why the floor had to stay at 100.
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        fd2.manager->load_user_table_storages_sync();

        const auto durable_rows = fd2.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid);
        REQUIRE(durable_rows == kRowsBeforeSeal);
    }

    std::filesystem::remove_all(dir);
}

// 2. EDGE CASE — min_prev_id == max(). The agents exist but own no checkpointable entry, so
//    not one of them reports a WAL floor and the cross-agent min stays at the sentinel.
//    Sealing on it would hand truncate_before wal::id_t max(), i.e. authorize deleting the
//    entire WAL. checkpoint_all must answer 0, the "do not truncate" value, instead.
TEST_CASE("services::disk::wal_seal::no_seal_when_no_entry_reports_a_floor") {
    auto dir = seal_dir() + "/empty";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    {
        // No bootstrap: the agents are spawned (the config path is non-empty) but their
        // storages_ slices are empty, so checkpoint_inner iterates nothing and returns the
        // max() sentinel from every one of them.
        fresh_disk fd(dir);
        REQUIRE(fd.checkpoint_round(services::wal::id_t{500}) == services::wal::id_t{0});
        // Not the sentinel, and not the current wal id either — both would be a truncation
        // boundary above records nothing has persisted.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{500}) !=
                std::numeric_limits<services::wal::id_t>::max());
    }

    std::filesystem::remove_all(dir);
}

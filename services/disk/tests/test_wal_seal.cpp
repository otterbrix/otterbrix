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

// WAL SEALING invariant: checkpoint_all's floor feeds wal_worker_t::truncate_before, which
// deletes every segment at or below it. The floor must be min(prev_checkpoint_wal_id) over
// EVERY entry the agents own, not just the ones checkpointed this round — a deferred entry's
// UNCHANGED prev still feeds that min, pinning the floor below its unpersisted records. See
// services/disk/tests/test_checkpoint_dirty.cpp for the unchanged-table path that also
// contributes to this min without rewriting a file.

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

        // Returns checkpoint_all's WAL floor. Watermark is max() (no open snapshot), so the
        // only deferral in play is whatever the test sets up.
        services::wal::id_t checkpoint_round(services::wal::id_t wal_id) {
            return invoke(&manager_disk_t::checkpoint_all,
                          session_id_t{},
                          wal_id,
                          std::numeric_limits<uint64_t>::max());
        }
    };

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

    // Un-drained: the agent erases the active_scans_ entry only on a drain.
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

// SEALING INVARIANT: a table deferred by a live fetch-next cursor pins the WAL floor at its
// own prev_checkpoint_wal_id across rounds while every other table advances; a restart proves
// the appends made after that floor really are only in the WAL.
TEST_CASE("services::disk::wal_seal::floor_pinned_by_deferred_table") {
    auto dir = seal_dir() + "/deferred";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    catalog::oid_t table_oid = catalog::INVALID_OID;

    {
        fresh_disk fd(dir);
        fd.manager->bootstrap_system_tables_sync();
        table_oid = make_seeded_table(fd, kRowsBeforeSeal);

        // First checkpoint never seals: no superseded root yet, floor 0.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{100}) == services::wal::id_t{0});

        // Unchanged since round 1, so this rewrites nothing but still advances prev to 100.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{200}) == services::wal::id_t{100});

        append_rows(fd, table_oid, kRowsBeforeSeal, kRowsAfterSeal);

        // From here on checkpoint_inner defers this oid.
        const auto cursor_id = open_undrained_cursor(fd, table_oid);
        REQUIRE(cursor_id != 0);
        REQUIRE(fd.manager->has_active_scan_for_oid_sync(table_oid));

        // Deferred table holds the floor at 100 while the rest of the catalog moves to 200.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{300}) == services::wal::id_t{100});

        // Still deferred, so the floor must stay at 100 despite the rest reaching 300.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{400}) == services::wal::id_t{100});

        // Durable root is still the one committed in round 2.
        auto peeked = fd.manager->peek_checkpoint_wal_id_from_disk(table_oid,
                                                                    catalog::well_known_oid::main_database);
        REQUIRE_FALSE(peeked.has_error());
        REQUIRE(peeked.value() == services::wal::id_t{200});
    }

    // Proof: a fresh manager reopens the table from the round-2 root; rows appended after it
    // exist only in the WAL.
    {
        fresh_disk fd2(dir);
        fd2.manager->bootstrap_system_tables_sync();
        fd2.manager->restore_oid_generator_sync();
        fd2.manager->load_user_table_storages_sync();

        const auto durable_rows =
            disk_test_helpers::read_ok(fd2.invoke(&manager_disk_t::storage_total_rows, session_id_t{}, table_oid));
        REQUIRE(durable_rows == kRowsBeforeSeal);
    }

    std::filesystem::remove_all(dir);
}

// EDGE CASE: no entry reports a floor, so the cross-agent min stays at the max() sentinel.
// checkpoint_all must answer 0, not the sentinel — sealing on it would authorize deleting the
// entire WAL.
TEST_CASE("services::disk::wal_seal::no_seal_when_no_entry_reports_a_floor") {
    auto dir = seal_dir() + "/empty";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    {
        // No bootstrap: storages_ is empty, so checkpoint_inner returns the max() sentinel.
        fresh_disk fd(dir);
        REQUIRE(fd.checkpoint_round(services::wal::id_t{500}) == services::wal::id_t{0});
        // Not the current wal id either — that would also be a truncation boundary above
        // records nothing has persisted.
        REQUIRE(fd.checkpoint_round(services::wal::id_t{500}) !=
                std::numeric_limits<services::wal::id_t>::max());
    }

    std::filesystem::remove_all(dir);
}

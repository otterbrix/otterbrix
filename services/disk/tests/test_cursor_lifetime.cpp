#include <catch2/catch_test_macros.hpp>

// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
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
#include <thread>
#include <unistd.h>

// A4: an ABANDONED fetch-next cursor must be releasable.
//
// storage_fetch_next_batch mints a cursor on the owning agent and erases it only along the
// DRAIN paths — the source has to keep pulling until the scan runs out. A source that stops
// early (an error mid-pump, a satisfied LIMIT, a dropped sub-plan) never gets there, so its
// active_scans_ entry used to live for the life of the process. That entry gates compact():
// the three compact sites skip any oid with a live cursor, because the cursor holds an
// absolute row position into the un-swapped collection. The comment there promises the table
// "compacts once the cursor drains" — which, for an abandoned cursor, is never.
//
// storage_close_cursor is the release leg. These cases pin both halves of its contract: an
// open cursor keeps the gate up, and closing it takes the gate back down.

using namespace services::disk;
using namespace disk_test_helpers;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;
using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::data_chunk_t;

namespace {

    std::string cursor_dir() {
        static std::string p = "/tmp/test_otterbrix_cursor_lifetime_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(cursor_dir()); }

    struct fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = cursor_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(cursor_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            manager.reset();
            scheduler->stop();
            delete scheduler;
            cleanup();
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
    };

    // A table with more rows than ONE batch can carry: the cursor is still open after the
    // first reply, which is the state an abandoned source leaves behind.
    catalog::oid_t make_seeded_table(fixture& fx) {
        auto ns_oid = test_create_namespace(fx, "nscursor");
        std::vector<components::table::column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type{logical_type::BIGINT});
        auto table_oid = test_create_table(fx, ns_oid, "rows", columns);
        REQUIRE(table_oid >= FIRST_USER_OID);
        fx.invoke(&manager_disk_t::create_storage_disk,
                  session_id_t{},
                  table_oid,
                  catalog::well_known_oid::main_database,
                  columns,
                  /*is_computed=*/false);

        constexpr uint64_t kRows = 3 * components::vector::DEFAULT_VECTOR_CAPACITY;
        uint64_t written = 0;
        while (written < kRows) {
            const uint64_t rows =
                std::min<uint64_t>(components::vector::DEFAULT_VECTOR_CAPACITY, kRows - written);
            std::pmr::vector<complex_logical_type> types(&fx.resource);
            complex_logical_type t{logical_type::BIGINT};
            t.set_alias("value");
            types.push_back(std::move(t));
            auto chunk = std::make_unique<data_chunk_t>(&fx.resource, types, rows);
            chunk->set_cardinality(rows);
            for (uint64_t i = 0; i < rows; i++) {
                chunk->set_value(0, i, static_cast<std::int64_t>(written + i));
            }
            std::pmr::vector<data_chunk_t> batch(&fx.resource);
            batch.emplace_back(std::move(*chunk));
            components::execution_context_t append_ctx{session_id_t{},
                                                       components::table::transaction_data{0, 0},
                                                       {},
                                                       table_oid};
            auto r = fx.invoke(&manager_disk_t::storage_append, append_ctx, table_oid, std::move(batch));
            REQUIRE_FALSE(r.has_error());
            written += rows;
        }
        return table_oid;
    }

    // OPEN a cursor and read exactly ONE batch, leaving it un-drained.
    uint64_t open_undrained_cursor(fixture& fx, catalog::oid_t table_oid) {
        auto reply = fx.invoke(&manager_disk_t::storage_fetch_next_batch,
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
        // A drained reply would defeat the whole point: the agent erases the entry itself then.
        REQUIRE(batch.batch->size() > 0);
        REQUIRE(batch.cursor_id != 0);
        return batch.cursor_id;
    }

} // namespace

TEST_CASE("services::disk::cursor_lifetime::open_cursor_holds_the_compact_gate") {
    fixture fx;
    auto table_oid = make_seeded_table(fx);
    auto cursor_id = open_undrained_cursor(fx, table_oid);
    REQUIRE(cursor_id != 0);

    // The premise the release leg exists for: an un-drained cursor gates compact on its table.
    INFO("an open fetch-next cursor must gate compact on its table");
    CHECK(fx.manager->has_active_scan_for_oid_sync(table_oid));
}

TEST_CASE("services::disk::cursor_lifetime::closing_an_abandoned_cursor_lifts_the_compact_gate") {
    fixture fx;
    auto table_oid = make_seeded_table(fx);
    auto cursor_id = open_undrained_cursor(fx, table_oid);

    REQUIRE(fx.manager->has_active_scan_for_oid_sync(table_oid));

    // Walk away from the cursor without draining it, then release it explicitly. Without the
    // release leg this state is unreachable: the gate stays up for the life of the process and
    // the table never compacts again.
    fx.invoke(&manager_disk_t::storage_close_cursor, session_id_t{}, table_oid, cursor_id);

    INFO("releasing an abandoned cursor must lift the compact gate");
    CHECK_FALSE(fx.manager->has_active_scan_for_oid_sync(table_oid));
}

TEST_CASE("services::disk::cursor_lifetime::closing_a_cursor_is_idempotent") {
    fixture fx;
    auto table_oid = make_seeded_table(fx);

    // Idempotence is part of the contract: the drain paths erase the entry themselves, so a
    // source that drains AND then releases must not be an error. A never-minted id is the
    // same shape.
    fx.invoke(&manager_disk_t::storage_close_cursor, session_id_t{}, table_oid, uint64_t{424242});
    CHECK_FALSE(fx.manager->has_active_scan_for_oid_sync(table_oid));

    auto cursor_id = open_undrained_cursor(fx, table_oid);
    fx.invoke(&manager_disk_t::storage_close_cursor, session_id_t{}, table_oid, cursor_id);
    fx.invoke(&manager_disk_t::storage_close_cursor, session_id_t{}, table_oid, cursor_id);
    CHECK_FALSE(fx.manager->has_active_scan_for_oid_sync(table_oid));
}

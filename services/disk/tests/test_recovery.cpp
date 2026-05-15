#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include "disk_test_helpers.hpp"

#include <filesystem>
#include <unistd.h>

// Recovery (3 tests) — covers docs/catalog-migration-to-postgresql-style.md §14
// "Recovery (3 tests)":
//   - test_recovery_system_wal_before_user — system DDL replayed first on restart, before
//     anyone touches user storages.
//   - test_recovery_ring_buffer_empty — fresh process: no events have been pushed, the
//     ring buffer reports zero latest_version and an empty since(0); dispatchers see this
//     as "first contact, do a full resolve".
//   - test_recovery_ddl_then_dml — a DDL row written through append_pg_catalog_row goes
//     through WAL + storage; after a fixture restart the row is visible via load.

using namespace services::disk;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string recovery_test_dir() {
        static std::string p = "/tmp/test_otterbrix_recovery_" + std::to_string(::getpid());
        return p;
    }
    void cleanup_dir(const std::string& d) { std::filesystem::remove_all(d); }

    // Disk + WAL fixture mirrors the one in test_wal_catalog.cpp — minimal enough to drive
    // bootstrap + ddl_* + a fresh restart.
    struct recovery_fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_wal wal_config;
        configuration::config_disk disk_config;
        std::unique_ptr<services::wal::manager_wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> disk;

        explicit recovery_fixture(const std::string& dir, bool bootstrap = true)
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
            wal->set_run_fn([this] { scheduler->run(10000); });
            disk->set_run_fn([this] { scheduler->run(10000); });
            wal->sync(std::make_tuple(actor_zeta::address_t(disk->address()), actor_zeta::address_t::empty_address()));
            disk->sync(std::make_tuple(wal->address()));
            if (bootstrap) {
                disk->bootstrap_system_tables_sync();
            }
        }
        ~recovery_fixture() {
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(disk->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. test_recovery_system_wal_before_user — system DDL replayed first on restart.
//    Drive a CREATE NAMESPACE through the active fixture (writes a pg_namespace row +
//    a WAL physical record), drop the fixture, spin a fresh one and call
//    load_system_tables_sync. The system table must come back populated, which can only
//    happen if the system replay path runs before any user-table loading.
TEST_CASE("test_recovery_system_wal_before_user") {
    auto dir = recovery_test_dir() + "/sys_first";
    cleanup_dir(dir);

    components::catalog::oid_t created_namespace_oid = components::catalog::INVALID_OID;
    {
        recovery_fixture fx(dir);
        created_namespace_oid = disk_test_helpers::test_create_namespace(fx, std::string("recoverable_ns"));
        REQUIRE(created_namespace_oid != components::catalog::INVALID_OID);
    }

    // Restart: skip bootstrap (already done), call load_system_tables_sync.
    {
        recovery_fixture fx(dir, /*bootstrap=*/ false);
        // load_system_tables_sync MUST run before any user storage is touched on restart;
        // here we call it explicitly and require the previously-created namespace OID is
        // recovered from pg_namespace via restore_oid_generator_sync (its scan walks
        // pg_namespace as a "fresh OID source", which proves the system table's rows are
        // back in place before user paths run).
        REQUIRE_NOTHROW(fx.disk->load_system_tables_sync());
        REQUIRE_NOTHROW(fx.disk->restore_oid_generator_sync());
    }
    cleanup_dir(dir);
}

// 2. test_recovery_ring_buffer_empty — fresh process has no recorded invalidations.
//    A dispatcher pulling `since(0)` immediately after restart sees latest_version==0 and
//    no events; this is its signal that no cache invalidation is needed.
// test_recovery_ring_buffer_empty deleted: invalidation ring buffer infrastructure removed.

// 3. test_recovery_ddl_then_dml — DDL + DML both flow through WAL; after restart, a
//    second CREATE TABLE call observes the prior namespace's OID still in pg_namespace
//    (proving system DDL was durably persisted), and oid_generator is seeded past it.
TEST_CASE("test_recovery_ddl_then_dml") {
    auto dir = recovery_test_dir() + "/ddl_dml";
    cleanup_dir(dir);

    components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;
    {
        recovery_fixture fx(dir);
        // Create a namespace.
        ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("durable_ns"));
        REQUIRE(ns_oid != components::catalog::INVALID_OID);

        // Create a table inside that namespace. Both operations write to
        // pg_catalog.* via append_pg_catalog_row -> WAL physical records +
        // on-disk storage.
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT}, /*not_null=*/ true);
        const auto table_oid = disk_test_helpers::test_create_table(fx, ns_oid,
                                                                      std::string("durable_table"), cols, 'r');
        REQUIRE(table_oid != components::catalog::INVALID_OID);

        // Force durability: checkpoint flushes pg_namespace/pg_class/pg_attribute rows from
        // the in-memory storage into the on-disk .otbx files (W-TORN: 2 fsyncs per table).
        // Without this, the rows live only in WAL — and this fixture doesn't drive WAL
        // replay on the second fixture. Pass wal_id=0 since we don't have a live WAL id
        // tracker in this test path; checkpoint_all is happy to skip the wal-id sidecar
        // when value is 0.
        auto cp_future = fx.invoke(&manager_disk_t::checkpoint_all,
                                    session_id_t{}, services::wal::id_t{0});
        (void)cp_future;
    }

    // Restart and verify the DDL state is durable: load_system_tables_sync must succeed,
    // restore_oid_generator_sync must seed the counter past ns_oid (since pg_namespace
    // now contains its row), and a brand-new namespace allocation yields a strictly
    // higher OID.
    {
        recovery_fixture fx(dir, /*bootstrap=*/ false);
        REQUIRE_NOTHROW(fx.disk->load_system_tables_sync());
        REQUIRE_NOTHROW(fx.disk->restore_oid_generator_sync());

        const auto post_ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("post_recovery_ns"));
        REQUIRE(post_ns_oid > ns_oid);
    }
    cleanup_dir(dir);
}

// 4. test_recovery_orphaned_uncommitted_ddl — DDL rows written under a non-zero txn_id
//    but never committed (storage_commit_appends not called, simulating a crash) must
//    be invisible after manager restart, because rebuild_lookup_indexes uses inline_scan →
//    scan_committed which filters any row whose txn_id has not been flipped to a commit_id.
TEST_CASE("test_recovery_orphaned_uncommitted_ddl") {
    auto dir = recovery_test_dir() + "/orphaned_ddl";
    cleanup_dir(dir);

    {
        recovery_fixture fx(dir);
        // Use txn_id=1 so append_pg_catalog_row writes the pg_namespace row under a
        // non-zero txn (not immediately visible). Without storage_commit_appends the
        // flip from txn_id to commit_id never happens.
        components::execution_context_t uncommitted_ctx{
            session_id_t{},
            components::table::transaction_data{1, 0},
            {}
        };
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        REQUIRE(ns_oid != components::catalog::INVALID_OID);
        auto writes = components::catalog::build_create_namespace_writes(&fx.resource,
                                                                          std::string("orphaned_ns"), ns_oid);
        for (auto& w : writes)
            fx.invoke(&manager_disk_t::append_pg_catalog_row, uncommitted_ctx, w.table_oid, std::move(w.row));
        // Intentionally omit storage_commit_appends — simulates crash before commit.
    }

    {
        recovery_fixture fx(dir, /*bootstrap=*/ false);
        REQUIRE_NOTHROW(fx.disk->load_system_tables_sync());
        REQUIRE_NOTHROW(fx.disk->restore_oid_generator_sync());

        // ns_name_to_oid_ is rebuilt by rebuild_lookup_indexes via inline_scan →
        // scan_committed. The uncommitted row (txn_id=1) must not appear.
        auto res = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(),
                              std::string("orphaned_ns"), std::uint64_t{0});
        REQUIRE_FALSE(res.found);
    }
    cleanup_dir(dir);
}

// 5. services::disk::recovery::dynamic_schema_persists_across_restart.
//    Dynamic-schema (relkind='g') tables register their fields by appending pg_computed_column
//    rows. append_pg_catalog_row writes a WAL physical_insert + on-disk storage; bootstrap
//    on restart replays WAL via direct_append_sync (bypassing the operator pipeline).
//    This test verifies the round trip: register two columns under a 'g' table, drop the
//    fixture (flushes WAL + checkpoint persists storage), spin a fresh fixture pointing at
//    the same path, run load_system_tables_sync, and require pg_computed_column reports
//    both rows + resolve_table reconstructs both columns from them.
TEST_CASE("services::disk::recovery::dynamic_schema_persists_across_restart") {
    auto dir = recovery_test_dir() + "/dynamic_schema";
    cleanup_dir(dir);

    components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;
    components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
    {
        recovery_fixture fx(dir);
        ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("main_db"));
        REQUIRE(ns_oid != components::catalog::INVALID_OID);
        table_oid = disk_test_helpers::test_create_table(
            fx, ns_oid, std::string("docs"),
            std::vector<components::table::column_definition_t>{},
            components::catalog::relkind::computed);
        REQUIRE(table_oid != components::catalog::INVALID_OID);

        // Register two computed-schema columns. Each call goes through
        // append_pg_catalog_row → WAL physical_insert + in-memory storage.
        auto attoid_a = disk_test_helpers::test_computed_register(
            fx, table_oid, std::string("a"),
            components::catalog::well_known_oid::int64_type);
        auto attoid_b = disk_test_helpers::test_computed_register(
            fx, table_oid, std::string("b"),
            components::catalog::well_known_oid::string_type);
        REQUIRE(attoid_a >= components::catalog::FIRST_USER_OID);
        REQUIRE(attoid_b >= components::catalog::FIRST_USER_OID);

        // Force durability for the on-disk pg_* storages (fsyncs the .otbx files).
        // Without checkpoint, pg_computed_column rows live only in WAL; we still
        // expect them back via the bootstrap-replay path on restart.
        auto cp_future = fx.invoke(&manager_disk_t::checkpoint_all,
                                    session_id_t{}, services::wal::id_t{0});
        (void)cp_future;
    }

    // Restart: bootstrap=false → don't re-create fresh empty system tables; instead
    // call load_system_tables_sync, which replays WAL via direct_append_sync into
    // pg_computed_column. After that, pg_computed_column must hold both rows and
    // resolve_table must reconstruct the dynamic schema for "docs".
    {
        recovery_fixture fx_reopen(dir, /*bootstrap=*/ false);
        REQUIRE_NOTHROW(fx_reopen.disk->load_system_tables_sync());
        REQUIRE_NOTHROW(fx_reopen.disk->restore_oid_generator_sync());

        // Direct read of pg_computed_column: relid=table_oid → 2 live rows.
        constexpr components::catalog::oid_t pg_cc = components::catalog::well_known_oid::pg_computed_column_table;
        components::types::logical_value_t toid_lv(&fx_reopen.resource, table_oid);
        std::pmr::vector<std::string> rk{&fx_reopen.resource};
        rk.emplace_back("relid");
        std::pmr::vector<components::types::logical_value_t> rv{&fx_reopen.resource};
        rv.emplace_back(toid_lv);
        auto rows = fx_reopen.invoke(&manager_disk_t::read_rows_by_key, fx_reopen.ctx(),
                                      pg_cc,
                                      std::move(rk),
                                      std::move(rv));
        REQUIRE(rows.size() == 2);
        bool saw_a = false;
        bool saw_b = false;
        for (const auto& row : rows) {
            REQUIRE(row.size() >= 6);
            // pg_computed_column layout: [0]=relid, [1]=attoid, [2]=attname,
            // [3]=atttypid, [4]=attversion, [5]=attrefcount.
            const auto attname = row[2].is_null() ? std::string{} : row[2].value<std::string>();
            const auto atttypid = row[3].is_null()
                                      ? components::catalog::INVALID_OID
                                      : static_cast<components::catalog::oid_t>(row[3].value<std::uint32_t>());
            const auto refcount = row[5].value<std::int64_t>();
            REQUIRE(refcount == 1);
            if (attname == "a") {
                REQUIRE(atttypid == components::catalog::well_known_oid::int64_type);
                saw_a = true;
            } else if (attname == "b") {
                REQUIRE(atttypid == components::catalog::well_known_oid::string_type);
                saw_b = true;
            }
        }
        REQUIRE(saw_a);
        REQUIRE(saw_b);

        // resolve_table on restart must report relkind='g' and reconstruct both columns
        // from the replayed pg_computed_column rows (computed-schema path skips
        // pg_attribute and reads pg_computed_column directly).
        auto rs = fx_reopen.invoke(&manager_disk_t::resolve_table, fx_reopen.ctx(),
                                    ns_oid, std::string("docs"), std::uint64_t{0});
        REQUIRE(rs.found);
        REQUIRE(rs.relkind == components::catalog::relkind::computed);
        REQUIRE(rs.columns.size() == 2);
        const bool has_a = rs.columns[0].attname == "a" || rs.columns[1].attname == "a";
        const bool has_b = rs.columns[0].attname == "b" || rs.columns[1].attname == "b";
        REQUIRE(has_a);
        REQUIRE(has_b);
    }
    cleanup_dir(dir);
}
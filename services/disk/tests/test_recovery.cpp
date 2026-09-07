#include <catch2/catch_test_macros.hpp>
#include <components/context/context.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
// actor-zeta/spawn.hpp uses std::unique_ptr but does not include <memory>
#include <memory>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/manager_wal_replicate.hpp>

#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>

using namespace services::disk;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string recovery_test_dir() {
        static std::string p = "/tmp/test_otterbrix_recovery_" + std::to_string(::getpid());
        return p;
    }
    void cleanup_dir(const std::string& d) { std::filesystem::remove_all(d); }

    struct recovery_fixture {
        core::pmr::otterbrix_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_wal wal_config;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> disk;
        std::unique_ptr<services::wal::manager_wal_replicate_t, actor_zeta::pmr::deleter_t> wal;

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
            , disk(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log))
            , wal(actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource,
                                                                            scheduler,
                                                                            wal_config,
                                                                            log,
                                                                            disk->address(),
                                                                            components::pipeline::no_mailbox())) {
            std::filesystem::create_directories(dir);
            disk->set_manager_wal_sync(wal->address());
            if (bootstrap) {
                disk->bootstrap_system_tables_sync();
            }
        }
        ~recovery_fixture() {
            // Destroy the managers first: each dtor joins its loop thread, which may still enqueue onto the scheduler.
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

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// System DDL must replay before user tables, or restore_oid_generator_sync finds nothing to seed from.
TEST_CASE("test_recovery_system_wal_before_user") {
    auto dir = recovery_test_dir() + "/sys_first";
    cleanup_dir(dir);

    components::catalog::oid_t created_namespace_oid = components::catalog::INVALID_OID;
    {
        recovery_fixture fx(dir);
        created_namespace_oid = disk_test_helpers::test_create_namespace(fx, std::string("recoverable_ns"));
        REQUIRE(created_namespace_oid != components::catalog::INVALID_OID);
    }

    {
        recovery_fixture fx(dir, /*bootstrap=*/false);
        REQUIRE_NOTHROW(fx.disk->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fx.disk->restore_oid_generator_sync());
    }
    cleanup_dir(dir);
}

// test_recovery_ring_buffer_empty deleted: invalidation ring buffer infrastructure removed.

// A second CREATE TABLE landing above ns_oid after restart proves the namespace row and OID counter both survived.
TEST_CASE("test_recovery_ddl_then_dml") {
    auto dir = recovery_test_dir() + "/ddl_dml";
    cleanup_dir(dir);

    components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;
    {
        recovery_fixture fx(dir);
        ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("durable_ns"));
        REQUIRE(ns_oid != components::catalog::INVALID_OID);

        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id",
                          components::types::complex_logical_type{components::types::logical_type::BIGINT},
                          /*not_null=*/true);
        const auto table_oid =
            disk_test_helpers::test_create_table(fx, ns_oid, std::string("durable_table"), cols, 'r');
        REQUIRE(table_oid != components::catalog::INVALID_OID);

        // wal_id=0 is fine here — checkpoint_all skips the wal-id sidecar when the value is 0.
        auto cp_future = fx.invoke(&manager_disk_t::checkpoint_all,
                                   session_id_t{},
                                   services::wal::id_t{0},
                                   std::numeric_limits<uint64_t>::max());
        (void) cp_future;
    }

    {
        recovery_fixture fx(dir, /*bootstrap=*/false);
        REQUIRE_NOTHROW(fx.disk->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fx.disk->restore_oid_generator_sync());

        const auto post_ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("post_recovery_ns"));
        REQUIRE(post_ns_oid > ns_oid);
    }
    cleanup_dir(dir);
}

// An uncommitted DDL row (txn_id never flipped to commit_id) must stay invisible; scan_committed filters it out.
TEST_CASE("test_recovery_orphaned_uncommitted_ddl") {
    auto dir = recovery_test_dir() + "/orphaned_ddl";
    cleanup_dir(dir);

    {
        recovery_fixture fx(dir);
        components::execution_context_t uncommitted_ctx{session_id_t{}, components::table::transaction_data{1, 0}, {}};
        auto oids = fx.invoke(&manager_disk_t::allocate_oids_batch, std::size_t{1});
        const components::catalog::oid_t ns_oid = oids[0];
        REQUIRE(ns_oid != components::catalog::INVALID_OID);
        auto writes =
            components::catalog::build_create_namespace_writes(&fx.resource, std::string("orphaned_ns"), ns_oid);
        for (auto& w : writes)
            disk_test_helpers::append_ok(fx.invoke(&manager_disk_t::append_pg_catalog_row,
                                                   uncommitted_ctx,
                                                   w.table_oid,
                                                   std::move(w.row)));
        // Intentionally omit storage_publish_commits — simulates crash before commit.
    }

    {
        recovery_fixture fx(dir, /*bootstrap=*/false);
        REQUIRE_NOTHROW(fx.disk->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fx.disk->restore_oid_generator_sync());

        auto res =
            fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("orphaned_ns"));
        REQUIRE_FALSE(res.has_error());
        REQUIRE_FALSE(res.value().found);
    }
    cleanup_dir(dir);
}

// 'g' columns replay via pg_computed_column through direct_append_sync, bypassing the operator pipeline.
TEST_CASE("services::disk::recovery::dynamic_schema_persists_across_restart") {
    auto dir = recovery_test_dir() + "/dynamic_schema";
    cleanup_dir(dir);

    components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;
    components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
    {
        recovery_fixture fx(dir);
        ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("main_db"));
        REQUIRE(ns_oid != components::catalog::INVALID_OID);
        table_oid = disk_test_helpers::test_create_table(fx,
                                                         ns_oid,
                                                         std::string("docs"),
                                                         std::vector<components::table::column_definition_t>{},
                                                         components::catalog::relkind::computed);
        REQUIRE(table_oid != components::catalog::INVALID_OID);

        auto attoid_a = disk_test_helpers::test_computed_register(fx,
                                                                  table_oid,
                                                                  std::string("a"),
                                                                  components::catalog::well_known_oid::int64_type);
        auto attoid_b = disk_test_helpers::test_computed_register(fx,
                                                                  table_oid,
                                                                  std::string("b"),
                                                                  components::catalog::well_known_oid::string_type);
        REQUIRE(attoid_a >= components::catalog::FIRST_USER_OID);
        REQUIRE(attoid_b >= components::catalog::FIRST_USER_OID);

        // Checkpoint forces pg_computed_column durable on disk here, not just in WAL, before the restart.
        auto cp_future = fx.invoke(&manager_disk_t::checkpoint_all,
                                   session_id_t{},
                                   services::wal::id_t{0},
                                   std::numeric_limits<uint64_t>::max());
        (void) cp_future;
    }

    {
        recovery_fixture fx_reopen(dir, /*bootstrap=*/false);
        REQUIRE_NOTHROW(fx_reopen.disk->bootstrap_system_tables_sync());
        REQUIRE_NOTHROW(fx_reopen.disk->restore_oid_generator_sync());

        constexpr components::catalog::oid_t pg_cc = components::catalog::well_known_oid::pg_computed_column_table;
        components::types::logical_value_t toid_lv(&fx_reopen.resource, table_oid);
        std::pmr::vector<std::uint64_t> rk{&fx_reopen.resource};
        rk.emplace_back(components::catalog::pg_computed_column_col::relid);
        std::pmr::vector<components::types::logical_value_t> rv{&fx_reopen.resource};
        rv.emplace_back(toid_lv);
        auto batches =
            disk_test_helpers::read_ok(fx_reopen.invoke(&manager_disk_t::read_chunks_by_key,
                                                        fx_reopen.ctx(),
                                                        pg_cc,
                                                        std::move(rk),
                                                        test_probe::build_key_chunk(&fx_reopen.resource, std::move(rv)),
                                                        std::pmr::vector<std::uint64_t>{&fx_reopen.resource}));
        std::uint64_t total = 0;
        for (const auto& c : batches) total += c.size();
        REQUIRE(total == 2);
        bool saw_a = false;
        bool saw_b = false;
        for (const auto& chunk : batches) {
            REQUIRE(chunk.column_count() >= 7);
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                // pg_computed_column layout: [0]=relid [1]=attoid [2]=attname [3]=atttypid [4]=atttypspec
                // [5]=attversion [6]=attrefcount.
                const auto attname =
                    chunk.value(2, i).is_null() ? std::string{} : std::string(chunk.get_value<std::string_view>(2, i));
                const auto atttypid =
                    chunk.value(3, i).is_null()
                        ? components::catalog::INVALID_OID
                        : static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                const auto refcount = chunk.get_value<std::int64_t>(6, i);
                REQUIRE(refcount == 1);
                if (attname == "a") {
                    REQUIRE(atttypid == components::catalog::well_known_oid::int64_type);
                    saw_a = true;
                } else if (attname == "b") {
                    REQUIRE(atttypid == components::catalog::well_known_oid::string_type);
                    saw_b = true;
                }
            }
        }
        REQUIRE(saw_a);
        REQUIRE(saw_b);

        // The computed-schema path skips pg_attribute and reconstructs columns straight from pg_computed_column.
        auto rs = test_probe::probe_table(fx_reopen, fx_reopen.ctx(), ns_oid, std::string("docs"));
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
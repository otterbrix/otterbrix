#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <unistd.h>

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string lookup_test_dir() {
        static std::string p = "/tmp/test_otterbrix_lookup_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(lookup_test_dir()); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit fixture(bool bootstrap = true)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = lookup_test_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(lookup_test_dir());
            manager->set_run_fn([this] { scheduler->run(10000); });
            if (bootstrap)
                manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke_async(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };

    // Reload fixture: simulates restart. Does NOT call bootstrap; calls load_system_tables_sync.
    struct reload_fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        explicit reload_fixture(const std::string& dir)
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = dir;
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            manager->set_run_fn([this] { scheduler->run(10000); });
            manager->load_system_tables_sync();
            manager->restore_oid_generator_sync();
        }
        ~reload_fixture() {
            scheduler->stop();
            delete scheduler;
        }

        template<typename Fn, typename... Args>
        auto invoke_async(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. After bootstrap, "public" namespace is in the index (resolve_namespace uses hash map).
TEST_CASE("services::disk::lookup::bootstrap_populates_ns_index") {
    fixture fx;
    auto r = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(),
                              std::string("public"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::public_namespace);
}

// 2. After ddl_create_namespace, the new namespace is findable via O(1) index.
TEST_CASE("services::disk::lookup::create_ns_updates_index") {
    fixture fx;
    auto cr = fx.invoke_async(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("myns"));
    REQUIRE(cr.created_oid >= FIRST_USER_OID);

    auto r = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(),
                              std::string("myns"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == cr.created_oid);
}

// 3. After ddl_drop_namespace, it's no longer in the index.
TEST_CASE("services::disk::lookup::drop_ns_removes_from_index") {
    fixture fx;
    auto cr = fx.invoke_async(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("dropme"));
    fx.invoke_async(&manager_disk_t::ddl_drop_namespace, fx.ctx(), cr.created_oid,
                    drop_behavior_t::restrict_);

    auto r = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(),
                              std::string("dropme"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 4. After ddl_create_table, the table is findable via O(1) index.
TEST_CASE("services::disk::lookup::create_table_updates_index") {
    fixture fx;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});

    auto cr = fx.invoke_async(&manager_disk_t::ddl_create_table, fx.ctx(),
                               well_known_oid::public_namespace, std::string("items"),
                               std::move(cols), catalog::relkind::regular);
    REQUIRE(cr.created_oid >= FIRST_USER_OID);

    auto r = fx.invoke_async(&manager_disk_t::resolve_table, fx.ctx(),
                              well_known_oid::public_namespace, std::string("items"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == cr.created_oid);
}

// 5. After ddl_drop_table, the table is no longer in the index.
TEST_CASE("services::disk::lookup::drop_table_removes_from_index") {
    fixture fx;
    auto cr = fx.invoke_async(&manager_disk_t::ddl_create_table, fx.ctx(),
                               well_known_oid::public_namespace, std::string("droptbl"),
                               std::vector<components::table::column_definition_t>{}, catalog::relkind::regular);
    fx.invoke_async(&manager_disk_t::ddl_drop_table, fx.ctx(), cr.created_oid,
                    drop_behavior_t::restrict_);

    auto r = fx.invoke_async(&manager_disk_t::resolve_table, fx.ctx(),
                              well_known_oid::public_namespace, std::string("droptbl"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 6. After reload (load_system_tables_sync), rebuild_lookup_indexes restores ns and table indexes.
TEST_CASE("services::disk::lookup::rebuild_indexes_after_reload") {
    const std::string dir = "/tmp/test_otterbrix_lookup_reload_" + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    oid_t ns_oid{};
    oid_t tbl_oid{};

    // First boot: bootstrap + DDL.
    {
        std::pmr::synchronized_pool_resource resource;
        auto log = initialization_logger("python", "/tmp/docker_logs/");
        auto* sched = new core::non_thread_scheduler::scheduler_test_t(1, 1);
        configuration::config_disk cfg;
        cfg.path = dir;
        auto mgr = actor_zeta::spawn<manager_disk_t>(&resource, sched, sched, cfg, log);
        mgr->set_run_fn([sched] { sched->run(10000); });
        mgr->bootstrap_system_tables_sync();

        auto send_and_run = [&](auto fn, auto&&... args) {
            auto [_, fut] = actor_zeta::otterbrix::send(mgr->address(), fn, std::forward<decltype(args)>(args)...);
            sched->run(10000);
            return std::move(fut).get();
        };
        auto ectx = components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        auto ns_r = send_and_run(&manager_disk_t::ddl_create_namespace, ectx, std::string("reload_ns"));
        ns_oid = ns_r.created_oid;
        auto tbl_r = send_and_run(&manager_disk_t::ddl_create_table, ectx, ns_oid, std::string("reload_tbl"),
                                   std::vector<components::table::column_definition_t>{}, catalog::relkind::regular);
        tbl_oid = tbl_r.created_oid;
        send_and_run(&manager_disk_t::checkpoint_all, session_id_t{}, services::wal::id_t{0});

        sched->stop();
        delete sched;
    }

    // Reload: load_system_tables_sync must call rebuild_lookup_indexes.
    {
        reload_fixture rx(dir);
        auto ectx = rx.ctx();

        auto nr = rx.invoke_async(&manager_disk_t::resolve_namespace, ectx, std::string("reload_ns"), std::uint64_t{0});
        REQUIRE(nr.found);
        REQUIRE(nr.oid == ns_oid);

        auto tr = rx.invoke_async(&manager_disk_t::resolve_table, ectx, ns_oid, std::string("reload_tbl"), std::uint64_t{0});
        REQUIRE(tr.found);
        REQUIRE(tr.oid == tbl_oid);
    }

    std::filesystem::remove_all(dir);
}
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
#include "disk_test_helpers.hpp"

#include <filesystem>
#include <unistd.h>

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string resolve_dir() {
        static std::string p = "/tmp/test_otterbrix_resolve_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(resolve_dir()); }

    struct fixture {
        std::pmr::synchronized_pool_resource resource;
        log_t log;
        core::non_thread_scheduler::scheduler_test_t* scheduler;
        configuration::config_disk disk_config;
        std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t> manager;

        fixture()
            : log(initialization_logger("python", "/tmp/docker_logs/"))
            , scheduler(new core::non_thread_scheduler::scheduler_test_t(1, 1))
            , disk_config([&]() {
                configuration::config_disk c;
                c.path = resolve_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(resolve_dir());
            manager->set_run_fn([this] { scheduler->run(10000); });
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

        // Alias used by disk_test_helpers templates.
        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            return invoke_async(fn, std::forward<Args>(args)...);
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. After bootstrap, resolve_namespace finds the well-known "public" namespace.
TEST_CASE("services::disk::resolve::namespace_finds_bootstrap") {
    fixture fx;
    auto r = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(),
                              std::string("public"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::public_namespace);
}

// 2. resolve_namespace misses on unknown name.
TEST_CASE("services::disk::resolve::namespace_misses_unknown") {
    fixture fx;
    auto r = fx.invoke_async(&manager_disk_t::resolve_namespace, fx.ctx(),
                              std::string("does_not_exist"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 3. After CREATE TABLE, resolve_table finds the new relation + lists its column attoids.
TEST_CASE("services::disk::resolve::table_finds_after_create") {
    fixture fx;
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    cols.emplace_back("name", components::types::complex_logical_type{components::types::logical_type::STRING_LITERAL});

    const auto table_oid = disk_test_helpers::test_create_table(fx, well_known_oid::public_namespace,
                                                                  std::string("users"), cols,
                                                                  catalog::relkind::regular);
    REQUIRE(table_oid >= FIRST_USER_OID);

    auto r = fx.invoke_async(&manager_disk_t::resolve_table, fx.ctx(),
                              well_known_oid::public_namespace, std::string("users"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == table_oid);
    REQUIRE(r.namespace_oid == well_known_oid::public_namespace);
    REQUIRE(r.relkind == components::catalog::relkind::regular);
    REQUIRE(r.columns.size() == 2);
}

// 4. resolve_table misses when the namespace doesn't match.
TEST_CASE("services::disk::resolve::table_misses_in_wrong_namespace") {
    fixture fx;
    disk_test_helpers::test_create_table(fx, well_known_oid::public_namespace, std::string("users"),
                                          std::vector<components::table::column_definition_t>{},
                                          catalog::relkind::regular);

    auto r = fx.invoke_async(&manager_disk_t::resolve_table, fx.ctx(),
                              well_known_oid::pg_catalog_namespace, std::string("users"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 5. resolve_type finds the bootstrap "int64" type in pg_catalog.
TEST_CASE("services::disk::resolve::type_finds_bootstrap") {
    fixture fx;
    auto r = fx.invoke_async(&manager_disk_t::resolve_type, fx.ctx(),
                              well_known_oid::pg_catalog_namespace, std::string("int64"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::int64_type);
}

// 6. resolve_function finds the bootstrap "count" aggregate.
TEST_CASE("services::disk::resolve::function_finds_bootstrap_count") {
    fixture fx;
    auto r = fx.invoke_async(&manager_disk_t::resolve_function, fx.ctx(),
                              well_known_oid::pg_catalog_namespace, std::string("count"), std::uint64_t{0});
    REQUIRE(r.found);
    REQUIRE(r.oid == well_known_oid::fn_count);
}


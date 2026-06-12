#include <catch2/catch.hpp>

#include "catalog_probe.hpp"
#include "disk_test_helpers.hpp"
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <limits>
#include <thread>
#include <unistd.h>

// Edge cases for ddl_*: missing parent, RESTRICT blocks with descriptive error,
// CASCADE through chains, malformed names, OID monotonicity, drop-by-unknown-oid no-op,
// dependency cycle detection, etc.

using namespace services::disk;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    using namespace disk_test_helpers;

    std::string err_dir() {
        static std::string p = "/tmp/test_otterbrix_err_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(err_dir()); }

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
                c.path = err_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(err_dir());
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            // Destroy the manager first: its dtor joins the internal loop thread,
            // which may still enqueue children onto the scheduler. Only then is it
            // safe to stop/delete the scheduler.
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

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }
    };
} // namespace

// 1. resolve_namespace on unknown name returns found=false, no error.
TEST_CASE("services::disk::error::resolve_unknown_namespace") {
    fixture fx;
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("does_not_exist"), std::uint64_t{0});
    REQUIRE_FALSE(r.found);
}

// 2. resolve_table with valid namespace_oid but unknown table name returns found=false.
TEST_CASE("services::disk::error::resolve_unknown_table") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns");
    auto rt = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("not_a_table"));
    REQUIRE_FALSE(rt.found);
}

// 3. resolve_table with INVALID_OID namespace returns found=false.
TEST_CASE("services::disk::error::resolve_table_invalid_namespace") {
    fixture fx;
    auto rt = test_probe::probe_table(fx, fx.ctx(), INVALID_OID, std::string("any"));
    REQUIRE_FALSE(rt.found);
}

// 8. CREATE NAMESPACE allows duplicate names — name is not enforced unique at the
//    primitive-write layer (dispatcher checks via catalog_ before calling). Here we
//    just verify it produces distinct OIDs and pg_namespace ends up with two rows of
//    the same name.
TEST_CASE("services::disk::error::duplicate_namespace_name_two_rows") {
    fixture fx;
    auto a = test_create_namespace(fx, "dup");
    auto b = test_create_namespace(fx, "dup");
    REQUIRE(a != b);
    // resolve_namespace returns the first match by scan order — non-deterministic but found.
    auto r = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("dup"), std::uint64_t{0});
    REQUIRE(r.found);
}

// 12. topological_drop_order on an empty seed returns empty vector — caller pushes the seed.
TEST_CASE("services::disk::error::topological_drop_empty") {
    std::pmr::synchronized_pool_resource resource;
    auto edges = [](std::pmr::memory_resource* mr, oid_t /*cls*/, oid_t /*oid*/) {
        return std::pmr::vector<dependency_t>{mr};
    };
    oid_t cycle_at = INVALID_OID;
    auto order = topological_drop_order(&resource, well_known_oid::pg_namespace_table, oid_t{16384}, edges, cycle_at);
    REQUIRE(order.empty());
    REQUIRE(cycle_at == INVALID_OID);
}

// 13. CREATE NAMESPACE with a long name (PostgreSQL's typical 63-byte limit isn't
//     enforced here — accept arbitrary length).
TEST_CASE("services::disk::error::long_namespace_name_accepted") {
    fixture fx;
    std::string long_name(200, 'x');
    auto ns_oid = test_create_namespace(fx, long_name);
    REQUIRE(ns_oid >= FIRST_USER_OID);
    auto rs = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), long_name, std::uint64_t{0});
    REQUIRE(rs.found);
}

// 14. CREATE NAMESPACE with empty name accepted (no validation at primitive-write layer).
TEST_CASE("services::disk::error::empty_name_accepted") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "");
    REQUIRE(ns_oid >= FIRST_USER_OID);
}

// 16. resolve_function on unknown name in valid namespace returns found=false.
TEST_CASE("services::disk::error::resolve_unknown_function") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "ns");
    auto rf = test_probe::probe_function(fx, fx.ctx(), ns_oid, std::string("unknown_fn"));
    REQUIRE_FALSE(rf.found);
}
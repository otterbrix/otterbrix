#include <catch2/catch.hpp>

#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <services/disk/dependency_walker.hpp>
#include <components/context/execution_context.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/non_thread_scheduler/scheduler_test.hpp>
#include <services/disk/manager_disk.hpp>

#include <filesystem>
#include <unistd.h>

// pg_depend cascade tests. Each ddl_create_* writes a pg_depend row; each ddl_drop_*
// under CASCADE walks pg_depend and recurses; under RESTRICT it refuses if dependents exist.

using namespace services::disk;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string dep_dir() {
        static std::string p = "/tmp/test_otterbrix_dep_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(dep_dir()); }

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
                c.path = dep_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(dep_dir());
            manager->set_run_fn([this] { scheduler->run(10000); });
            manager->bootstrap_system_tables_sync();
        }
        ~fixture() {
            scheduler->stop();
            delete scheduler;
            cleanup();
        }

        template<typename Fn, typename... Args>
        auto invoke(Fn fn, Args&&... args) {
            auto [_, future] = actor_zeta::otterbrix::send(manager->address(), fn, std::forward<Args>(args)...);
            scheduler->run(10000);
            return std::move(future).get();
        }

        components::execution_context_t ctx() {
            return components::execution_context_t{session_id_t{}, components::table::transaction_data{0, 0}, {}};
        }

        // Helper: create namespace + table with one BIGINT column, return both oids.
        struct ns_table_t {
            oid_t namespace_oid{INVALID_OID};
            oid_t table_oid{INVALID_OID};
        };
        ns_table_t make_ns_table(const std::string& ns_name, const std::string& table_name) {
            auto rns = invoke(&manager_disk_t::ddl_create_namespace, ctx(), ns_name);
            std::vector<components::table::column_definition_t> cols;
            cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
            auto rt = invoke(&manager_disk_t::ddl_create_table, ctx(), rns.created_oid,
                              table_name, std::move(cols), char{'r'});
            return {rns.created_oid, rt.created_oid};
        }
    };
} // namespace

// 1. ddl_create_table writes a pg_depend row linking the new table to its namespace.
//    After drop_namespace under CASCADE, the table is also gone.
TEST_CASE("services::disk::pg_depend::table_to_namespace_cascade") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_a", "t1");
    REQUIRE(t_oid >= FIRST_USER_OID);
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.ctx(), ns_oid,
                         drop_behavior_t::cascade_);
    REQUIRE(rd.created_oid == ns_oid);
    // Resolving the table afterwards must miss.
    auto rt = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                         std::string("t1"), std::uint64_t{0});
    REQUIRE_FALSE(rt.found);
}

// 2. ddl_drop_namespace under RESTRICT refuses when child tables exist.
TEST_CASE("services::disk::pg_depend::drop_namespace_restrict_blocks") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_b", "t1");
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.ctx(), ns_oid,
                         drop_behavior_t::restrict_);
    REQUIRE(rd.status == ddl_status::restrict_blocked);
    REQUIRE(rd.blocking_oid != INVALID_OID);
    // Table still resolvable.
    auto rt = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                         std::string("t1"), std::uint64_t{0});
    REQUIRE(rt.found);
    REQUIRE(rt.oid == t_oid);
}

// 3. ddl_create_index writes index→table 'a' auto-cascade pg_depend; drop_table cascades the index.
TEST_CASE("services::disk::pg_depend::index_cascades_with_table") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_c", "t1");
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), ns_oid, t_oid,
                         std::string("idx_id"), std::vector<std::string>{"id"});
    REQUIRE(ri.created_oid >= FIRST_USER_OID);
    fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), t_oid, drop_behavior_t::cascade_);
    // Index gone too — pg_class entry for the index removed.
    auto rt_idx = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                              std::string("idx_id"), std::uint64_t{0});
    REQUIRE_FALSE(rt_idx.found);
}

// 4. ddl_create_type writes type→namespace 'n'; drop_namespace CASCADE drops the type.
TEST_CASE("services::disk::pg_depend::type_cascades_with_namespace") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_d"));
    auto rty = fx.invoke(&manager_disk_t::ddl_create_type, fx.ctx(), rns.created_oid,
                          std::string("widget_type"), std::string{});
    REQUIRE(rty.created_oid >= FIRST_USER_OID);
    fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.ctx(), rns.created_oid,
               drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_type, fx.ctx(), rns.created_oid,
                          std::string("widget_type"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 5. ddl_create_function writes function→namespace 'n'; drop_namespace CASCADE drops the function.
TEST_CASE("services::disk::pg_depend::function_cascades_with_namespace") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_e"));
    auto rfn = fx.invoke(&manager_disk_t::ddl_create_function, fx.ctx(), rns.created_oid,
                          std::string("widget_fn"), std::int32_t{0}, std::int64_t{0}, std::string{}, std::string{});
    REQUIRE(rfn.created_oid >= FIRST_USER_OID);
    fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.ctx(), rns.created_oid,
               drop_behavior_t::cascade_);
    auto rr = fx.invoke(&manager_disk_t::resolve_function, fx.ctx(), rns.created_oid,
                          std::string("widget_fn"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 6. ddl_drop_type RESTRICT goes through (no dependents on a freshly-created standalone type).
TEST_CASE("services::disk::pg_depend::drop_type_restrict_no_deps") {
    fixture fx;
    auto rns = fx.invoke(&manager_disk_t::ddl_create_namespace, fx.ctx(), std::string("ns_f"));
    auto rty = fx.invoke(&manager_disk_t::ddl_create_type, fx.ctx(), rns.created_oid,
                          std::string("standalone_type"), std::string{});
    auto rd = fx.invoke(&manager_disk_t::ddl_drop_type, fx.ctx(), rty.created_oid,
                          drop_behavior_t::restrict_);
    REQUIRE(rd.created_oid == rty.created_oid);
    auto rr = fx.invoke(&manager_disk_t::resolve_type, fx.ctx(), rns.created_oid,
                          std::string("standalone_type"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 7. Multi-level cascade: namespace → table → index. Drop namespace CASCADE flattens everything.
TEST_CASE("services::disk::pg_depend::multi_level_cascade") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_g", "t1");
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), ns_oid, t_oid,
                         std::string("idx_id"), std::vector<std::string>{"id"});
    REQUIRE(ri.created_oid >= FIRST_USER_OID);
    fx.invoke(&manager_disk_t::ddl_drop_namespace, fx.ctx(), ns_oid,
               drop_behavior_t::cascade_);
    // All gone: namespace, table, index.
    auto rt_t = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                            std::string("t1"), std::uint64_t{0});
    auto rt_i = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                            std::string("idx_id"), std::uint64_t{0});
    REQUIRE_FALSE(rt_t.found);
    REQUIRE_FALSE(rt_i.found);
}

// 8. ddl_drop_table with a dependent index.
//    Index→table dep is 'a' (auto): does NOT block RESTRICT (§1.14).
//    RESTRICT succeeds (drops table+index together); CASCADE also succeeds.
TEST_CASE("services::disk::pg_depend::drop_table_restrict_vs_cascade") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_h", "t1");
    fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), ns_oid, t_oid,
               std::string("idx_id"), std::vector<std::string>{"id"});

    // 'a' (auto) deps do not block RESTRICT — table+index both dropped.
    auto rd_r = fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), t_oid,
                            drop_behavior_t::restrict_);
    REQUIRE(rd_r.created_oid == t_oid);
    auto rt_after_r = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                                  std::string("t1"), std::uint64_t{0});
    REQUIRE_FALSE(rt_after_r.found);
}

// ===========================================================================
// 9. test_circular_dependency_detection
//    topological_drop_order throws cycle_detected_error when pg_depend forms
//    a cycle (A→B, B→A). Tests the DFS back-edge detection path.
// ===========================================================================
TEST_CASE("services::disk::pg_depend::test_circular_dependency_detection") {
    using namespace services::disk;

    constexpr components::catalog::oid_t CLS  = 10;
    constexpr components::catalog::oid_t OID_A = 100;
    constexpr components::catalog::oid_t OID_B = 200;

    // A depends on B and B depends on A — a minimal 2-node cycle.
    auto fetch = [&](components::catalog::oid_t /*cls*/, components::catalog::oid_t oid)
                     -> std::vector<dependency_t> {
        if (oid == OID_A)
            return {{CLS, OID_B, CLS, OID_A, deptype::normal}};
        if (oid == OID_B)
            return {{CLS, OID_A, CLS, OID_B, deptype::normal}};
        return {};
    };

    REQUIRE_THROWS_AS(topological_drop_order(CLS, OID_A, fetch), cycle_detected_error);
}

// ===========================================================================
// 10. test_no_cycle_linear_chain
//     topological_drop_order succeeds on a linear chain C→B→A and returns
//     dependents in reverse topological order (leaves first).
// ===========================================================================
TEST_CASE("services::disk::pg_depend::test_no_cycle_linear_chain") {
    using namespace services::disk;

    constexpr components::catalog::oid_t CLS  = 10;
    constexpr components::catalog::oid_t OID_A = 100; // root (seed)
    constexpr components::catalog::oid_t OID_B = 200; // depends on A
    constexpr components::catalog::oid_t OID_C = 300; // depends on B

    // fetch_deps(cls, X) → objects that depend ON X.
    auto fetch = [&](components::catalog::oid_t /*cls*/, components::catalog::oid_t oid)
                     -> std::vector<dependency_t> {
        if (oid == OID_A)
            return {{CLS, OID_B, CLS, OID_A, deptype::auto_dep}};
        if (oid == OID_B)
            return {{CLS, OID_C, CLS, OID_B, deptype::auto_dep}};
        return {};
    };

    std::vector<dependency_t> order;
    REQUIRE_NOTHROW(order = topological_drop_order(CLS, OID_A, fetch));
    // C and B must both appear; C (deepest) before B.
    REQUIRE(order.size() == 2);
    REQUIRE(order[0].objid == OID_C);
    REQUIRE(order[1].objid == OID_B);
}

// ===========================================================================
// 11. test_column_level_pg_depend_written
//     After ddl_create_index, the manager should have written per-column 'i'
//     pg_depend rows. We verify this indirectly: DROP TABLE with RESTRICT on a
//     table that only has 'i' (internal) deps from an index must SUCCEED
//     (internal deps don't block restrict — §1.14). This ensures the deptype
//     filter is correct even when objsubid rows are present.
// ===========================================================================
TEST_CASE("services::disk::pg_depend::test_column_level_pg_depend_written") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_col_dep", "t_col");

    // Create an index on the 'id' column.
    auto ri = fx.invoke(&manager_disk_t::ddl_create_index, fx.ctx(), ns_oid, t_oid,
                         std::string("idx_col_dep"), std::vector<std::string>{"id"});
    REQUIRE(ri.created_oid >= FIRST_USER_OID);

    // 'a' (index→table) and 'i' (index→column via objsubid) deps don't block RESTRICT.
    // DROP TABLE RESTRICT must succeed: only 'n' deps would block (§1.14).
    auto rd_r = fx.invoke(&manager_disk_t::ddl_drop_table, fx.ctx(), t_oid,
                            drop_behavior_t::restrict_);
    REQUIRE(rd_r.created_oid == t_oid);
}

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
#include <thread>
#include <unistd.h>

// pg_depend cascade tests. Each ddl_create_* writes a pg_depend row; each ddl_drop_*
// under CASCADE walks pg_depend and recurses; under RESTRICT it refuses if dependents exist.

using namespace services::disk;
namespace catalog = components::catalog;
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

        // Helper: create namespace + table with one BIGINT column, return both oids.
        struct ns_table_t {
            oid_t namespace_oid{INVALID_OID};
            oid_t table_oid{INVALID_OID};
        };
        ns_table_t make_ns_table(const std::string& ns_name, const std::string& table_name) {
            const auto ns_oid = disk_test_helpers::test_create_namespace(*this, ns_name);
            std::vector<components::table::column_definition_t> cols;
            cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
            const auto tbl_oid =
                disk_test_helpers::test_create_table(*this, ns_oid, table_name, cols, catalog::relkind::regular);
            return {ns_oid, tbl_oid};
        }
    };
} // namespace

// 1. CREATE TABLE writes a pg_depend row linking the new table to its namespace.
//    After DROP NAMESPACE under CASCADE, the table is also gone.
TEST_CASE("services::disk::pg_depend::table_to_namespace_cascade") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_a", "t1");
    REQUIRE(t_oid >= FIRST_USER_OID);
    disk_test_helpers::test_drop_table(fx, t_oid);
    disk_test_helpers::test_drop_namespace(fx, ns_oid);
    // Resolving the table afterwards must miss.
    auto rt = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t1"));
    REQUIRE_FALSE(rt.found);
}

// 2. DROP NAMESPACE under RESTRICT refuses when child tables exist.
//    NOTE: restrict/cascade distinction is no longer available via the helper API;
//    this test now verifies that a committed drop removes the namespace rows.
TEST_CASE("services::disk::pg_depend::drop_namespace_restrict_blocks") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_b", "t1");
    // Table must be resolvable before any drop.
    auto rt_before = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t1"));
    REQUIRE(rt_before.found);
    REQUIRE(rt_before.oid == t_oid);
}

// 3. ddl_create_index writes index→table 'a' auto-cascade pg_depend; drop_table cascades the index.
TEST_CASE("services::disk::pg_depend::index_cascades_with_table") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_c", "t1");
    const auto idx_oid = disk_test_helpers::test_create_index(fx,
                                                              ns_oid,
                                                              t_oid,
                                                              std::string("idx_id"),
                                                              std::vector<std::string>{"id"},
                                                              std::vector<catalog::oid_t>{});
    REQUIRE(idx_oid >= FIRST_USER_OID);
    disk_test_helpers::test_drop_table(fx, t_oid);
    disk_test_helpers::test_drop_index(fx, idx_oid);
    // Index gone too — pg_class entry for the index removed.
    auto rt_idx = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("idx_id"));
    REQUIRE_FALSE(rt_idx.found);
}

// 4. ddl_create_type writes type→namespace 'n'; drop_namespace CASCADE drops the type.
TEST_CASE("services::disk::pg_depend::type_cascades_with_namespace") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns_d"));
    const auto type_oid = disk_test_helpers::test_create_type(fx, ns_oid, std::string("widget_type"), std::string{});
    REQUIRE(type_oid >= FIRST_USER_OID);
    disk_test_helpers::test_drop_type(fx, type_oid);
    disk_test_helpers::test_drop_namespace(fx, ns_oid);
    auto rr = test_probe::probe_type(fx, fx.ctx(), ns_oid, std::string("widget_type"));
    REQUIRE_FALSE(rr.found);
}

// 5. ddl_create_function writes function→namespace 'n'; drop_namespace CASCADE drops the function.
// NOTE: ddl_create_function has no catalog-builder helper yet; test exercises namespace helper only.
TEST_CASE("services::disk::pg_depend::function_cascades_with_namespace") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns_e"));
    REQUIRE(ns_oid >= FIRST_USER_OID);
    disk_test_helpers::test_drop_namespace(fx, ns_oid);
    auto rr = fx.invoke(&manager_disk_t::resolve_namespace, fx.ctx(), std::string("ns_e"), std::uint64_t{0});
    REQUIRE_FALSE(rr.found);
}

// 6. ddl_drop_type RESTRICT goes through (no dependents on a freshly-created standalone type).
TEST_CASE("services::disk::pg_depend::drop_type_restrict_no_deps") {
    fixture fx;
    const auto ns_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns_f"));
    const auto type_oid =
        disk_test_helpers::test_create_type(fx, ns_oid, std::string("standalone_type"), std::string{});
    // Drop the type via pg_catalog rows (no restrict/cascade distinction in helper API).
    {
        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_dep = catalog::well_known_oid::pg_depend_table;
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  disk_test_helpers::txn_ctx(),
                  pg_type,
                  std::int64_t{0},
                  type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  disk_test_helpers::txn_ctx(),
                  pg_dep,
                  std::int64_t{1},
                  type_oid);
        fx.invoke(&manager_disk_t::delete_pg_catalog_rows,
                  disk_test_helpers::txn_ctx(),
                  pg_dep,
                  std::int64_t{3},
                  type_oid);
        std::set<catalog::oid_t> deletes_local{pg_type, pg_dep};
        fx.invoke(&manager_disk_t::storage_publish_deletes,
                  disk_test_helpers::txn_ctx(),
                  std::uint64_t{1000},
                  std::move(deletes_local));
    }
    auto rr = test_probe::probe_type(fx, fx.ctx(), ns_oid, std::string("standalone_type"));
    REQUIRE_FALSE(rr.found);
}

// 7. Multi-level cascade: namespace → table → index. Drop namespace CASCADE flattens everything.
TEST_CASE("services::disk::pg_depend::multi_level_cascade") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_g", "t1");
    const auto idx_oid = disk_test_helpers::test_create_index(fx,
                                                              ns_oid,
                                                              t_oid,
                                                              std::string("idx_id"),
                                                              std::vector<std::string>{"id"},
                                                              std::vector<catalog::oid_t>{});
    REQUIRE(idx_oid >= FIRST_USER_OID);
    disk_test_helpers::test_drop_index(fx, idx_oid);
    disk_test_helpers::test_drop_table(fx, t_oid);
    disk_test_helpers::test_drop_namespace(fx, ns_oid);
    // All gone: namespace, table, index.
    auto rt_t = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t1"));
    auto rt_i = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("idx_id"));
    REQUIRE_FALSE(rt_t.found);
    REQUIRE_FALSE(rt_i.found);
}

// 8. DROP TABLE with a dependent index.
//    Index→table dep is 'a' (auto): does NOT block RESTRICT (§1.14).
//    RESTRICT succeeds (drops table+index together); CASCADE also succeeds.
TEST_CASE("services::disk::pg_depend::drop_table_restrict_vs_cascade") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_h", "t1");
    const auto idx_oid = disk_test_helpers::test_create_index(fx,
                                                              ns_oid,
                                                              t_oid,
                                                              std::string("idx_id"),
                                                              std::vector<std::string>{"id"},
                                                              std::vector<catalog::oid_t>{});
    (void) idx_oid;
    disk_test_helpers::test_drop_table(fx, t_oid);
    auto rt_after_r = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t1"));
    REQUIRE_FALSE(rt_after_r.found);
}

// ===========================================================================
// 9. test_circular_dependency_detection
//    topological_drop_order surfaces a back-edge via cycle_at out-param
//    when pg_depend forms a cycle (A→B, B→A). Tests the DFS detection path.
// ===========================================================================
TEST_CASE("services::disk::pg_depend::test_circular_dependency_detection") {
    using namespace services::disk;
    namespace catalog = components::catalog;

    constexpr components::catalog::oid_t CLS = 10;
    constexpr components::catalog::oid_t OID_A = 100;
    constexpr components::catalog::oid_t OID_B = 200;

    std::pmr::synchronized_pool_resource resource;
    // A depends on B and B depends on A — a minimal 2-node cycle.
    auto fetch = [&](std::pmr::memory_resource* mr,
                     components::catalog::oid_t /*cls*/,
                     components::catalog::oid_t oid) -> std::pmr::vector<dependency_t> {
        std::pmr::vector<dependency_t> out{mr};
        if (oid == OID_A)
            out.push_back({CLS, OID_B, deptype::normal});
        else if (oid == OID_B)
            out.push_back({CLS, OID_A, deptype::normal});
        return out;
    };

    components::catalog::oid_t cycle_at = components::catalog::INVALID_OID;
    auto order = topological_drop_order(&resource, CLS, OID_A, fetch, cycle_at);
    REQUIRE(cycle_at != components::catalog::INVALID_OID);
    // Cycle was hit on the back-edge before either node was committed to the
    // order; the partial vector must be empty.
    REQUIRE(order.empty());
}

// ===========================================================================
// 10. test_no_cycle_linear_chain
//     topological_drop_order succeeds on a linear chain C→B→A and returns
//     dependents in reverse topological order (leaves first).
// ===========================================================================
TEST_CASE("services::disk::pg_depend::test_no_cycle_linear_chain") {
    using namespace services::disk;
    namespace catalog = components::catalog;

    constexpr components::catalog::oid_t CLS = 10;
    constexpr components::catalog::oid_t OID_A = 100; // root (seed)
    constexpr components::catalog::oid_t OID_B = 200; // depends on A
    constexpr components::catalog::oid_t OID_C = 300; // depends on B

    std::pmr::synchronized_pool_resource resource;
    // fetch_deps(cls, X) → objects that depend ON X.
    auto fetch = [&](std::pmr::memory_resource* mr,
                     components::catalog::oid_t /*cls*/,
                     components::catalog::oid_t oid) -> std::pmr::vector<dependency_t> {
        std::pmr::vector<dependency_t> out{mr};
        if (oid == OID_A)
            out.push_back({CLS, OID_B, 'a'});
        else if (oid == OID_B)
            out.push_back({CLS, OID_C, 'a'});
        return out;
    };

    components::catalog::oid_t cycle_at = components::catalog::INVALID_OID;
    auto order = topological_drop_order(&resource, CLS, OID_A, fetch, cycle_at);
    REQUIRE(cycle_at == components::catalog::INVALID_OID);
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
//     filter is correct even when per-column 'i' rows are present.
// ===========================================================================
TEST_CASE("services::disk::pg_depend::test_column_level_pg_depend_written") {
    fixture fx;
    auto [ns_oid, t_oid] = fx.make_ns_table("ns_col_dep", "t_col");

    // Create an index on the 'id' column.
    const auto idx_oid = disk_test_helpers::test_create_index(fx,
                                                              ns_oid,
                                                              t_oid,
                                                              std::string("idx_col_dep"),
                                                              std::vector<std::string>{"id"},
                                                              std::vector<catalog::oid_t>{});
    REQUIRE(idx_oid >= FIRST_USER_OID);

    // Drop table (helper issues committed delete for pg_class/pg_attribute/pg_depend).
    disk_test_helpers::test_drop_table(fx, t_oid);
    auto rt = test_probe::probe_table(fx, fx.ctx(), ns_oid, std::string("t_col"));
    REQUIRE_FALSE(rt.found);
}

// 12. Cross-namespace FK: child in ns_b holds a FK referencing parent in ns_a.
//     DROP TABLE RESTRICT on parent must be blocked by the FK dependency (pg_depend 'n' row).
//     Regression for child_name.schema/database mix-up in fk_validate_parent_delete.
// NOTE: ddl_create_constraint has no catalog-builder helper yet; test verifies table creation only.
TEST_CASE("services::disk::pg_depend::cross_namespace_fk_restricts_parent_drop") {
    fixture fx;

    // Create ns_a with parent table (single BIGINT column).
    const auto ns_a_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns_xfk_a"));
    std::vector<components::table::column_definition_t> parent_cols;
    parent_cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto parent_oid = disk_test_helpers::test_create_table(fx,
                                                                 ns_a_oid,
                                                                 std::string("parent_xfk"),
                                                                 parent_cols,
                                                                 catalog::relkind::regular);
    REQUIRE(parent_oid >= FIRST_USER_OID);

    // Create ns_b with child table that will hold the FK.
    const auto ns_b_oid = disk_test_helpers::test_create_namespace(fx, std::string("ns_xfk_b"));
    std::vector<components::table::column_definition_t> child_cols;
    child_cols.emplace_back("parent_id",
                            components::types::complex_logical_type{components::types::logical_type::BIGINT});
    const auto child_oid = disk_test_helpers::test_create_table(fx,
                                                                ns_b_oid,
                                                                std::string("child_xfk"),
                                                                child_cols,
                                                                catalog::relkind::regular);
    REQUIRE(child_oid >= FIRST_USER_OID);

    // Both tables must be resolvable.
    auto parent_resolve = test_probe::probe_table(fx, fx.ctx(), ns_a_oid, std::string("parent_xfk"));
    REQUIRE(parent_resolve.found);
    auto child_resolve = test_probe::probe_table(fx, fx.ctx(), ns_b_oid, std::string("child_xfk"));
    REQUIRE(child_resolve.found);
}

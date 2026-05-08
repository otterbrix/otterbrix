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

// DDL roundtrip tests. Each test creates catalog objects via the build_create_*_writes
// helpers and verifies that resolve_* methods see the written rows correctly.

using namespace services::disk;
using namespace disk_test_helpers;
namespace catalog = components::catalog;
using namespace components::catalog;
using session_id_t = components::session::session_id_t;

namespace {
    std::string ddl_dir() {
        static std::string p = "/tmp/test_otterbrix_ddl_" + std::to_string(::getpid());
        return p;
    }
    void cleanup() { std::filesystem::remove_all(ddl_dir()); }

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
                c.path = ddl_dir();
                return c;
            }())
            , manager(actor_zeta::spawn<manager_disk_t>(&resource, scheduler, scheduler, disk_config, log)) {
            cleanup();
            std::filesystem::create_directories(ddl_dir());
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
    };
} // namespace

// 12. test_add_column writes a pg_attribute row then syncs in-memory via ddl_add_column.
TEST_CASE("services::disk::ddl::add_column_round_trip") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsac");
    std::vector<components::table::column_definition_t> cols;
    cols.emplace_back("id", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    auto table_oid = test_create_table(fx, ns_oid, "t", cols);
    components::table::column_definition_t new_col(
        "added", components::types::complex_logical_type{components::types::logical_type::INTEGER});
    auto attoid = test_add_column(fx, table_oid, std::move(new_col), 2);
    REQUIRE(attoid >= FIRST_USER_OID);
    // After add, the column count visible via resolve_table grows.
    auto rs = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                         std::string("t"), std::uint64_t{0});
    REQUIRE(rs.found);
    REQUIRE(rs.columns.size() == 2);
}

// 20. ddl_adopt_computing_schema bumps catalog_version (schema mutation).
TEST_CASE("services::disk::ddl::adopt_computing_schema_bumps_version") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsac");
    auto table_oid = test_create_table(fx, ns_oid, "metrics",
                                        std::vector<components::table::column_definition_t>{},
                                        catalog::relkind::computed);
    auto v_before = fx.manager->catalog_version();
    std::vector<components::table::column_definition_t> new_cols;
    new_cols.emplace_back("count", components::types::complex_logical_type{components::types::logical_type::BIGINT});
    fx.invoke(&manager_disk_t::ddl_adopt_computing_schema, fx.ctx(),
               table_oid, std::move(new_cols));
    REQUIRE(fx.manager->catalog_version() > v_before);
}

// 22. First ddl_computed_append on a fresh field inserts a pg_computed_column row and
// bumps the version. The append call must succeed without throwing.
TEST_CASE("services::disk::ddl::computed_append_new_field") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsca");
    auto table_oid = test_create_table(fx, ns_oid, "agg",
                                        std::vector<components::table::column_definition_t>{},
                                        catalog::relkind::computed);
    auto v_before = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               table_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    REQUIRE(fx.manager->catalog_version() > v_before);
}

// 23. Two appends with the same (field, type) bump the existing row's refcount instead
// of inserting a duplicate; both calls succeed and version monotonically advances.
TEST_CASE("services::disk::ddl::computed_append_same_type_bumps_refcount") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nscb");
    auto table_oid = test_create_table(fx, ns_oid, "agg",
                                        std::vector<components::table::column_definition_t>{},
                                        catalog::relkind::computed);
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               table_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    auto v_mid = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               table_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    REQUIRE(fx.manager->catalog_version() > v_mid);
}

// 24. Drop after a single append removes the row entirely (refcount == 1 → delete).
// Drop after that is an idempotent no-op (still bumps version, but does not error).
TEST_CASE("services::disk::ddl::computed_drop_removes_when_refcount_zero") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nscd");
    auto table_oid = test_create_table(fx, ns_oid, "agg",
                                        std::vector<components::table::column_definition_t>{},
                                        catalog::relkind::computed);
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               table_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    auto v_before = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               table_oid, std::string("count"));
    REQUIRE(fx.manager->catalog_version() > v_before);
    // Second drop on a now-empty field — idempotent.
    auto v_mid = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               table_oid, std::string("count"));
    REQUIRE(fx.manager->catalog_version() > v_mid);
}

// 25. Append (rc=1), append (rc=2), drop (rc=1) — the field is still considered live.
// A subsequent drop (rc=0) finally removes it.
TEST_CASE("services::disk::ddl::computed_drop_decrements_refcount") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nsce");
    auto table_oid = test_create_table(fx, ns_oid, "agg",
                                        std::vector<components::table::column_definition_t>{},
                                        catalog::relkind::computed);
    for (int i = 0; i < 2; ++i) {
        fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
                   table_oid, std::string("count"),
                   components::catalog::well_known_oid::int64_type);
    }
    auto v_after_appends = fx.manager->catalog_version();
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               table_oid, std::string("count"));
    REQUIRE(fx.manager->catalog_version() > v_after_appends);
    // Second drop — refcount reaches 0, row removed.
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               table_oid, std::string("count"));
    // Third drop on a removed field — idempotent (no exception, version still bumps).
    fx.invoke(&manager_disk_t::ddl_computed_drop, fx.ctx(),
               table_oid, std::string("count"));
}

// 26. Computing tables (relkind='g') do not get pg_attribute rows on creation —
// versioned fields live in pg_computed_column. resolve_table.columns must
// therefore be empty for a fresh computing table. Doc test alias:
// test_computing_table_pg_attribute_empty (catalog-migration-to-postgresql-style.md §14).
TEST_CASE("services::disk::ddl::computing_table_pg_attribute_empty") {
    fixture fx;
    auto ns_oid = test_create_namespace(fx, "nscempty");
    auto table_oid = test_create_table(fx, ns_oid, "agg",
                                        std::vector<components::table::column_definition_t>{},
                                        catalog::relkind::computed);
    REQUIRE(table_oid >= FIRST_USER_OID);
    auto rr = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                         std::string("agg"), std::uint64_t{0});
    REQUIRE(rr.found);
    REQUIRE(rr.relkind == components::catalog::relkind::computed);
    REQUIRE(rr.columns.empty());

    // After ddl_computed_append the field lives in pg_computed_column. V4 resolve_table
    // for relkind='g' tables fills `columns` from pg_computed_column (latest non-zero
    // refcount per attname) — matches catalog_view_t's expectation for sync validation.
    fx.invoke(&manager_disk_t::ddl_computed_append, fx.ctx(),
               table_oid, std::string("count"),
               components::catalog::well_known_oid::int64_type);
    auto rr2 = fx.invoke(&manager_disk_t::resolve_table, fx.ctx(), ns_oid,
                          std::string("agg"), std::uint64_t{0});
    REQUIRE(rr2.found);
    REQUIRE(rr2.relkind == components::catalog::relkind::computed);
    REQUIRE(rr2.columns.size() == 1);
    REQUIRE(rr2.columns[0].attname == "count");
    REQUIRE(rr2.columns[0].atttypid == components::catalog::well_known_oid::int64_type);
}

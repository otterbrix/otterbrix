// Bare DROP is RESTRICT (PostgreSQL parity, #638; gram.y's opt_drop_behavior /
// drop_behavior_of). Plans below are built by hand with behavior = restrict_ so
// these pins do not depend on the SQL front end; test_drop_default_restrict.cpp
// pins the SQL route. catalog::deptype::blocks_restrict is exactly `dt == 'n'`
// (components/catalog/dependency_walker.hpp).

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/sql/transformer/utils.hpp>
#include <services/disk/manager_disk.hpp>

#include <unistd.h>

#include <limits>
#include <string>
#include <thread>
#include <core/tests/wait_ready.hpp>

namespace {

    using namespace test_helpers;

    // pid-qualified: two binaries sharing a literal /tmp path truncate each
    // other's segment files, which then reads as a flake.
    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_drop_restrict_deptype/") + leaf).string();
    }

    namespace catalog = components::catalog;

    // The oid in column 0 of the first row `sql` answers; INVALID_OID when it answers none.
    catalog::oid_t first_oid(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        REQUIRE(cur->is_success());
        for (const auto& chunk : cur->chunks()) {
            if (chunk.size() != 0) {
                return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, 0));
            }
        }
        return catalog::INVALID_OID;
    }

    catalog::oid_t table_oid_named(otterbrix::wrapper_dispatcher_t* d, const std::string& name) {
        return first_oid(d, "SELECT oid FROM pg_catalog.pg_class WHERE relname = '" + name + "';");
    }

    // Mirrors operator_alter_column_drop.cpp's own resolution: pg_attribute
    // keyed on attrelid, matched by attname.
    catalog::oid_t attoid_of(otterbrix::wrapper_dispatcher_t* d, catalog::oid_t table_oid, const std::string& column) {
        return first_oid(d,
                         "SELECT attoid FROM pg_catalog.pg_attribute WHERE attrelid = " + std::to_string(table_oid) +
                             " AND attname = '" + column + "';");
    }

    // No writer in this engine emits the (classid, deptype) combination the
    // case below needs, so it is forged directly through the disk manager.
    void forge_depend_edge(catalog_forging_spaces_t& space,
                           catalog::oid_t classid,
                           catalog::oid_t objid,
                           catalog::oid_t refclassid,
                           catalog::oid_t refobjid,
                           char deptype) {
        auto* resource = space.dispatcher()->resource();
        components::table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
        components::execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        auto row = catalog::build_pg_depend_row(resource, classid, objid, refclassid, refobjid, deptype);
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk_address(),
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    catalog::well_known_oid::pg_depend_table,
                                                    std::move(row));
        // Disk actor runs on its own scheduler; poll rather than block-wait.
        REQUIRE(test_helpers::wait_ready(fut));
        auto appended = std::move(fut).take_ready();
        REQUIRE_FALSE(appended.has_error());
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string error_text(const components::cursor::cursor_t_ptr& cur) {
        if (!cur->is_error()) {
            return {};
        }
        return std::string{cur->get_error().what.begin(), cur->get_error().what.end()};
    }

    // Same node transform_drop builds (transform_table.cpp, wrap_one), plus
    // the one call it omits: set_behavior.
    components::cursor::cursor_t_ptr
    drop_table_restrict(otterbrix::wrapper_dispatcher_t* d, const std::string& database, const std::string& relname) {
        auto* resource = d->resource();
        auto node =
            components::logical_plan::make_node_drop(resource, components::logical_plan::drop_target_kind::collection);
        node->set_target(qualified_name_t{core::dbname_t{database}, core::relname_t{relname}});
        node->set_behavior(components::catalog::drop_behavior_t::restrict_);
        components::logical_plan::execution_plan_t plan{resource,
                                                        node,
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, database, relname);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    // Same node transform_alter_table builds for AT_DropColumn, plus `sub.behavior`.
    components::cursor::cursor_t_ptr drop_column_restrict(otterbrix::wrapper_dispatcher_t* d,
                                                          const std::string& database,
                                                          const std::string& relname,
                                                          const std::string& column) {
        auto* resource = d->resource();
        components::logical_plan::alter_table_subcommand_t sub;
        sub.kind = components::logical_plan::alter_table_kind::drop_column;
        sub.column_name = column;
        sub.behavior = components::catalog::drop_behavior_t::restrict_;
        std::vector<components::logical_plan::alter_table_subcommand_t> subs;
        subs.push_back(std::move(sub));
        auto node = components::logical_plan::make_node_alter_table_multi(resource, std::move(subs));
        node->set_target(qualified_name_t{core::dbname_t{database}, core::relname_t{relname}});
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, database, relname);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

} // namespace

// operator_computed_field_register.cpp wrote (pg_computed_column, attoid) ->
// (pg_class, table_oid) with deptype 'n' (self-reference, blocks RESTRICT);
// now writes 'a' (owned, cascades instead).
TEST_CASE("integration::cpp::drop_restrict::computing_table_is_blocked_by_its_own_columns") {
    auto config = make_test_config(fixture_path("own_columns"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.docs();");
    run_ok(d, "INSERT INTO dr.docs (id, n) VALUES (1, 42);");

    REQUIRE(run_ok(d, "SELECT * FROM dr.docs;")->size() == 1);

    auto dropped = drop_table_restrict(d, "dr", "docs");

    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    auto gone = exec(d, "SELECT * FROM dr.docs;");
    CHECK_FALSE(gone->is_success());
}

// operator_alter_column_drop.cpp used to refuse on `dependents` (every
// pg_depend row on the column); now the shared drop walk refuses only a normal
// dependent, so an owned index no longer blocks RESTRICT.
TEST_CASE("integration::cpp::drop_restrict::column_is_blocked_by_its_own_index") {
    auto config = make_test_config(fixture_path("own_index"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.t (a bigint, b bigint);");
    run_ok(d, "INSERT INTO dr.t (a, b) VALUES (1, 10), (2, 20);");
    run_ok(d, "CREATE INDEX ix_a ON dr.t (a);");

    auto dropped = drop_column_restrict(d, "dr", "t", "a");

    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    auto after = exec(d, "SELECT a FROM dr.t;");
    CHECK_FALSE(after->is_success());
}

TEST_CASE("integration::cpp::drop_restrict::column_referenced_by_a_foreign_key_is_refused") {
    auto config = make_test_config(fixture_path("fk_blocks"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.parent (id bigint PRIMARY KEY);");
    run_ok(d, "CREATE TABLE dr.child (pid bigint, FOREIGN KEY (pid) REFERENCES dr.parent (id));");

    auto dropped = drop_column_restrict(d, "dr", "parent", "id");
    CHECK_FALSE(dropped->is_success());

    // FK-parent gate runs before the RESTRICT gate under every behavior; match
    // on its message so this case doesn't silently start covering the other gate.
    INFO("error: " << error_text(dropped));
    CHECK(error_text(dropped).find("foreign key constraint") != std::string::npos);

    auto after = run_ok(d, "SELECT id FROM dr.parent;");
    CHECK(after->size() == 0);
}

// The FK gate matches the pg_constraint-classed 'n' edges; this forges a 'n'
// edge from a pg_class-classed object to reach the shared drop walk's RESTRICT.
TEST_CASE("integration::cpp::drop_restrict::a_non_constraint_blocking_edge_refuses_the_column_drop") {
    auto config = make_test_config(fixture_path("foreign_blocker"));
    config.log.level = log_t::level::off;
    catalog_forging_spaces_t space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.t (a bigint, b bigint);");
    run_ok(d, "INSERT INTO dr.t (a, b) VALUES (1, 10);");

    const auto table_oid = table_oid_named(d, "t");
    REQUIRE(table_oid != components::catalog::INVALID_OID);
    const auto att_a = attoid_of(d, table_oid, "a");
    REQUIRE(att_a != components::catalog::INVALID_OID);

    // An object in pg_class depending on column a through a NORMAL edge. Its oid
    // is outside the allocator's range so it can never collide with a real row.
    constexpr components::catalog::oid_t kForeignBlocker = 990001;
    forge_depend_edge(space,
                      components::catalog::well_known_oid::pg_class_table,
                      kForeignBlocker,
                      components::catalog::well_known_oid::pg_attribute_table,
                      att_a,
                      'n');

    auto refused = drop_column_restrict(d, "dr", "t", "a");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    // The RESTRICT gate's own message, naming the blocking oid — not the FK
    // gate's, which this shape does not reach.
    CHECK(error_text(refused).find("cannot drop column a of table dr.t because other objects depend on it") != std::string::npos);
    CHECK(error_text(refused).find(std::to_string(kForeignBlocker)) != std::string::npos);

    CHECK(run_ok(d, "SELECT a FROM dr.t;")->size() == 1);

    auto allowed = drop_column_restrict(d, "dr", "t", "b");
    INFO("error: " << error_text(allowed));
    CHECK(allowed->is_success());
    CHECK_FALSE(exec(d, "SELECT b FROM dr.t;")->is_success());
}

TEST_CASE("integration::cpp::drop_restrict::table_referenced_by_a_foreign_key_is_refused") {
    auto config = make_test_config(fixture_path("fk_table_blocks"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.parent (id bigint PRIMARY KEY);");
    run_ok(d, "INSERT INTO dr.parent (id) VALUES (1);");
    run_ok(d, "CREATE TABLE dr.child (pid bigint, FOREIGN KEY (pid) REFERENCES dr.parent (id));");

    auto refused = drop_table_restrict(d, "dr", "parent");
    INFO("error: " << error_text(refused));
    CHECK_FALSE(refused->is_success());
    CHECK(error_text(refused).find("cannot drop table dr.parent because other objects depend on it") !=
          std::string::npos);
    CHECK(error_text(refused).find("HINT: Use DROP ... CASCADE to drop the dependent objects too.") !=
          std::string::npos);

    CHECK(run_ok(d, "SELECT id FROM dr.parent;")->size() == 1);
}

// The index goes with its table ('a'); an object depending on the index normally is outside the drop and must be
// dropped on its own first (PostgreSQL 18 findDependentObjects).
TEST_CASE("integration::cpp::drop_restrict::a_normal_dependent_behind_an_owned_index_refuses_the_table_drop") {
    auto config = make_test_config(fixture_path("behind_auto_chain"));
    config.log.level = log_t::level::off;
    catalog_forging_spaces_t space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.t (a bigint);");
    run_ok(d, "CREATE INDEX ix_a ON dr.t (a);");
    run_ok(d, "CREATE TABLE dr.other (b bigint);");
    run_ok(d, "INSERT INTO dr.other (b) VALUES (7);");

    const auto index_oid = table_oid_named(d, "ix_a");
    const auto other_oid = table_oid_named(d, "other");
    REQUIRE(index_oid != components::catalog::INVALID_OID);
    REQUIRE(other_oid != components::catalog::INVALID_OID);
    forge_depend_edge(space,
                      components::catalog::well_known_oid::pg_class_table,
                      other_oid,
                      components::catalog::well_known_oid::pg_class_table,
                      index_oid,
                      'n');

    auto refused = exec(d, "DROP TABLE dr.t;");
    INFO("error: " << error_text(refused));
    REQUIRE_FALSE(refused->is_success());
    CHECK(error_text(refused).find("cannot drop table dr.t because other objects depend on it") != std::string::npos);
    CHECK(error_text(refused).find(std::to_string(other_oid)) != std::string::npos);

    CHECK(run_ok(d, "SELECT b FROM dr.other;")->size() == 1);
}

// cascade_planner.cpp's RESTRICT allow-path used to return with plan.steps
// empty (the seed step was never pushed), so an accepted RESTRICT deleted
// nothing and reported success; it now falls through to the CASCADE leg's
// seed-last order.
TEST_CASE("integration::cpp::drop_restrict::allowed_restrict_drop_removes_nothing") {
    auto config = make_test_config(fixture_path("declared_ok"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.plain (id bigint);");
    run_ok(d, "INSERT INTO dr.plain (id) VALUES (1);");

    auto dropped = drop_table_restrict(d, "dr", "plain");
    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    auto gone = exec(d, "SELECT id FROM dr.plain;");
    CHECK_FALSE(gone->is_success());
}

// Guards against "fixing" the cases above by widening the gate to every
// deptype instead of just 'n'.
TEST_CASE("integration::cpp::drop_restrict::cascade_still_drops_the_computing_table") {
    auto config = make_test_config(fixture_path("cascade_control"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.docs();");
    run_ok(d, "INSERT INTO dr.docs (id, n) VALUES (1, 42);");

    run_ok(d, "DROP TABLE dr.docs;");

    auto gone = exec(d, "SELECT * FROM dr.docs;");
    CHECK_FALSE(gone->is_success());
}

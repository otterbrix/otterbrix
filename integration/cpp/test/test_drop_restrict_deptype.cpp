// ============================================================================
// RESTRICT IS UNREACHABLE FROM SQL, AND THE TWO THINGS THAT WOULD BREAK IF IT
// WERE WIRED UP.
//
// gram.y fills DropStmt::behavior and AlterTableCmd::behavior, and
// `opt_drop_behavior` defaults to DROP_RESTRICT
// (components/sql/parser/gram.y:3108-3112). Everything from the logical node
// down is already wired: node_drop_t::behavior() reaches
// node_dynamic_cascade_delete_t and alter_table_subcommand_t::behavior reaches
// node_alter_column_t (both components/planner/planner.cpp), and both operators
// implement the restrict_ leg. The ONE missing hop is the transformer copying
// the parser's value onto the node — and because the grammar's default is
// RESTRICT, adding that hop is not a wiring change, it is a semantics change for
// EVERY existing statement: 89 `DROP TABLE` and 138 `DROP COLUMN` occurrences in
// this tree, of which only 10 and 1 respectively say CASCADE.
//
// So the hop is deliberately NOT taken here; this file builds the two plans by
// hand with behavior = restrict_ — the same nodes the transformer would produce
// — and runs them through the ordinary execute_plan channel.
//
// Both cases below are CHARACTERIZATION, each with a `correct:` note stating
// what it must do once the defect above it is fixed. Both defects live outside
// this file, so they are measured here and reported, not patched:
//
//   (1) components/physical_plan/operators/operator_computed_field_register.cpp
//       :288 writes the edge (pg_computed_column, attoid) -> (pg_class,
//       table_oid) with deptype 'n' — a table's dependency on ITSELF.
//       catalog::deptype::blocks_restrict is exactly `dt == 'n'`
//       (components/catalog/dependency_walker.hpp:24), so under RESTRICT a
//       computing table is blocked from being dropped BY ITS OWN COLUMNS.
//       PostgreSQL writes 'a' (auto) for an owned object; 'a' still cascades
//       (topological_drop_order does not filter on deptype,
//       cascade_planner.cpp:40-42), it just stops blocking RESTRICT.
//
//   (2) components/physical_plan/operators/operator_alter_column_drop.cpp
//       refuses DROP COLUMN RESTRICT when `dependents` is non-empty, and
//       `dependents` is EVERY pg_depend row keyed on the column, collected
//       unconditionally. The deptype filter its own comment describes ("abort if
//       any non-internal dep exists") is the one that builds `blocking` further
//       up, and it was never applied here — so an index or a constraint on the
//       column, an 'a'/'i' edge PostgreSQL drops ALONG WITH the column, blocks
//       the drop instead.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/sql/transformer/utils.hpp>

#include <unistd.h>

#include <string>

namespace {

    using namespace test_helpers;

    // Fixture roots are qualified by pid: two binaries sharing a literal /tmp
    // path truncate each other's segment files, and the resulting real I/O
    // failures get read as flakes.
    std::string fixture_path(const char* leaf) {
        std::string p = "/tmp/test_drop_restrict_deptype_";
        p += std::to_string(static_cast<long>(::getpid()));
        p += '_';
        p += leaf;
        return p;
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

    // `DROP TABLE <db>.<rel> RESTRICT` as a plan. This is exactly the node
    // transform_drop builds (components/sql/transformer/impl/transform_table.cpp,
    // wrap_one), plus the one line it does not have: set_behavior.
    components::cursor::cursor_t_ptr drop_table_restrict(otterbrix::wrapper_dispatcher_t* d,
                                                         const std::string& database,
                                                         const std::string& relname) {
        auto* resource = d->resource();
        auto node = components::logical_plan::make_node_drop(resource,
                                                             components::logical_plan::drop_target_kind::collection);
        node->set_dbname(database);
        node->set_relname(relname);
        node->set_behavior(components::catalog::drop_behavior_t::restrict_);
        components::logical_plan::execution_plan_t plan{resource,
                                                        node,
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, database, relname);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

    // `ALTER TABLE <db>.<rel> DROP COLUMN <col> RESTRICT` as a plan — the node
    // transform_alter_table builds for AT_DropColumn, plus `sub.behavior`.
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
        node->set_dbname(database);
        node->set_relname(relname);
        components::logical_plan::execution_plan_t plan{resource,
                                                        components::logical_plan::node_ptr{node},
                                                        components::logical_plan::make_parameter_node(resource)};
        components::sql::transform::register_catalog_resolve_table(resource, &plan.catalog_resolves, database, relname);
        return d->execute_plan(otterbrix::session_id_t(), std::move(plan));
    }

} // namespace

// ---------------------------------------------------------------------------
// BLOCKER (1): a computing table's own columns block dropping it under RESTRICT.
//
// `CREATE TABLE ns.docs();` makes a relkind='g' table; the first INSERT
// registers its columns through operator_computed_field_register, which writes
// the self-edge with deptype 'n'. Nothing outside this table depends on it, so
// RESTRICT has nothing to refuse — and refuses anyway.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::computing_table_is_blocked_by_its_own_columns") {
    auto config = make_test_config(fixture_path("own_columns"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.docs();");
    run_ok(d, "INSERT INTO dr.docs (id, n) VALUES (1, 42);");

    // Control: the table really is there and really is readable, so a refusal
    // below is a refusal about dependencies and not about a missing target.
    REQUIRE(run_ok(d, "SELECT * FROM dr.docs;")->size() == 1);

    auto dropped = drop_table_restrict(d, "dr", "docs");

    // characterization. correct: is_success() — the only pg_depend edges on this
    // table are its OWN columns, which PostgreSQL records as 'a' (auto) and
    // drops with the table. Fix at operator_computed_field_register.cpp:288.
    CHECK_FALSE(dropped->is_success());
    CHECK(error_text(dropped).find("DROP RESTRICT: object has dependents") != std::string::npos);

    // And the table is still there, which is the part that makes this a defect
    // rather than a wording quibble: a user who wrote RESTRICT to mean "refuse
    // if anything ELSE depends on it" cannot drop this table at all.
    CHECK(run_ok(d, "SELECT * FROM dr.docs;")->size() == 1);
}

// ---------------------------------------------------------------------------
// BLOCKER (2): DROP COLUMN RESTRICT counts every dependent, not the blocking ones.
//
// An index on the column is an auto dependency: PostgreSQL drops the index
// along with the column and lets RESTRICT through. operator_alter_column_drop
// counts it as a blocker because it never applies the deptype filter it
// already computed further up.
// ---------------------------------------------------------------------------
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

    // characterization. correct: is_success() — the index depends on the column
    // through an auto edge and is dropped with it; nothing EXTERNAL references
    // column a. Fix at operator_alter_column_drop.cpp — filter `dependents`
    // through catalog::deptype::blocks_restrict, the way `blocking` already is.
    CHECK_FALSE(dropped->is_success());
    CHECK(error_text(dropped).find("DROP COLUMN RESTRICT: column has dependent objects") != std::string::npos);

    // The column survived, so this is a statement that cannot be expressed at
    // all rather than one that is merely worded oddly.
    auto after = run_ok(d, "SELECT a FROM dr.t;");
    CHECK(after->size() == 2);
}

// ---------------------------------------------------------------------------
// BLOCKER (3), found by the control case for blocker (1) and worse than either:
// THE RESTRICT PATH THAT ALLOWS A DROP DROPS NOTHING, AND REPORTS SUCCESS.
//
// A DECLARED table owns no pg_computed_column self-edges, so it reaches the
// allow-path that blockers (1) and (2) never get to — and that path is:
//
//   components/catalog/cascade_planner.cpp:14-25
//     if (behavior == drop_behavior_t::restrict_) {
//         for (const auto& d : deps) { ...blocks_restrict → restrict_blocked... }
//         // No external deps → RESTRICT allows the drop (only auto/internal children).
//         return plan;                     // <-- plan.steps is EMPTY
//     }
//
// The CASCADE path below it ends with `plan.steps.push_back({seed_classid, seed_oid,
// 'n'})` (:43) — the seed is what makes the drop delete the object's own catalog rows.
// The RESTRICT allow-path never pushes it, nor the auto/internal children its own
// comment says it is allowing, and hands back a plan with zero steps.
//
// operator_dynamic_cascade_delete then dedups zero steps into zero steps, probes
// nothing, emits no delete spec, and runs to mark_executed(). So `DROP TABLE t
// RESTRICT` on a table nothing depends on is a COMPLETE no-op that answers success —
// the "success over the rows it did not touch" shape, in the planner rather than the
// operator. Fixing (1) and (2) would only widen the set of statements that reach it.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::allowed_restrict_drop_removes_nothing") {
    auto config = make_test_config(fixture_path("declared_ok"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.plain (id bigint);");
    run_ok(d, "INSERT INTO dr.plain (id) VALUES (1);");

    // The statement is ACCEPTED — nothing blocking depends on this table, so
    // RESTRICT has nothing to refuse. This half is correct and stays.
    auto dropped = drop_table_restrict(d, "dr", "plain");
    INFO("error: " << error_text(dropped));
    CHECK(dropped->is_success());

    // characterization. correct: CHECK_FALSE(...is_success()) — the table was
    // reported dropped, so selecting from it must fail. Today the accepted DROP
    // deleted nothing and the table is still fully live, rows and all.
    // Fix at components/catalog/cascade_planner.cpp:23-24 — the allow-path must
    // build the same seed-last step list the CASCADE path does.
    auto gone = exec(d, "SELECT id FROM dr.plain;");
    CHECK(gone->is_success());
    CHECK(gone->size() == 1);
}

// ---------------------------------------------------------------------------
// The control the two cases above need: CASCADE — today's only reachable
// behaviour, and the one every SQL statement in the tree gets — is unaffected.
// If the two defects above are fixed by weakening the restrict_ leg rather
// than by correcting the deptype, this case still passes and the two above
// start passing too, so it is here to keep the CASCADE contract pinned while
// that work happens.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::drop_restrict::cascade_still_drops_the_computing_table") {
    auto config = make_test_config(fixture_path("cascade_control"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE dr;");
    run_ok(d, "CREATE TABLE dr.docs();");
    run_ok(d, "INSERT INTO dr.docs (id, n) VALUES (1, 42);");

    // Plain SQL: the transformer leaves node_drop_t at its cascade_ default.
    run_ok(d, "DROP TABLE dr.docs;");

    auto gone = exec(d, "SELECT * FROM dr.docs;");
    CHECK_FALSE(gone->is_success());
}

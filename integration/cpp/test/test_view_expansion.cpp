// View expansion: what a query OVER a view answers.
//
// Three defects meet in this file.
//
// [D1] The stored body IS the semantics — it is re-parsed on every read. A body
//      reconstructed by searching the raw SQL for the substring " AS " (and
//      defaulting to "SELECT *" when the search misses) breaks on a newline after
//      AS or on `AS(SELECT ...)`: CREATE VIEW reports SUCCESS and
//      pg_rewrite.ev_action holds a query the user never wrote.
//
// [D2] Expansion must SPLICE the view body under whatever is built above it.
//      Replacing the whole plan with the body
//      (`plan.sub_queries.back() = std::move(expanded_plan)`) drops the outer
//      WHERE, the narrowed projection, the aggregate, the join, and hands back the
//      unfiltered body as a successful answer. That is a wrong answer, not a
//      missing feature, which is why every case below checks CONTENT (rows /
//      columns / values) rather than cursor status.
//
// [D3] CREATE MATERIALIZED VIEW cannot populate anything: nothing in the pipeline
//      populates a matview at CREATE time and REFRESH MATERIALIZED VIEW is not
//      lowered either. The implicit (PostgreSQL default) WITH DATA form is
//      therefore refused loudly; WITH NO DATA still creates the empty matview it
//      names.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>

using namespace test_helpers;
using components::cursor::cursor_t_ptr;

namespace {

    // t: four rows, two of them over the view's own threshold of 10.
    //
    //   col_a | col_b
    //    'a'  |   5
    //    'b'  |  15
    //    'c'  |  20
    //    'd'  |   8
    //
    // v = SELECT col_a, col_b FROM t WHERE col_b > 10   -> rows 'b' and 'c'.
    //
    // The constant 10 lives IN THE BODY on purpose: it is the only way to catch a
    // parameter-id collision between the body and an outer WHERE, which produces a
    // silently wrong row set and no error at all.
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE vx;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE vx.t (col_a STRING, col_b BIGINT);")->is_success());
        REQUIRE(exec(d,
                     "INSERT INTO vx.t (col_a, col_b) VALUES ('a', 5), ('b', 15), ('c', 20), ('d', 8);")
                    ->is_success());
        REQUIRE(exec(d, "CREATE VIEW vx.v AS SELECT col_a, col_b FROM vx.t WHERE col_b > 10;")->is_success());
    }

    std::set<std::string> aliases(const cursor_t_ptr& cur) {
        std::set<std::string> s;
        if (cur->is_success() && !cur->chunks().empty()) {
            const auto& chunk = cur->chunks().front();
            for (size_t c = 0; c < chunk.column_count(); ++c) {
                s.insert(std::string(chunk.data[c].type().alias()));
            }
        }
        return s;
    }

    size_t col_of(const cursor_t_ptr& cur, const std::string& alias) {
        const auto& chunk = cur->chunks().front();
        for (size_t c = 0; c < chunk.column_count(); ++c) {
            if (std::string(chunk.data[c].type().alias()) == alias) {
                return c;
            }
        }
        FAIL("no column aliased '" << alias << "'");
        return 0;
    }

    // Every value of one string column, as a set.
    std::set<std::string> str_set(const cursor_t_ptr& cur, const std::string& alias) {
        std::set<std::string> s;
        REQUIRE(cur->is_success());
        if (cur->chunks().empty()) {
            return s;
        }
        const auto column = col_of(cur, alias);
        for (const auto& chunk : cur->chunks()) {
            for (size_t r = 0; r < chunk.size(); ++r) {
                const auto cell = chunk.value(column, r);
                if (!cell.is_null()) {
                    s.insert(cell.value<const std::string&>());
                }
            }
        }
        return s;
    }

    size_t column_count(const cursor_t_ptr& cur) {
        REQUIRE(cur->is_success());
        return cur->chunks().empty() ? 0 : cur->chunks().front().column_count();
    }

} // namespace

// [D2] A narrowed projection over a view returns the columns the OUTER query
// asked for. The whole-plan replacement returned the body's two columns instead
// — the behaviour test_jsonb_support::view_over_navigation used to describe in a
// comment ("a narrowed projection over the view is ignored") without pinning it.
TEST_CASE("integration::cpp::test_view_expansion::narrowed_projection_over_view") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/projection"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT col_a FROM vx.v;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 2);
    CHECK(column_count(cur) == 1);
    CHECK(aliases(cur) == std::set<std::string>{"col_a"});
}

// [D2] An outer WHERE over a view filters the view's rows. The replacement
// dropped the outer predicate and answered with the body's own row set.
TEST_CASE("integration::cpp::test_view_expansion::outer_where_over_view") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/where"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT * FROM vx.v WHERE col_b > 18;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 1);
    CHECK(str_set(cur, "col_a") == std::set<std::string>{"c"});
}

// [D2] An aggregate over a view aggregates the view's rows. The replacement
// dropped the aggregate entirely and returned the body's rows.
TEST_CASE("integration::cpp::test_view_expansion::aggregate_over_view") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/aggregate"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT COUNT(*) AS n FROM vx.v;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
    CHECK(cur->chunks().front().get_value<int64_t>(col_of(cur, "n"), 0) == 2);
}

// [D2] The view's own constant survives the outer query's constant.
//
// Both plans number their bound parameters from zero (parameter_node_t::counter_
// is per node), so merging the body's parameter map into the outer plan's under
// the SAME ids let `> 18` overwrite the body's `> 10`. Nothing errors when that
// happens — the row set is just quietly wrong — so the pin is the equality
// against the hand-written equivalent query on the base table.
TEST_CASE("integration::cpp::test_view_expansion::view_constant_not_clobbered_by_outer_constant") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/params"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto through_view = exec(d, "SELECT col_a FROM vx.v WHERE col_b > 18;");
    auto direct = exec(d, "SELECT col_a FROM vx.t WHERE col_b > 10 AND col_b > 18;");
    REQUIRE(through_view->is_success());
    REQUIRE(direct->is_success());
    CHECK(through_view->size() == direct->size());
    CHECK(str_set(through_view, "col_a") == str_set(direct, "col_a"));
    // and it is the answer the view's own threshold implies, not the outer one's
    CHECK(str_set(through_view, "col_a") == std::set<std::string>{"c"});
}

// [D2] A join whose left side is a view keeps the join. The replacement
// answered with the bare view body and no join at all.
TEST_CASE("integration::cpp::test_view_expansion::join_with_view_side") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/join"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE TABLE vx.t2 (col_a STRING, tag STRING);")->is_success());
    REQUIRE(exec(d, "INSERT INTO vx.t2 (col_a, tag) VALUES ('b', 'B'), ('c', 'C'), ('d', 'D');")->is_success());

    auto cur = exec(d, "SELECT vx.v.col_a, vx.t2.tag FROM vx.v JOIN vx.t2 ON vx.v.col_a = vx.t2.col_a;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 2);
    CHECK(str_set(cur, "tag") == std::set<std::string>{"B", "C"});
}

// [D2] A view over a view resolves both levels. Each pass splices one level
// and the level it added only becomes visible after its own resolve round.
TEST_CASE("integration::cpp::test_view_expansion::view_over_view") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/nested"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE VIEW vx.v2 AS SELECT col_a, col_b FROM vx.v;")->is_success());

    auto cur = exec(d, "SELECT * FROM vx.v2;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 2);
    CHECK(str_set(cur, "col_a") == std::set<std::string>{"b", "c"});
}

// [D2] INSERT through a view is refused, and SAYS SO.
//
// This one was NOT red: an INSERT naming a view already failed before the fix,
// because a view carries no pg_attribute columns and the column binding has
// nothing to bind to — an accident of the validator, phrased as a column error.
// It is pinned anyway because the bind guard added for D2 stops the view's oid
// from being stamped on the DML node, and without an explicit refusal the
// accident that produced the old error would be gone too. The message is part of
// the pin: it has to name the view, not a missing column.
TEST_CASE("integration::cpp::test_view_expansion::dml_through_view_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/dml"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto ins = exec(d, "INSERT INTO vx.v (col_a, col_b) VALUES ('z', 99);");
    REQUIRE_FALSE(ins->is_success());
    CHECK(std::string(ins->get_error().what.c_str()).find("view") != std::string::npos);

    // and the base table is untouched
    auto after = exec(d, "SELECT col_a FROM vx.t;");
    REQUIRE(after->is_success());
    CHECK(after->size() == 4);
}

// DROP VIEW still finds the view's oid.
//
// The D2 bind guard stops a relkind='v' entry from being pasted onto a query node,
// and it is deliberately narrow: DROP reaches the view's oid through the
// drop_target_kind::view branch of the same function, which the guard does not
// touch. There was no e2e coverage of DROP VIEW at all, so the guard's blast
// radius is pinned here.
TEST_CASE("integration::cpp::test_view_expansion::drop_view_still_resolves_the_view") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/drop"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    REQUIRE(exec(d, "SELECT * FROM vx.v;")->is_success());
    REQUIRE(exec(d, "DROP VIEW vx.v;")->is_success());

    INFO("the view is gone");
    CHECK_FALSE(exec(d, "SELECT * FROM vx.v;")->is_success());

    INFO("and its base table is not");
    auto base = exec(d, "SELECT col_a FROM vx.t;");
    REQUIRE(base->is_success());
    CHECK(base->size() == 4);
}

// [D1] The stored body is the query that was written, whatever whitespace and
// bracketing it was written with. Read back through the view: if the body were
// the old "SELECT *" default the view would answer with every row of t (or fail
// to resolve at all), and if the slice ran past the body it would carry the
// trailing clause into the stored SQL.
TEST_CASE("integration::cpp::test_view_expansion::body_is_what_was_written") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/body"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    INFO("newline after AS — the old substring search for \" AS \" missed this one");
    REQUIRE(exec(d, "CREATE VIEW vx.v_nl AS\nSELECT col_a FROM vx.t WHERE col_b > 10;")->is_success());
    auto nl = exec(d, "SELECT * FROM vx.v_nl;");
    REQUIRE(nl->is_success());
    CHECK(nl->size() == 2);
    CHECK(str_set(nl, "col_a") == std::set<std::string>{"b", "c"});

    INFO("AS(SELECT ...) — no space before the paren, also missed");
    REQUIRE(exec(d, "CREATE VIEW vx.v_par AS(SELECT col_a FROM vx.t WHERE col_b > 18);")->is_success());
    auto par = exec(d, "SELECT * FROM vx.v_par;");
    REQUIRE(par->is_success());
    CHECK(par->size() == 1);
    CHECK(str_set(par, "col_a") == std::set<std::string>{"c"});
}

// [D1] A column alias list renames the body's output columns and is carried
// nowhere, so accepting it would promise column names the stored body does not
// produce. Refused (rule 6) instead of half-supported.
TEST_CASE("integration::cpp::test_view_expansion::view_column_alias_list_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/aliases"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK_FALSE(exec(d, "CREATE VIEW vx.v_alias (x) AS SELECT col_a FROM vx.t;")->is_success());
}

// [D3] CREATE MATERIALIZED VIEW without an explicit WITH NO DATA is refused.
// It used to report success and leave `SELECT * FROM mv` answering zero rows
// forever, with nothing said. WITH NO DATA — the form whose meaning IS an empty
// matview — keeps working.
TEST_CASE("integration::cpp::test_view_expansion::matview_without_no_data_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/matview"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    INFO("the implicit (PostgreSQL default) WITH DATA form is a loud error");
    CHECK_FALSE(exec(d, "CREATE MATERIALIZED VIEW vx.mv AS SELECT col_a FROM vx.t WHERE col_b > 10;")->is_success());

    INFO("WITH NO DATA still creates the empty matview it names");
    REQUIRE(exec(d, "CREATE MATERIALIZED VIEW vx.mv2 AS SELECT col_a FROM vx.t WHERE col_b > 10 WITH NO DATA;")
                ->is_success());
    auto cur = exec(d, "SELECT * FROM vx.mv2;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 0);
}

// [D2] TWO view references in ONE plan — the case a single reference cannot
// reach.
//
// Each reference gets its OWN parse+transform of the body. Sharing one subtree
// between both sides would corrupt it, because the spliced nodes carry
// per-reference state (table_oid, table_metadata, output_types, projected_cols,
// read_cap) and filter pushdown APPENDS a match child into the body it pushes
// into.
//
// It is also the reason the driver snapshots every reference's body SQL before
// merging any resolves: merge_catalog_resolves appends to the entries vector,
// which reallocates it, and the collected references point INTO that vector.
TEST_CASE("integration::cpp::test_view_expansion::same_view_referenced_twice") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/twice"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    INFO("the same view on both sides of a join");
    // v is {'b',15}, {'c',20}. Joined to itself on col_a that is 2 rows, and the
    // hand-written equivalent over the base table says the same.
    auto through_view = exec(d, "SELECT a.col_a FROM vx.v AS a JOIN vx.v AS b ON a.col_a = b.col_a;");
    auto direct = exec(d,
                       "SELECT a.col_a FROM vx.t AS a JOIN vx.t AS b ON a.col_a = b.col_a "
                       "WHERE a.col_b > 10 AND b.col_b > 10;");
    REQUIRE(through_view->is_success());
    REQUIRE(direct->is_success());
    CHECK(through_view->size() == direct->size());
    CHECK(str_set(through_view, "col_a") == str_set(direct, "col_a"));
    // Both thresholds survived: 'a' (5) and 'd' (8) are below the view's own 10.
    CHECK(str_set(through_view, "col_a") == std::set<std::string>{"b", "c"});
}

// [D2] Two DIFFERENT views in one plan, each with its own constant. This is
// what pins renumbering ACROSS bodies: every body numbers its constants from 0
// (parameter_node_t::counter_ is per node), so without a fresh id per body both
// bodies read the same slot and one silently overwrites the other.
//
// The constants are chosen so that EITHER collision outcome is wrong, whichever
// body happens to write the shared slot last:
//
//   v = col_b > 10  -> {b,c}      w = col_b < 18  -> {a,b,d}
//   correct join on col_a          -> {b}
//   shared slot holds 10: v > 10 = {b,c},  w < 10 = {a,d}   -> {}
//   shared slot holds 18: v > 18 = {c},    w < 18 = {a,b,d} -> {}
//
// Nothing fails in either case — the row set is just quietly wrong.
TEST_CASE("integration::cpp::test_view_expansion::two_views_keep_their_own_constants") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/two_views"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    REQUIRE(exec(d, "CREATE VIEW vx.w AS SELECT col_a, col_b FROM vx.t WHERE col_b < 18;")->is_success());

    auto through_views = exec(d, "SELECT a.col_a FROM vx.v AS a JOIN vx.w AS b ON a.col_a = b.col_a;");
    auto direct = exec(d,
                       "SELECT a.col_a FROM vx.t AS a JOIN vx.t AS b ON a.col_a = b.col_a "
                       "WHERE a.col_b > 10 AND b.col_b < 18;");
    REQUIRE(through_views->is_success());
    REQUIRE(direct->is_success());
    CHECK(through_views->size() == direct->size());
    CHECK(str_set(through_views, "col_a") == str_set(direct, "col_a"));
    CHECK(str_set(through_views, "col_a") == std::set<std::string>{"b"});
}

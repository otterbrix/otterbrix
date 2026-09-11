// [D1] The view body is re-parsed on every read via a raw-SQL search for " AS ", so a newline after AS or
//      `AS(SELECT ...)` silently swaps in the wrong query.
// [D2] Expansion splices the body under whatever is built above it, so every case here checks CONTENT, not
//      cursor status.
// [D3] CREATE MATERIALIZED VIEW never populates data and REFRESH is not lowered, so implicit WITH DATA is refused.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>

using namespace test_helpers;
using components::cursor::cursor_t_ptr;

namespace {

    // Threshold 10 lives in the body on purpose: only there can it catch a parameter-id collision with an outer WHERE.
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE vx;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE vx.t (col_a STRING, col_b BIGINT);")->is_success());
        REQUIRE(
            exec(d, "INSERT INTO vx.t (col_a, col_b) VALUES ('a', 5), ('b', 15), ('c', 20), ('d', 8);")->is_success());
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

// [D2] Also described but never pinned by test_jsonb_support::view_over_navigation.
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

// Both plans number parameters from zero, so merging the parameter map under shared ids let `> 18` overwrite `> 10`.
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
    CHECK(str_set(through_view, "col_a") == std::set<std::string>{"c"});
}

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

// Each pass splices one level; the level it adds only becomes visible after its own resolve round.
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

// A view carries no pg_attribute columns, so without explicit refusal this fails only by accident, as a column error.
TEST_CASE("integration::cpp::test_view_expansion::dml_through_view_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/dml"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto ins = exec(d, "INSERT INTO vx.v (col_a, col_b) VALUES ('z', 99);");
    REQUIRE_FALSE(ins->is_success());
    CHECK(std::string(ins->get_error().what.c_str()).find("view") != std::string::npos);

    auto after = exec(d, "SELECT col_a FROM vx.t;");
    REQUIRE(after->is_success());
    CHECK(after->size() == 4);
}

// The D2 bind guard blocks relkind='v' on a query node but spares drop_target_kind::view (how DROP reaches the oid).
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

// A column alias list is carried nowhere, so accepting it would promise column names the stored body does not produce.
TEST_CASE("integration::cpp::test_view_expansion::view_column_alias_list_is_refused") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/aliases"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK_FALSE(exec(d, "CREATE VIEW vx.v_alias (x) AS SELECT col_a FROM vx.t;")->is_success());
}

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

// Each reference needs its own parse+transform, since spliced nodes carry per-reference state. The driver also
// snapshots each body's SQL first because merge_catalog_resolves reallocates the vector references point into.
TEST_CASE("integration::cpp::test_view_expansion::same_view_referenced_twice") {
    auto config = make_test_config(integration_fixture_path("test_view_expansion/twice"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    INFO("the same view on both sides of a join");
    auto through_view = exec(d, "SELECT a.col_a FROM vx.v AS a JOIN vx.v AS b ON a.col_a = b.col_a;");
    auto direct = exec(d,
                       "SELECT a.col_a FROM vx.t AS a JOIN vx.t AS b ON a.col_a = b.col_a "
                       "WHERE a.col_b > 10 AND b.col_b > 10;");
    REQUIRE(through_view->is_success());
    REQUIRE(direct->is_success());
    CHECK(through_view->size() == direct->size());
    CHECK(str_set(through_view, "col_a") == str_set(direct, "col_a"));
    CHECK(str_set(through_view, "col_a") == std::set<std::string>{"b", "c"});
}

// Pins renumbering across bodies: v's 10 and w's 18 are chosen so a collision either way returns a wrong result.
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

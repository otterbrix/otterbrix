// otterbrix has no native json/jsonb type; nine postgres-spelled jsonb operators work by
// name-mangling over flattened columns. Cases are SUPPORTED or BUG (a "correct:" comment gives
// the intended result). Regression c59d95e8 (#622) refuses table-valued operators (-> #> - #-)
// in the select list. The codec is components/expressions/jsonb_path.hpp.
//
// NOT PINNABLE: `SELECT CASE WHEN t #>> 'a.b' = 10 ... FROM t` segfaults on a NULL leaf row
// (general 3VL bug) -- cannot live in a test binary.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>
#include <unistd.h>

using namespace test_helpers;
using components::cursor::cursor_t_ptr;

namespace {

    std::string fixture_dir(const char* leaf) {
        return integration_fixture_path(std::string("test_jsonb_matrix/") + leaf).string();
    }

    // Four rows over a computing table, deliberately ragged so per-row-absent keys are covered.
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());
        REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b, a.c, x) VALUES (1, 10, 20, 'p'), (2, 30, 40, 'q');")->is_success());
        REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b, a.c) VALUES (3, 50, 60);")->is_success());
        REQUIRE(exec(d, "INSERT INTO jp.t (id, a.c) VALUES (4, 70);")->is_success());
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

    int64_t i64(const cursor_t_ptr& cur, const std::string& alias, size_t row) {
        return cur->chunks().front().get_value<int64_t>(col_of(cur, alias), row);
    }

    std::string str(const cursor_t_ptr& cur, const std::string& alias, size_t row) {
        return cur->chunks().front().value(col_of(cur, alias), row).value<const std::string&>();
    }

    bool is_null(const cursor_t_ptr& cur, const std::string& alias, size_t row) {
        return cur->chunks().front().value(col_of(cur, alias), row).is_null();
    }

    components::types::logical_type type_of(const cursor_t_ptr& cur, const std::string& alias) {
        return cur->chunks().front().data[col_of(cur, alias)].type().type();
    }

    // Used where ORDER BY can't apply: otterbrix rejects an output alias as a sort key.
    std::set<int64_t> i64_set(const cursor_t_ptr& cur, const std::string& alias) {
        std::set<int64_t> s;
        REQUIRE(cur->is_success());
        if (!cur->chunks().empty()) {
            const auto& chunk = cur->chunks().front();
            const auto col = col_of(cur, alias);
            for (size_t r = 0; r < chunk.size(); ++r) {
                if (!chunk.value(col, r).is_null()) {
                    s.insert(chunk.get_value<int64_t>(col, r));
                }
            }
        }
        return s;
    }

    std::set<int64_t> ids(const cursor_t_ptr& cur) { return i64_set(cur, "id"); }

    void check_value_position_refusal(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        INFO(sql);
        auto cur = exec(d, sql);
        REQUIRE(cur);
        REQUIRE_FALSE(cur->is_success());
        CHECK(std::string(cur->get_error().what).find("is not valid in a value position") != std::string::npos);
    }

} // namespace

TEST_CASE("integration::cpp::test_jsonb_support::flattened_storage_model") {
    auto config = make_test_config(fixture_dir("storage"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT * FROM jp.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 4);
    CHECK(aliases(cur) == std::set<std::string>{"id", "a/b", "a/c", "x"});
    CHECK(i64(cur, "a/b", 0) == 10);
    CHECK(i64(cur, "a/c", 0) == 20);
    CHECK(str(cur, "x", 0) == "p");
    CHECK(is_null(cur, "x", 2));   // row 3 never supplied x
    CHECK(is_null(cur, "a/b", 3)); // row 4 never supplied a.b
    CHECK(i64(cur, "a/c", 3) == 70);
}

TEST_CASE("integration::cpp::test_jsonb_support::extract_scalar") {
    auto config = make_test_config(fixture_dir("extract"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("top-level key") {
        auto cur = exec(d, "SELECT id, t ->> 'x' AS v FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        CHECK(str(cur, "v", 0) == "p");
        CHECK(str(cur, "v", 1) == "q");
        CHECK(is_null(cur, "v", 2));
        CHECK(is_null(cur, "v", 3));
    }

    SECTION("nested path: all four spellings agree") {
        const char* q[] = {"SELECT id, t -> 'a' ->> 'b' AS v FROM jp.t ORDER BY id;",
                           "SELECT id, t #>> 'a.b' AS v FROM jp.t ORDER BY id;",
                           "SELECT id, t #>> '{a,b}' AS v FROM jp.t ORDER BY id;",
                           "SELECT id, t #>> '{ a , b }' AS v FROM jp.t ORDER BY id;"};
        for (const auto* sql : q) {
            INFO(sql);
            auto cur = exec(d, sql);
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 4);
            CHECK(i64(cur, "v", 0) == 10);
            CHECK(i64(cur, "v", 1) == 30);
            CHECK(i64(cur, "v", 2) == 50);
            CHECK(is_null(cur, "v", 3));
        }
    }

    SECTION("the extracted value keeps its native column type — ->> does NOT return text") {
        auto cur = exec(d, "SELECT t ->> 'x' AS s, t #>> 'a.b' AS n FROM jp.t;");
        REQUIRE(cur->is_success());
        CHECK(type_of(cur, "s") == components::types::logical_type::STRING_LITERAL);
        CHECK(type_of(cur, "n") == components::types::logical_type::BIGINT);
    }

    SECTION("navigation works through a table alias, which hides the base name") {
        CHECK(i64(exec(d, "SELECT tt #>> 'a.b' AS v FROM jp.t AS tt ORDER BY id;"), "v", 0) == 10);
        CHECK_FALSE(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t AS tt ORDER BY id;")->is_success());
    }

    SECTION("keys are case-sensitive") { CHECK_FALSE(exec(d, "SELECT t ->> 'X' AS v FROM jp.t;")->is_success()); }
}

// Table-valued (widens to one column per child). correct: -> 'a' / #> '{a}' expand to {b, c}.
TEST_CASE("integration::cpp::test_jsonb_support::expand_object") {
    auto config = make_test_config(fixture_dir("expand"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("select-list expansion is refused (regression)") {
        check_value_position_refusal(d, "SELECT t -> 'a' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t #> '{a}' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT id, t -> 'a' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t -> 'a' -> 'b' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t -> 'x' FROM jp.t ORDER BY id;");
    }

    SECTION("a table-valued operator cannot be used as a scalar — and says so") {
        auto cur = exec(d, "SELECT id FROM jp.t WHERE t #> '{a}' = 1;");
        REQUIRE_FALSE(cur->is_success());
        CHECK(std::string(cur->get_error().what).find("cannot be used as a scalar") != std::string::npos);
    }
}

// Projects every column except the named key/subtree. correct: - 'x' -> {id,a/b,a/c}; - 'nokey' no-op.
TEST_CASE("integration::cpp::test_jsonb_support::delete_keys") {
    auto config = make_test_config(fixture_dir("delete"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("select-list key deletion is refused (regression)") {
        check_value_position_refusal(d, "SELECT t - 'x' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t - 'a' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t #- 'a.b' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t #- '{a}' FROM jp.t ORDER BY id;");
        check_value_position_refusal(d, "SELECT t - 'nokey' FROM jp.t ORDER BY id;");
    }
}

// Exists means the column exists and the row's value is non-null; a ragged row tests false.
TEST_CASE("integration::cpp::test_jsonb_support::existence_predicates") {
    auto config = make_test_config(fixture_dir("exists"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x';")) == std::set<int64_t>{1, 2});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE NOT (t ? 'x');")) == std::set<int64_t>{3, 4});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x' AND t ? 'id';")) == std::set<int64_t>{1, 2});

    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?| '{x,id}';")) == std::set<int64_t>{1, 2, 3, 4});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?& '{x,id}';")) == std::set<int64_t>{1, 2});

    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ?| '{}';")->size() == 0);
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?& '{}';")) == std::set<int64_t>{1, 2, 3, 4});

    auto cnt = exec(d, "SELECT COUNT(*) AS n FROM jp.t WHERE t ? 'x';");
    REQUIRE(cnt->is_success());
    CHECK(cnt->chunks().front().get_value<uint64_t>(0, 0) == 2);
}

TEST_CASE("integration::cpp::test_jsonb_support::predicates_and_dml") {
    auto config = make_test_config(fixture_dir("dml"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("comparison, BETWEEN, boolean composition") {
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = 10;")) == std::set<int64_t>{1});
        auto coerced = exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = '10';");
        REQUIRE_FALSE(coerced->is_success());
        CHECK(std::string(coerced->get_error().what).find("no type is common") != std::string::npos);
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' BETWEEN 5 AND 15;")) == std::set<int64_t>{1});
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = 10 OR t #>> 'a.c' = 40;")) ==
              std::set<int64_t>{1, 2});
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = 10 AND t ? 'x';")) == std::set<int64_t>{1});
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ->> 'x' = 'p';")) == std::set<int64_t>{1});
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ->> 'id' = 1;")) == std::set<int64_t>{1});
    }

    SECTION("UPDATE ... WHERE <navigation> touches exactly the matching row") {
        REQUIRE(exec(d, "UPDATE jp.t SET x = 'z' WHERE t #>> 'a.b' = 30;")->is_success());
        auto cur = exec(d, "SELECT id, x FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(str(cur, "x", 0) == "p");
        CHECK(str(cur, "x", 1) == "z");
        CHECK(is_null(cur, "x", 2));
    }

    SECTION("DELETE ... WHERE <navigation> removes exactly the matching row") {
        REQUIRE(exec(d, "DELETE FROM jp.t WHERE t #>> 'a.c' = 60;")->is_success());
        CHECK(ids(exec(d, "SELECT id FROM jp.t ORDER BY id;")) == std::set<int64_t>{1, 2, 4});
    }

    SECTION("a navigating subquery drives an IN predicate") {
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE id IN (SELECT id FROM jp.t WHERE t #>> 'a.b' = 10);")) ==
              std::set<int64_t>{1});
    }
}

TEST_CASE("integration::cpp::test_jsonb_support::navigation_in_expressions") {
    auto config = make_test_config(fixture_dir("expr"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("arithmetic over a navigated leaf") {
        auto plus = exec(d, "SELECT id, (t #>> 'a.c') + 1 AS v FROM jp.t ORDER BY id;");
        REQUIRE(plus->is_success());
        CHECK(i64(plus, "v", 0) == 21);
        CHECK(i64(plus, "v", 3) == 71);

        auto mul = exec(d, "SELECT id, (t #>> 'a.c') * 2 AS v FROM jp.t ORDER BY id;");
        REQUIRE(mul->is_success());
        CHECK(i64(mul, "v", 1) == 80);

        auto minus = exec(d, "SELECT id, (t #>> 'a.c') - 1 AS v FROM jp.t ORDER BY id;");
        REQUIRE(minus->is_success());
        CHECK(i64(minus, "v", 2) == 59);
    }

    SECTION("aggregates work over a navigated leaf once it is wrapped in arithmetic") {
        // SUM(nav) alone fails ("unable to parse value"); + 0 makes it arithmetic the aggregate accepts.
        auto s = exec(d, "SELECT SUM((t #>> 'a.c') + 0) AS s FROM jp.t;");
        REQUIRE(s->is_success());
        CHECK(i64(s, "s", 0) == 190);

        auto m = exec(d, "SELECT MIN((t #>> 'a.c') + 0) AS s FROM jp.t;");
        REQUIRE(m->is_success());
        CHECK(i64(m, "s", 0) == 20);
    }

    SECTION("CASE over a fully-populated leaf") {
        auto cur = exec(d, "SELECT id, CASE WHEN t #>> 'a.c' = 20 THEN 1 ELSE 0 END AS v FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(i64(cur, "v", 0) == 1);
        CHECK(i64(cur, "v", 1) == 0);
    }

    SECTION("DISTINCT and LIMIT over a navigated leaf") {
        CHECK(exec(d, "SELECT DISTINCT t ->> 'x' AS v FROM jp.t;")->size() == 3);
        CHECK(exec(d, "SELECT t #>> 'a.c' AS v FROM jp.t LIMIT 2;")->size() == 2);
    }
}

// Not gated on relkind: also works over an ordinary table. correct: SELECT r -> 'v' yields {v}.
TEST_CASE("integration::cpp::test_jsonb_support::regular_table") {
    auto config = make_test_config(fixture_dir("regular"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.r (id BIGINT, v BIGINT, s TEXT);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.r (id, v, s) VALUES (1, 10, 'p'), (2, 30, 'q');")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.r (id) VALUES (3);")->is_success());

    auto nav = exec(d, "SELECT id, r ->> 'v' AS nv FROM jp.r ORDER BY id;");
    REQUIRE(nav->is_success());
    CHECK(i64(nav, "nv", 0) == 10);
    CHECK(is_null(nav, "nv", 2));

    check_value_position_refusal(d, "SELECT r -> 'v' FROM jp.r ORDER BY id;");

    CHECK(ids(exec(d, "SELECT id FROM jp.r WHERE r ->> 'v' = 10;")) == std::set<int64_t>{1});
    CHECK(ids(exec(d, "SELECT id FROM jp.r WHERE r ? 'v';")) == std::set<int64_t>{1, 2});
}

TEST_CASE("integration::cpp::test_jsonb_support::two_table_join") {
    auto config = make_test_config(fixture_dir("join"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, lv, d.e) VALUES (1, 100, 111), (2, 200, 222);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, mv, d.e) VALUES (1, 10, 11), (2, 20, 22);")->is_success());

    SECTION("navigation into the right-hand table, in the SELECT list") {
        auto cur = exec(d, "SELECT m #>> 'd.e' AS mde FROM jp.l JOIN jp.m ON l.k = m.k;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        CHECK(i64_set(cur, "mde") == std::set<int64_t>{11, 22});
    }

    SECTION("navigation into either table, in the WHERE clause") {
        auto right = exec(d, "SELECT l.k AS id FROM jp.l JOIN jp.m ON l.k = m.k WHERE m #>> 'mv' = 10;");
        CHECK(ids(right) == std::set<int64_t>{1});

        auto left = exec(d, "SELECT l.k AS id FROM jp.l JOIN jp.m ON l.k = m.k WHERE l #>> 'lv' = 200;");
        CHECK(ids(left) == std::set<int64_t>{2});
    }

    SECTION("navigation as the join condition itself") {
        auto cur = exec(d, "SELECT l.k AS id FROM jp.l JOIN jp.m ON l #>> 'k' = m #>> 'k';");
        CHECK(ids(cur) == std::set<int64_t>{1, 2});
    }
}

// Flattened columns survive a WAL/disk round-trip. correct: aliases == {b, c}.
TEST_CASE("integration::cpp::test_jsonb_support::persistence") {
    auto config = make_test_config(fixture_dir("persist"));
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        seed(d);
        CHECK(i64(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t ORDER BY id;"), "v", 0) == 10);
    }
    {
        test_spaces space(config);
        auto* d = space.dispatcher();

        auto star = exec(d, "SELECT * FROM jp.t ORDER BY id;");
        REQUIRE(star->is_success());
        REQUIRE(star->size() == 4);
        CHECK(aliases(star) == std::set<std::string>{"id", "a/b", "a/c", "x"});

        auto nav = exec(d, "SELECT id, t #>> 'a.b' AS v, t ->> 'x' AS s FROM jp.t ORDER BY id;");
        REQUIRE(nav->is_success());
        CHECK(i64(nav, "v", 0) == 10);
        CHECK(i64(nav, "v", 2) == 50);
        CHECK(is_null(nav, "v", 3));
        CHECK(str(nav, "s", 1) == "q");

        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x';")) == std::set<int64_t>{1, 2});
        check_value_position_refusal(d, "SELECT t -> 'a' FROM jp.t;");
    }
}

// A view is the one way to hand a navigated alias to an outer query; a CTE loses it.
TEST_CASE("integration::cpp::test_jsonb_support::view_over_navigation") {
    auto config = make_test_config(fixture_dir("view"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    REQUIRE(exec(d, "CREATE VIEW jp.v AS SELECT id, t #>> 'a.b' AS ab FROM jp.t;")->is_success());

    auto cur = exec(d, "SELECT * FROM jp.v;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 4);
    CHECK(aliases(cur) == std::set<std::string>{"id", "ab"});
    CHECK(i64_set(cur, "ab") == std::set<int64_t>{10, 30, 50});
    CHECK(is_null(cur, "ab", 3));

    // Defect specific to a navigated alias (analog over plain columns is green). correct: {ab}, 4 rows.
    auto narrowed = exec(d, "SELECT ab FROM jp.v;");
    REQUIRE_FALSE(narrowed->is_success());
    CHECK(std::string(narrowed->get_error().what).find("'ab' was not found") != std::string::npos);
}

// Routes by position: the i-th projected column lands in the i-th written target; only an arity mismatch is refused.
TEST_CASE("integration::cpp::test_jsonb_support::insert_select_maps_projection_to_target_columns") {
    auto config = make_test_config(fixture_dir("insert_select"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto unaliased = exec(d, "INSERT INTO jp.t (id, a.b) SELECT 5, 55;");
    REQUIRE(unaliased->is_success());
    CHECK(unaliased->size() == 1);

    auto cur = exec(d, "SELECT * FROM jp.t ORDER BY id;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 5);
    CHECK(aliases(cur) == std::set<std::string>{"id", "a/b", "a/c", "x"});
    CHECK(i64(cur, "id", 4) == 5);
    CHECK(i64(cur, "a/b", 4) == 55);
    CHECK(is_null(cur, "a/c", 4));
    CHECK(is_null(cur, "x", 4));
    CHECK(i64_set(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t;"), "v") == std::set<int64_t>{10, 30, 50, 55});

    REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b) SELECT 6 AS c1, 66 AS c2;")->is_success());
    auto widened = exec(d, "SELECT * FROM jp.t;");
    REQUIRE(widened->is_success());
    CHECK(widened->size() == 6);
    CHECK(aliases(widened) == std::set<std::string>{"id", "a/b", "a/c", "x"});
    auto routed = exec(d, "SELECT id, \"a/b\" FROM jp.t WHERE id = 6;");
    REQUIRE(routed->is_success());
    REQUIRE(routed->size() == 1);
    CHECK(i64(routed, "a/b", 0) == 66);

    REQUIRE(exec(d, "CREATE TABLE jp.src ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.src (k, w) VALUES (100, 200);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.t (a.b, id) SELECT w, k FROM jp.src;")->is_success());
    auto from_src = exec(d, "SELECT id, \"a/b\" FROM jp.t WHERE id = 100;");
    REQUIRE(from_src->is_success());
    REQUIRE(from_src->size() == 1);
    CHECK(i64(from_src, "a/b", 0) == 200);
    CHECK(aliases(exec(d, "SELECT * FROM jp.t;")) == std::set<std::string>{"id", "a/b", "a/c", "x"});

    auto short_list = exec(d, "INSERT INTO jp.t (id) SELECT 7, 77;");
    REQUIRE_FALSE(short_list->is_success());
    CHECK(std::string(short_list->get_error().what).find("INSERT names 1 columns but the source provides 2") !=
          std::string::npos);
}

// A jsonb navigation lowers to a plain column read, so comparing two is an ordinary column comparison.
TEST_CASE("integration::cpp::test_jsonb_support::compare_two_navigations") {
    auto config = make_test_config(fixture_dir("nav_cmp"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b, a.c) VALUES (1, 10, 20), (2, 30, 30), (3, 99, 99);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.t (id, a.c) VALUES (4, 70);")->is_success());

    SECTION("returns exactly the equal-leaf rows; a NULL leaf never matches") {
        auto cur = exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = t #>> 'a.c';");
        REQUIRE(cur->is_success());
        CHECK(ids(cur) == std::set<int64_t>{2, 3});
    }

    SECTION("a navigation naming no column is still a clean error, not a wrong answer") {
        CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = t #>> 'nokey';")->is_success());
    }
}

TEST_CASE("integration::cpp::test_jsonb_support::clean_rejections") {
    auto config = make_test_config(fixture_dir("reject"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const char* rejected[] = {
        "CREATE INDEX ix ON jp.t (t #>> 'a.b');",
        "SELECT id FROM jp.t WHERE t #>> 'a.b' IN (10, 30);",
        "SELECT id FROM jp.t WHERE t ->> 'x' LIKE 'p%';",
        "SELECT id FROM jp.t WHERE t #- 'a.b' = 1;",
        "DELETE FROM jp.t WHERE id = 3 RETURNING t - 'x';",
        "SELECT t -> 'a' FROM jp.t UNION SELECT t -> 'nokey' FROM jp.t;",
        "WITH c AS (SELECT id, t #>> 'a.b' AS ab FROM jp.t) SELECT ab FROM c;",
        "SELECT t ->> $1 AS v FROM jp.t;",
        "SELECT t #>> ARRAY['a','b'] AS v FROM jp.t;",
        "SELECT t #>> 'a.b' AS v FROM jp.t GROUP BY id;",
        "SELECT t #>> '{\"a\",\"b\"}' AS v FROM jp.t;",
        "SELECT t #>> 'a' AS v FROM jp.t;",
        "SELECT id FROM jp.t WHERE t @> '{\"x\":1}';",
        "SELECT id FROM jp.t WHERE t <@ '{\"x\":1}';",
        "SELECT id FROM jp.t WHERE t @? '$.x';",
        "SELECT id FROM jp.t WHERE t @@ '$.x == 1';",
    };
    for (const auto* sql : rejected) {
        INFO(sql);
        auto cur = exec(d, sql);
        REQUIRE(cur);
        CHECK_FALSE(cur->is_success());
    }

    REQUIRE(exec(d, "UPDATE jp.t SET x = t ->> 'x' WHERE id = 1;")->is_success());
    auto after = exec(d, "SELECT id, x FROM jp.t WHERE id = 1;");
    REQUIRE(after->is_success());
    CHECK(str(after, "x", 0) == "p");

    // Upstream #634 folded four duplicated resolvers into one transform_expression. Pinned by
    // value, not is_success(): this file is excluded from origin/main's build, so these are the only guard.
    {
        INFO("ORDER BY over a navigated key sorts by the physical column it names");
        auto desc = exec(d, "SELECT id FROM jp.t ORDER BY t #>> 'a.b' DESC;");
        REQUIRE(desc->is_success());
        REQUIRE(desc->size() == 4);
        CHECK(i64(desc, "id", 0) == 4);
        CHECK(i64(desc, "id", 1) == 3);
        CHECK(i64(desc, "id", 2) == 2);
        CHECK(i64(desc, "id", 3) == 1);

        auto asc = exec(d, "SELECT id FROM jp.t ORDER BY t #>> 'a.b' ASC;");
        REQUIRE(asc->is_success());
        REQUIRE(asc->size() == 4);
        CHECK(i64(asc, "id", 0) == 1);
        CHECK(i64(asc, "id", 3) == 4);
    }
    {
        INFO("an aggregate over a navigated argument reduces the same values the column holds");
        auto sum = exec(d, "SELECT SUM(t #>> 'a.b') AS s FROM jp.t;");
        REQUIRE(sum->is_success());
        REQUIRE(sum->size() == 1);
        CHECK(i64(sum, "s", 0) == 90);
    }
    {
        INFO("RETURNING over a navigated key answers for the row it removed, and removes it");
        auto ret = exec(d, "DELETE FROM jp.t WHERE id = 3 RETURNING t ->> 'x';");
        REQUIRE(ret->is_success());
        REQUIRE(ret->size() == 1);
        auto gone = exec(d, "SELECT id FROM jp.t WHERE id = 3;");
        REQUIRE(gone->is_success());
        CHECK(gone->size() == 0);
    }

    CHECK_FALSE(exec(d, "SELECT id FROM jp.t ORDER BY t #>> 'nokey' DESC;")->is_success());
    CHECK_FALSE(exec(d, "SELECT SUM(t #>> 'nokey') FROM jp.t;")->is_success());
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t ORDER BY t -> 'a' DESC;")->is_success());
    CHECK_FALSE(exec(d, "SELECT SUM(t -> 'a') FROM jp.t;")->is_success());
    {
        // Refusal must come before the delete: an unlowerable RETURNING may not take the row.
        INFO("a RETURNING over an unknown path refuses without deleting");
        CHECK_FALSE(exec(d, "DELETE FROM jp.t WHERE id = 4 RETURNING t ->> 'nokey';")->is_success());
        auto still = exec(d, "SELECT id FROM jp.t WHERE id = 4;");
        REQUIRE(still->is_success());
        CHECK(still->size() == 1);
    }
}

// Must be refused before reaching physical execution — under GROUP BY (or a bare aggregate) it segfaults there.
TEST_CASE("integration::cpp::test_jsonb_support::table_valued_op_rejected_under_grouping") {
    auto config = make_test_config(fixture_dir("gb_reject"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.gb ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.gb (g, a.b, x) VALUES (1, 1, 9), (1, 2, 90);")->is_success());

    CHECK_FALSE(exec(d, "SELECT g, gb -> 'a' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb #> 'a' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb - 'x' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb #- 'a.b' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb -> 'a', COUNT(x) FROM jp.gb;")->is_success());

    auto ok = exec(d, "SELECT g, COUNT(x) AS n FROM jp.gb GROUP BY g;");
    REQUIRE(ok->is_success());
    CHECK(ok->size() == 1);
}

// Keeps the wiki's SQL-Standards matrix honest: every SQL:2016 JSON feature must fail, never half-work.
TEST_CASE("integration::cpp::test_jsonb_support::sql_json_standard_absent") {
    auto config = make_test_config(fixture_dir("std"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.r (id BIGINT, s TEXT);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.r (id, s) VALUES (1, '{\"a\":1}');")->is_success());

    SECTION("4.1 there is no JSON / JSONB column type") {
        auto j = exec(d, "CREATE TABLE jp.j1 (id BIGINT, j JSON);");
        REQUIRE_FALSE(j->is_success());
        CHECK(std::string(j->get_error().what).find("json") != std::string::npos);
        CHECK_FALSE(exec(d, "CREATE TABLE jp.j2 (id BIGINT, j JSONB);")->is_success());
    }

    SECTION("4.2/4.4 the SQL/JSON functions do not exist") {
        const char* fns[] = {"SELECT JSON_QUERY(s, '$.a') FROM jp.r;",
                             "SELECT JSON_VALUE(s, '$.a') FROM jp.r;",
                             "SELECT JSON_EXISTS(s, '$.a') FROM jp.r;",
                             "SELECT JSON_OBJECT('a', 1);",
                             "SELECT JSON_ARRAY(1, 2);",
                             "SELECT JSON_ARRAYAGG(id) FROM jp.r;",
                             "SELECT to_json(id) FROM jp.r;"};
        for (const auto* sql : fns) {
            INFO(sql);
            CHECK_FALSE(exec(d, sql)->is_success());
        }
    }

    SECTION("4.3/4.5 JSON_TABLE, IS JSON and MATCH_RECOGNIZE are syntax errors") {
        const char* syn[] = {"SELECT * FROM JSON_TABLE('{\"a\":1}', '$' COLUMNS (a INT PATH '$.a'));",
                             "SELECT s IS JSON FROM jp.r;",
                             "SELECT * FROM jp.r MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS id > 0);"};
        for (const auto* sql : syn) {
            INFO(sql);
            CHECK_FALSE(exec(d, sql)->is_success());
        }
    }
}

// BUG characterization: each CHECK below pins behavior that is currently wrong.

// Guards against a navigated value's cast folding into one per-run constant repeated on every
// row, instead of reading the column per row.
TEST_CASE("integration::cpp::test_jsonb_support::cast_nav_in_arithmetic_reads_the_column") {
    auto config = make_test_config(fixture_dir("cast_arith"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cast_only = exec(d, "SELECT id, (t #>> 'a.c')::bigint AS v FROM jp.t ORDER BY id;");
    REQUIRE_FALSE(cast_only->is_success());
    CHECK(std::string(cast_only->get_error().what).find("cast spelled on a column reference") != std::string::npos);

    auto arith_only = exec(d, "SELECT id, (t #>> 'a.c') + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(arith_only->is_success());
    CHECK(i64(arith_only, "v", 0) == 21);

    auto both = exec(d, "SELECT id, (t #>> 'a.c')::bigint + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(both->is_success());
    CHECK(i64(both, "v", 0) == 21);
    CHECK(i64(both, "v", 1) == 41);
    CHECK(i64(both, "v", 2) == 61);
    CHECK(i64(both, "v", 3) == 71);

    auto both2 = exec(d, "SELECT id, 100 - (t #>> 'a.b')::bigint AS v FROM jp.t WHERE id < 3 ORDER BY id;");
    REQUIRE(both2->is_success());
    CHECK(i64(both2, "v", 0) == 90);
    CHECK(i64(both2, "v", 1) == 70);

    auto plain = exec(d, "SELECT id, (id)::bigint + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(plain->is_success());
    CHECK(i64(plain, "v", 0) == 2);
}

// An INSERT target of any depth flattens to the full slash-joined name; either spelling round-trips.
TEST_CASE("integration::cpp::test_jsonb_support::deep_path_insert_keeps_all_segments") {
    auto config = make_test_config(fixture_dir("depth"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.d ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.d (id, a.b.c) VALUES (1, 111);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.d (id, p.q.r.s.t) VALUES (2, 222);")->is_success());

    auto cur = exec(d, "SELECT * FROM jp.d ORDER BY id;");
    REQUIRE(cur->is_success());
    CHECK(aliases(cur) == std::set<std::string>{"id", "a/b/c", "p/q/r/s/t"});
    CHECK(i64(cur, "a/b/c", 0) == 111);
    CHECK(i64(cur, "p/q/r/s/t", 1) == 222);

    CHECK(i64(exec(d, "SELECT d #>> 'a.b.c' AS v FROM jp.d WHERE id = 1;"), "v", 0) == 111);
    CHECK(i64(exec(d, "SELECT d #>> '{a,b,c}' AS v FROM jp.d WHERE id = 1;"), "v", 0) == 111);
    CHECK(i64(exec(d, "SELECT d #>> 'p.q.r.s.t' AS v FROM jp.d WHERE id = 2;"), "v", 0) == 222);
    CHECK_FALSE(exec(d, "SELECT d #>> 'a.c' AS v FROM jp.d;")->is_success());

    check_value_position_refusal(d, "SELECT d -> 'a' -> 'b' FROM jp.d WHERE id = 1;");
}

// A subscript like arr[0] flattens like any segment, to "arr/0".
TEST_CASE("integration::cpp::test_jsonb_support::subscript_insert_target") {
    auto config = make_test_config(fixture_dir("subscript"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.d ();")->is_success());

    REQUIRE(exec(d, "INSERT INTO jp.d (id, arr[0], arr[1]) VALUES (1, 7, 8);")->is_success());
    auto cur = exec(d, "SELECT * FROM jp.d ORDER BY id;");
    REQUIRE(cur->is_success());
    CHECK(aliases(cur) == std::set<std::string>{"id", "arr/0", "arr/1"});
    CHECK(i64(cur, "arr/0", 0) == 7);
    CHECK(i64(cur, "arr/1", 0) == 8);
    CHECK(i64(exec(d, "SELECT d #>> 'arr.0' AS v FROM jp.d;"), "v", 0) == 7);
}

// A NULL into a not-yet-existing column has no type, so it's dropped instead of segfaulting.
// OPEN DEFECT: the arity guard (validate_logical_plan.cpp, bind_computed_rename) checks the typed
// schema instead of the projection's expression count, wrongly refusing an all-NULL write.
TEST_CASE("integration::cpp::test_jsonb_support::insert_null_into_new_column_is_absent") {
    auto config = make_test_config(fixture_dir("null_insert"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());

    auto nulled = exec(d, "INSERT INTO jp.t (a.b, x) VALUES (NULL, 'z');");
    REQUIRE_FALSE(nulled->is_success());
    CHECK(std::string(nulled->get_error().what).find("INSERT names 2 columns but the source provides 1") !=
          std::string::npos);
    CHECK(exec(d, "SELECT * FROM jp.t;")->size() == 0);

    REQUIRE(exec(d, "CREATE TABLE jp.u ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.u (id, v) VALUES (1, NULL), (2, NULL), (3, 7);")->is_success());
    auto u = exec(d, "SELECT id, v FROM jp.u ORDER BY id;");
    REQUIRE(u->is_success());
    CHECK(i64(u, "id", 0) == 1);
    CHECK(is_null(u, "v", 0));
    CHECK(is_null(u, "v", 1));
    CHECK(i64(u, "v", 2) == 7);

    REQUIRE(exec(d, "CREATE TABLE jp.w ();")->is_success());
    auto one = exec(d, "INSERT INTO jp.w (a.b) VALUES (NULL);");
    REQUIRE_FALSE(one->is_success());
    CHECK(std::string(one->get_error().what).find("INSERT names 1 columns but the source provides 0") !=
          std::string::npos);
    auto two = exec(d, "INSERT INTO jp.w (a.b, x) VALUES (NULL, NULL);");
    REQUIRE_FALSE(two->is_success());
    CHECK(std::string(two->get_error().what).find("INSERT names 2 columns but the source provides 0") !=
          std::string::npos);
    CHECK(exec(d, "SELECT * FROM jp.w;")->size() == 0);
}

// `- '{a,b}'` (`jsonb - text[]`) removes several keys; `- 'key'` removes one; `#- 'a.b'` deletes a path.
TEST_CASE("integration::cpp::test_jsonb_support::delete_key_array_form") {
    auto config = make_test_config(fixture_dir("del_arr"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b, a.c, x, y) VALUES (1, 10, 20, 'p', 'q');")->is_success());

    check_value_position_refusal(d, "SELECT t - 'x' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t - '{x}' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t - '{x,y}' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t - '{x,a}' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t - '{}' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t - '{nokey}' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t #- '{a,b}' FROM jp.t;");
    check_value_position_refusal(d, "SELECT t - '{a}' FROM jp.t;");
}

// A miss must error like the scalar form does; silently erasing it would change row arity unnoticed.
TEST_CASE("integration::cpp::test_jsonb_support::zero_match_expand_is_an_error") {
    auto config = make_test_config(fixture_dir("zero_exp"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK_FALSE(exec(d, "SELECT t ->> 'nokey' AS v FROM jp.t;")->is_success());
    CHECK_FALSE(exec(d, "SELECT t -> 'nokey' FROM jp.t;")->is_success());
    CHECK_FALSE(exec(d, "SELECT t -> 'nokey', x FROM jp.t;")->is_success());
    CHECK_FALSE(exec(d, "SELECT id, t -> 'nokey', x FROM jp.t;")->is_success());

    CHECK_FALSE(exec(d, "SELECT t -> 'a.b' FROM jp.t;")->is_success());

    // correct: a matching key expands to {b, c}.
    check_value_position_refusal(d, "SELECT t -> 'a' FROM jp.t;");
}

// LIMITATION: '/' is a legal identifier char, so nested path a.b and a column literally named
// "a/b" collide; fixing this needs percent-escaping every identifier system-wide, so it's not done.
TEST_CASE("integration::cpp::test_jsonb_support::flattened_name_and_nested_path_share_storage") {
    auto config = make_test_config(fixture_dir("sep"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.s ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.s (id, a.b) VALUES (1, 10);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.s (id, \"a/b\") VALUES (2, 20);")->is_success());

    auto star = exec(d, "SELECT * FROM jp.s ORDER BY id;");
    REQUIRE(star->is_success());
    CHECK(aliases(star) == std::set<std::string>{"id", "a/b"});

    CHECK(i64_set(exec(d, "SELECT s #>> 'a.b' AS v FROM jp.s;"), "v") == std::set<int64_t>{10, 20});
    CHECK(i64_set(exec(d, "SELECT s ->> 'a/b' AS v FROM jp.s;"), "v") == std::set<int64_t>{10, 20});
}

// Existence follows postgres 3VL instead of hard-erroring: absent is false, an intermediate key
// is present iff a child is, and one absent key does not poison an any-of.
TEST_CASE("integration::cpp::test_jsonb_support::existence_over_missing_key") {
    auto config = make_test_config(fixture_dir("missing"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x';")) == std::set<int64_t>{1, 2});
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ? 'nokey';")->size() == 0);
    CHECK(exec(d, "SELECT id FROM jp.t WHERE NOT (t ? 'nokey');")->size() == 4);

    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?| '{x,nokey}';")) == std::set<int64_t>{1, 2});
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ?& '{x,nokey}';")->size() == 0);
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ?| '{nokey,nokey2}';")->size() == 0);

    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'a';")) == std::set<int64_t>{1, 2, 3, 4});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?& '{a,x}';")) == std::set<int64_t>{1, 2});

    REQUIRE(exec(d, "CREATE TABLE jp.r (id BIGINT, v BIGINT);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.r (id, v) VALUES (1, 5);")->is_success());
    CHECK_FALSE(exec(d, "SELECT id FROM jp.r WHERE nosuchcol IS NULL;")->is_success());
}

// Deferred: scalar navigation over an absent key should yield SQL NULL (and `<nav> IS NULL` a
// boolean), which needs the select-list/compare paths to synthesize a typed NULL leaf.
TEST_CASE("integration::cpp::test_jsonb_support::navigation_over_missing_key_still_errors") {
    auto config = make_test_config(fixture_dir("missing_nav"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK_FALSE(exec(d, "SELECT t ->> 'nokey' AS v FROM jp.t;")->is_success());           // deferred: NULL every row
    CHECK_FALSE(exec(d, "SELECT t #>> 'no.key' AS v FROM jp.t;")->is_success());          // deferred: NULL every row
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' IS NULL;")->is_success()); // deferred: row 4
}

// The key operand is a literal (string/number, a cast, or NULL): a cast is transparent, a
// numeric key is stringified, and NULL is a clean error (get_str_value).
TEST_CASE("integration::cpp::test_jsonb_support::key_operand_literals") {
    auto config = make_test_config(fixture_dir("key_operand"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("a cast on a scalar key names the same column as the bare key") {
        auto cast = exec(d, "SELECT id, t ->> ('x'::text) AS v FROM jp.t ORDER BY id;");
        REQUIRE(cast->is_success());
        CHECK(str(cast, "v", 0) == "p");
        CHECK(str(cast, "v", 1) == "q");
        CHECK(is_null(cast, "v", 2));
        auto plain = exec(d, "SELECT id, t ->> 'x' AS v FROM jp.t ORDER BY id;");
        REQUIRE(plain->is_success());
        CHECK(str(plain, "v", 0) == "p");
    }

    SECTION("a cast on a path key splits into segments just like the bare path") {
        auto cast = exec(d, "SELECT id, t #>> ('a.b'::text) AS v FROM jp.t ORDER BY id;");
        REQUIRE(cast->is_success());
        CHECK(i64(cast, "v", 0) == 10);
        CHECK(i64(cast, "v", 1) == 30);
        CHECK_FALSE(exec(d, "SELECT t ->> ('a.b'::text) AS v FROM jp.t;")->is_success());
    }

    SECTION("a cast key composes in every clause the bare key does") {
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? ('x'::text);")) == std::set<int64_t>{1, 2});
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> ('a.b'::text) = 10;")) == std::set<int64_t>{1});
    }

    SECTION("a NULL key is a clean error, not a crash") {
        auto cur = exec(d, "SELECT t ->> NULL FROM jp.t;");
        REQUIRE_FALSE(cur->is_success());
        CHECK(std::string(cur->get_error().what).find("NULL") != std::string::npos);
        CHECK_FALSE(exec(d, "SELECT t #>> NULL FROM jp.t;")->is_success());
        CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t ? NULL;")->is_success());
    }

    SECTION("a non-string cast key resolves to its value and never crashes") {
        // (1::bool) resolves to the text "1" (no such column), erroring cleanly rather than crashing.
        CHECK_FALSE(exec(d, "SELECT t ->> (1::bool) AS v FROM jp.t;")->is_success());
        CHECK_FALSE(exec(d, "SELECT t -> (1::bool) FROM jp.t;")->is_success());
    }
}

// Regression #622: a bare cast over a navigated value is refused entirely. correct: it converts
// (::text yields the text "20").
TEST_CASE("integration::cpp::test_jsonb_support::bug_cast_over_navigation_is_a_noop") {
    auto config = make_test_config(fixture_dir("cast_noop"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT (t #>> 'a.c')::text AS v FROM jp.t ORDER BY id;");
    REQUIRE_FALSE(cur->is_success());
    CHECK(std::string(cur->get_error().what).find("cast spelled on a column reference") != std::string::npos);
}

TEST_CASE("integration::cpp::test_jsonb_support::expand_inside_join_is_side_aware") {
    auto config = make_test_config(fixture_dir("join_expand"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, lv, d.e, d.f) VALUES (1, 100, 111, 112), (2, 200, 222, 223);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, mv, d.e, d.f) VALUES (1, 10, 11, 12), (2, 20, 22, 23);")->is_success());

    CHECK(i64_set(exec(d, "SELECT m #>> 'd.e' AS v FROM jp.l JOIN jp.m ON l.k = m.k;"), "v") ==
          std::set<int64_t>{11, 22});
    CHECK(i64_set(exec(d, "SELECT l #>> 'd.e' AS v FROM jp.l JOIN jp.m ON l.k = m.k;"), "v") ==
          std::set<int64_t>{111, 222});

    check_value_position_refusal(d, "SELECT m -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
    check_value_position_refusal(d, "SELECT l -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
    check_value_position_refusal(d, "SELECT m - 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
}

// A resolution bug, not a missing feature.
TEST_CASE("integration::cpp::test_jsonb_support::expand_in_join_with_unique_subtree") {
    auto config = make_test_config(fixture_dir("join_expand_ok"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, lv) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, d.e, d.f) VALUES (1, 11, 12), (2, 22, 23);")->is_success());

    check_value_position_refusal(d, "SELECT m -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
}

// Triggered by 3+ joins sharing a column name — a join-resolution bug, not a jsonb one.
TEST_CASE("integration::cpp::test_jsonb_support::bug_three_table_join_takes_leftmost_value") {
    auto config = make_test_config(fixture_dir("join3"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.n ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, v) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, v) VALUES (1, 10), (2, 20);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.n (k, v) VALUES (1, 1000), (2, 2000);")->is_success());

    auto cur = exec(d, "SELECT m #>> 'v' AS v FROM jp.l JOIN jp.m ON l.k = m.k JOIN jp.n ON m.k = n.k;");
    REQUIRE(cur->is_success());
    // correct: {10, 20} (the values of m). We get l's values instead.
    CHECK(i64_set(cur, "v") == std::set<int64_t>{100, 200});
}


// JSONB support suite.
//
// otterbrix has NO json/jsonb data type, no binary json storage, no SQL/JSON
// path engine and no SQL/JSON functions (see the sql_json_standard_absent case
// at the bottom, which pins that). What it *does* have — added by PR #499 with
// zero grammar changes, purely in the transformer — is nine postgres-spelled
// jsonb operators that are name-mangling over *flattened columns*:
//
//   CREATE TABLE t ();                                 -- "computing" table, relkind 'g'
//   INSERT INTO t (id, a.b, a.c, x) VALUES (1,10,20,'p');
//   -- storage is four plain columns: id, "a/b", "a/c", x
//   SELECT t #>> 'a.b' FROM t;                         -- compiles to a get_field projection on "a/b"
//
//   scalar  (one column) : ->>  #>>          .. terminate a chain, return the leaf value
//   table   (n columns)  : ->   #>           .. expand an object into its child columns
//   existence (predicate): ?    ?|    ?&     .. per-row "key present and not null"
//   delete  (projection) : -    #-           .. project every column except the named subtree
//
// Paths are spelled either dotted ('a.b') or as a postgres text array ('{a,b}').
// Keys are case-sensitive; a path that no column matches is a hard ERROR, not NULL.
//
// The cases below are split in two:
//   * SUPPORTED  — value-exact pins of behavior that is correct and worth keeping.
//   * BUG        — characterization pins of behavior that is WRONG today. Each one
//                  asserts what the engine currently does and states what it should
//                  do in a "correct:" comment, so a fix flips a visible assertion
//                  instead of silently changing an untested result.
//
// NOT PINNABLE — these SEGFAULT the process, so they cannot live in a test binary.
// Verified on this commit; kept here as the repro list:
//   INSERT INTO t (a.b, x) VALUES (NULL, 'z');          -- NULL literal in a dotted target [C3]
//   SELECT CASE WHEN t #>> 'a.b' = 10 THEN 1 ELSE 0 END -- only when the leaf has a NULL row;
//     FROM t;                                           --   reproduces on plain columns too (general 3VL bug)
// And this one escapes as an uncaught C++ exception rather than a cursor error:
//   INSERT INTO t (id, arr[0]) VALUES (1, 1);           -- "basic_string: construction from null is not valid" [C4]
//
// FIXED (were crashes/wrong results, now pinned as correct below):
//   [A] get_str_value hardening — a cast key is transparent, a NULL key is a clean
//       error, and neither `t -> (1::bool)` nor `t ->> NULL` crashes any more.

#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <set>
#include <string>

using namespace test_helpers;
using components::cursor::cursor_t_ptr;

namespace {

    // The shared fixture used by most cases. Four rows over a computing table,
    // deliberately ragged so that per-row-absent keys are covered:
    //
    //   id | a/b  | a/c | x
    //    1 |   10 |  20 | 'p'
    //    2 |   30 |  40 | 'q'
    //    3 |   50 |  60 | NULL   <- x absent for this row
    //    4 | NULL |  70 | NULL   <- a.b absent for this row
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
        REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());
        REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b, a.c, x) VALUES (1, 10, 20, 'p'), (2, 30, 40, 'q');")
                    ->is_success());
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

    // Every non-null integer value of one column, as a set. Used where ORDER BY
    // cannot be applied — otterbrix does not accept an output alias as a sort key.
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

    // ids of every returned row, for set-comparison of predicates.
    std::set<int64_t> ids(const cursor_t_ptr& cur) { return i64_set(cur, "id"); }

} // namespace

// ===========================================================================
// SUPPORTED
// ===========================================================================

// The flattened storage model itself: dotted INSERT targets become real columns
// named with '/' separators, and rows that omit a target leave it NULL.
TEST_CASE("integration::cpp::test_jsonb_support::flattened_storage_model") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/storage");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT * FROM jp.t ORDER BY id;");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 4);
    // The dotted targets a.b / a.c are stored as two ordinary columns "a/b", "a/c" —
    // there is no nested value anywhere.
    CHECK(aliases(cur) == std::set<std::string>{"id", "a/b", "a/c", "x"});
    CHECK(i64(cur, "a/b", 0) == 10);
    CHECK(i64(cur, "a/c", 0) == 20);
    CHECK(str(cur, "x", 0) == "p");
    CHECK(is_null(cur, "x", 2));    // row 3 never supplied x
    CHECK(is_null(cur, "a/b", 3));  // row 4 never supplied a.b
    CHECK(i64(cur, "a/c", 3) == 70);
}

// ->> and #>> : scalar extraction. Every spelling of the same path must agree.
TEST_CASE("integration::cpp::test_jsonb_support::extract_scalar") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/extract");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("top-level key") {
        auto cur = exec(d, "SELECT id, t ->> 'x' AS v FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        CHECK(str(cur, "v", 0) == "p");
        CHECK(str(cur, "v", 1) == "q");
        // A key the row never supplied reads back as NULL (only a key that matches
        // no column *at all* is an error — see bug_missing_key_errors_instead_of_null).
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
        // In PostgreSQL ->> is text-returning; here an integer leaf stays BIGINT.
        CHECK(type_of(cur, "n") == components::types::logical_type::BIGINT);
    }

    SECTION("navigation works through a table alias, and via the base name too") {
        CHECK(i64(exec(d, "SELECT tt #>> 'a.b' AS v FROM jp.t AS tt ORDER BY id;"), "v", 0) == 10);
        CHECK(i64(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t AS tt ORDER BY id;"), "v", 0) == 10);
    }

    SECTION("keys are case-sensitive") {
        CHECK_FALSE(exec(d, "SELECT t ->> 'X' AS v FROM jp.t;")->is_success());
    }
}

// -> and #> : object expansion. These are table-valued — they widen the projection
// to one column per child, which is why they cannot terminate a scalar chain.
TEST_CASE("integration::cpp::test_jsonb_support::expand_object") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/expand");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("-> expands to the child columns, named by their leaf segment") {
        auto cur = exec(d, "SELECT t -> 'a' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 4);
        CHECK(aliases(cur) == std::set<std::string>{"b", "c"});
        CHECK(i64(cur, "b", 0) == 10);
        CHECK(i64(cur, "c", 0) == 20);
        CHECK(is_null(cur, "b", 3));
        CHECK(i64(cur, "c", 3) == 70);
    }

    SECTION("#> with a path array expands the same way") {
        auto cur = exec(d, "SELECT t #> '{a}' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(aliases(cur) == std::set<std::string>{"b", "c"});
        CHECK(i64(cur, "b", 1) == 30);
    }

    SECTION("expansion composes with ordinary columns and with a further ->") {
        auto cur = exec(d, "SELECT id, t -> 'a' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(aliases(cur) == std::set<std::string>{"id", "b", "c"});

        auto chained = exec(d, "SELECT t -> 'a' -> 'b' FROM jp.t ORDER BY id;");
        REQUIRE(chained->is_success());
        CHECK(aliases(chained) == std::set<std::string>{"b"});
        CHECK(i64(chained, "b", 0) == 10);
    }

    SECTION("-> on a leaf yields that single column") {
        auto cur = exec(d, "SELECT t -> 'x' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(aliases(cur) == std::set<std::string>{"x"});
        CHECK(str(cur, "x", 0) == "p");
    }

    SECTION("a table-valued operator cannot be used as a scalar — and says so") {
        auto cur = exec(d, "SELECT id FROM jp.t WHERE t #> '{a}' = 1;");
        REQUIRE_FALSE(cur->is_success());
        CHECK(std::string(cur->get_error().what).find("cannot be used as a scalar") != std::string::npos);
    }
}

// - and #- : project everything EXCEPT the named key/subtree.
TEST_CASE("integration::cpp::test_jsonb_support::delete_keys") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/delete");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("- drops a leaf key") {
        auto cur = exec(d, "SELECT t - 'x' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(aliases(cur) == std::set<std::string>{"id", "a/b", "a/c"});
        CHECK(i64(cur, "a/b", 0) == 10);
    }

    SECTION("- drops a whole subtree when given the prefix") {
        auto cur = exec(d, "SELECT t - 'a' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(aliases(cur) == std::set<std::string>{"id", "x"});
    }

    SECTION("#- drops one leaf of a subtree, dotted or as a path array") {
        auto dotted = exec(d, "SELECT t #- 'a.b' FROM jp.t ORDER BY id;");
        REQUIRE(dotted->is_success());
        CHECK(aliases(dotted) == std::set<std::string>{"id", "a/c", "x"});
        CHECK(i64(dotted, "a/c", 0) == 20);

        auto arr = exec(d, "SELECT t #- '{a}' FROM jp.t ORDER BY id;");
        REQUIRE(arr->is_success());
        CHECK(aliases(arr) == std::set<std::string>{"id", "x"});
    }

    SECTION("deleting a key that does not exist is a no-op, as in postgres") {
        auto cur = exec(d, "SELECT t - 'nokey' FROM jp.t ORDER BY id;");
        REQUIRE(cur->is_success());
        CHECK(aliases(cur) == std::set<std::string>{"id", "a/b", "a/c", "x"});
    }
}

// ? / ?| / ?& : per-row existence. "Exists" means the column exists in the schema
// AND this row's value is not null — so a ragged row tests false, as in postgres.
TEST_CASE("integration::cpp::test_jsonb_support::existence_predicates") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/exists");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // rows 3 and 4 never supplied x.
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x';")) == std::set<int64_t>{1, 2});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE NOT (t ? 'x');")) == std::set<int64_t>{3, 4});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x' AND t ? 'id';")) == std::set<int64_t>{1, 2});

    // ?| any-of, ?& all-of.
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?| '{x,id}';")) == std::set<int64_t>{1, 2, 3, 4});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?& '{x,id}';")) == std::set<int64_t>{1, 2});

    // Degenerate key lists: an empty any-of is false for every row, an empty
    // all-of is vacuously true for every row.
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ?| '{}';")->size() == 0);
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?& '{}';")) == std::set<int64_t>{1, 2, 3, 4});

    // Existence composes with COUNT and with a subquery.
    auto cnt = exec(d, "SELECT COUNT(*) AS n FROM jp.t WHERE t ? 'x';");
    REQUIRE(cnt->is_success());
    CHECK(cnt->chunks().front().get_value<uint64_t>(0, 0) == 2);
}

// Navigation as a predicate, and as the WHERE of a DML statement.
TEST_CASE("integration::cpp::test_jsonb_support::predicates_and_dml") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/dml");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("comparison, BETWEEN, boolean composition") {
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = 10;")) == std::set<int64_t>{1});
        // an integer leaf also compares against a string literal (values are coerced)
        CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' = '10';")) == std::set<int64_t>{1});
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

// Navigated values inside expressions. The parenthesised form is the supported one.
TEST_CASE("integration::cpp::test_jsonb_support::navigation_in_expressions") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/expr");
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
        // SUM(t #>> 'a.c') alone fails with "unable to parse value"; + 0 makes it an
        // ordinary arithmetic expression, which the aggregate accepts.
        auto s = exec(d, "SELECT SUM((t #>> 'a.c') + 0) AS s FROM jp.t;");
        REQUIRE(s->is_success());
        CHECK(i64(s, "s", 0) == 190); // 20 + 40 + 60 + 70

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
        CHECK(exec(d, "SELECT DISTINCT t ->> 'x' AS v FROM jp.t;")->size() == 3); // 'p', 'q', NULL
        CHECK(exec(d, "SELECT t #>> 'a.c' AS v FROM jp.t LIMIT 2;")->size() == 2);
    }
}

// The operators are not gated on relkind: they work over an ordinary fixed-schema
// table too, where they degrade to plain column access plus an is-not-null test.
TEST_CASE("integration::cpp::test_jsonb_support::regular_table") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/regular");
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

    auto exp = exec(d, "SELECT r -> 'v' FROM jp.r ORDER BY id;");
    REQUIRE(exp->is_success());
    CHECK(aliases(exp) == std::set<std::string>{"v"});

    CHECK(ids(exec(d, "SELECT id FROM jp.r WHERE r ->> 'v' = 10;")) == std::set<int64_t>{1});
    // ? is a not-null test on the column: the row that never got a value is excluded.
    CHECK(ids(exec(d, "SELECT id FROM jp.r WHERE r ? 'v';")) == std::set<int64_t>{1, 2});
}

// Navigation on either side of a two-table join.
TEST_CASE("integration::cpp::test_jsonb_support::two_table_join") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/join");
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

// Flattened columns survive a WAL/disk round-trip, and navigation still resolves
// against the restored catalog.
TEST_CASE("integration::cpp::test_jsonb_support::persistence") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/persist", true, true);
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        seed(d);
        CHECK(i64(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t ORDER BY id;"), "v", 0) == 10);
    }
    {
        // reopen the same directory: no clear, disk+wal on
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
        CHECK(aliases(exec(d, "SELECT t -> 'a' FROM jp.t;")) == std::set<std::string>{"b", "c"});
    }
}

// A view can capture a navigation, which is the one way to hand a navigated value
// to an outer query — the CTE / derived-table route loses the alias (see
// clean_rejections). INSERT ... SELECT into dotted targets works as well.
TEST_CASE("integration::cpp::test_jsonb_support::view_over_navigation") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/view");
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

    // NOTE: a narrowed projection over the view is ignored — "SELECT ab FROM jp.v"
    // still returns both columns. That is a view bug, not a jsonb one (it happens
    // for views over plain tables too), so it is only recorded, not pinned here.
}

// INSERT ... SELECT reports "1 row inserted" and then discards every value: on a
// computing table it appends a row in which even the plain columns are NULL.
// (On a fixed-schema table the same statement inserts nothing at all, so the
// broken piece is INSERT ... SELECT itself; the flattened-storage variant is
// pinned here because it is the one that leaves corrupt rows behind.)
TEST_CASE("integration::cpp::test_jsonb_support::bug_insert_select_appends_a_null_row") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/insert_select");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto ins = exec(d, "INSERT INTO jp.t (id, a.b) SELECT 5, 55;");
    REQUIRE(ins->is_success());
    CHECK(ins->size() == 1); // claims one row

    auto cur = exec(d, "SELECT * FROM jp.t ORDER BY id;");
    REQUIRE(cur->is_success());
    // correct: 4 rows unchanged plus (id=5, a/b=55). We get a 5th row of all NULLs.
    CHECK(cur->size() == 5);
    CHECK(is_null(cur, "id", 4));
    CHECK(is_null(cur, "a/b", 4));
    CHECK(i64_set(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t;"), "v") == std::set<int64_t>{10, 30, 50});
}

// Everything that is NOT supported must fail loudly. This case exists so that a
// future change cannot quietly start returning a wrong answer for any of them.
TEST_CASE("integration::cpp::test_jsonb_support::clean_rejections") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/reject");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const char* rejected[] = {
        // navigation is not accepted as a sort / group key
        "SELECT id FROM jp.t ORDER BY t #>> 'a.b' DESC;",
        "SELECT t #>> 'a.b' AS k, COUNT(*) FROM jp.t GROUP BY t #>> 'a.b';",
        // nor as an UPDATE SET source, nor as an index expression
        "UPDATE jp.t SET x = t ->> 'x' WHERE id = 1;",
        "CREATE INDEX ix ON jp.t (t #>> 'a.b');",
        // nor on the left of IN / LIKE, nor against another navigation
        "SELECT id FROM jp.t WHERE t #>> 'a.b' IN (10, 30);",
        "SELECT id FROM jp.t WHERE t ->> 'x' LIKE 'p%';",
        "SELECT id FROM jp.t WHERE t #>> 'a.b' = t #>> 'a.c';",
        // functions cannot take a navigated argument
        "SELECT upper(t ->> 'x') AS v FROM jp.t;",
        "SELECT SUM(t #>> 'a.b') FROM jp.t;",
        // the delete operators are SELECT-list only
        "SELECT id FROM jp.t WHERE t #- 'a.b' = 1;",
        // no navigation or deletion in a RETURNING clause
        "DELETE FROM jp.t WHERE id = 3 RETURNING t ->> 'x';",
        "DELETE FROM jp.t WHERE id = 3 RETURNING t - 'x';",
        // UNION over expansions is only as valid as the two arities happen to be
        "SELECT t -> 'a' FROM jp.t UNION SELECT t -> 'nokey' FROM jp.t;",
        // a navigated alias is not visible to an enclosing CTE (a VIEW does work —
        // see view_over_navigation)
        "WITH c AS (SELECT id, t #>> 'a.b' AS ab FROM jp.t) SELECT ab FROM c;",
        // a parameter cannot supply the key: $1 is taken as the literal path "$1"
        "SELECT t ->> $1 AS v FROM jp.t;",
        // ARRAY[...] is not accepted as a path (only 'a.b' and '{a,b}' are)
        "SELECT t #>> ARRAY['a','b'] AS v FROM jp.t;",
        // navigation in the SELECT list of a GROUP BY query
        "SELECT t #>> 'a.b' AS v FROM jp.t GROUP BY id;",
        // path segments are not dequoted
        "SELECT t #>> '{\"a\",\"b\"}' AS v FROM jp.t;",
        // an intermediate (non-leaf) key is not a scalar
        "SELECT t #>> 'a' AS v FROM jp.t;",
        // postgres jsonpath / containment / concatenation operators do not exist
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
}

// The wiki's SQL-Standards matrix marks every SQL:2016 JSON row ❌ for otterbrix.
// That is accurate, and this case keeps it honest: none of it may start silently
// half-working. (The failure MODE differs per feature — type rejection, unknown
// function, or plain syntax error — but none of them execute.)
TEST_CASE("integration::cpp::test_jsonb_support::sql_json_standard_absent") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/std");
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

// ===========================================================================
// BUG characterization — each CHECK below pins behavior that is WRONG.
// ===========================================================================

// A cast applied to a navigated value, then used in arithmetic, reads
// uninitialized memory: the result is a per-run-varying constant repeated on
// every row. Without the cast, or without the arithmetic, the same expression is
// correct — so the two together corrupt the value.
TEST_CASE("integration::cpp::test_jsonb_support::bug_cast_nav_in_arithmetic_is_garbage") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/cast_arith");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // both halves are individually correct
    auto cast_only = exec(d, "SELECT id, (t #>> 'a.c')::bigint AS v FROM jp.t ORDER BY id;");
    REQUIRE(cast_only->is_success());
    CHECK(i64(cast_only, "v", 0) == 20);

    auto arith_only = exec(d, "SELECT id, (t #>> 'a.c') + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(arith_only->is_success());
    CHECK(i64(arith_only, "v", 0) == 21);

    // together: garbage. Same value on every row, unrelated to the data.
    auto both = exec(d, "SELECT id, (t #>> 'a.c')::bigint + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(both->is_success());
    // correct: 21, 41, 61, 71
    CHECK_FALSE(i64(both, "v", 0) == 21);
    CHECK(i64(both, "v", 0) == i64(both, "v", 1)); // every row gets the same garbage
    CHECK(i64(both, "v", 1) == i64(both, "v", 2));

    // the same shape over a plain column is fine — this is navigation-specific
    auto plain = exec(d, "SELECT id, (id)::bigint + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(plain->is_success());
    CHECK(i64(plain, "v", 0) == 2);
}

// An INSERT target three or more segments deep silently loses its middle
// segments: only the first and last survive. The value is stored — under a path
// nobody asked for — and the path that was written is then unreadable.
TEST_CASE("integration::cpp::test_jsonb_support::bug_deep_path_insert_drops_middle_segments") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/depth");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.d ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.d (id, a.b.c) VALUES (1, 111);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.d (id, p.q.r.s) VALUES (2, 222);")->is_success());

    auto cur = exec(d, "SELECT * FROM jp.d ORDER BY id;");
    REQUIRE(cur->is_success());
    // correct: {"id", "a/b/c", "p/q/r/s"}
    CHECK(aliases(cur) == std::set<std::string>{"id", "a/c", "p/s"});
    CHECK(i64(cur, "a/c", 0) == 111); // the value landed under the truncated path
    CHECK(i64(cur, "p/s", 1) == 222); // two segments dropped here

    // consequence: the path that was inserted cannot be read back
    CHECK_FALSE(exec(d, "SELECT d #>> 'a.b.c' AS v FROM jp.d;")->is_success());   // correct: 111
    CHECK_FALSE(exec(d, "SELECT d #>> '{a,b,c}' AS v FROM jp.d;")->is_success()); // correct: 111
    CHECK(exec(d, "SELECT d #>> 'a.c' AS v FROM jp.d;")->is_success());           // the path nobody wrote
}

// `- '{key}'` (the postgres text-array form of key deletion) is silently ignored:
// no column is removed and no error is raised. Only the bare `- 'key'` form works.
TEST_CASE("integration::cpp::test_jsonb_support::bug_array_form_of_delete_is_silent_noop") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/del_arr");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto ok = exec(d, "SELECT t - 'x' FROM jp.t;");
    REQUIRE(ok->is_success());
    CHECK(aliases(ok) == std::set<std::string>{"id", "a/b", "a/c"});

    auto arr = exec(d, "SELECT t - '{x}' FROM jp.t;");
    REQUIRE(arr->is_success());
    // correct: {"id", "a/b", "a/c"} — x should have been deleted, or the form rejected.
    CHECK(aliases(arr) == std::set<std::string>{"id", "a/b", "a/c", "x"});
}

// Expanding a path that matches no column does not error — the expansion item is
// silently ERASED from the select list. On its own that yields a zero-column
// result; alongside other items it silently changes the arity of the answer.
TEST_CASE("integration::cpp::test_jsonb_support::bug_zero_match_expand_drops_the_select_item") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/zero_exp");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // the scalar operator errors, as it should
    CHECK_FALSE(exec(d, "SELECT t ->> 'nokey' AS v FROM jp.t;")->is_success());

    // the table-valued one silently produces nothing
    auto cur = exec(d, "SELECT t -> 'nokey' FROM jp.t;");
    REQUIRE(cur->is_success()); // correct: this should be an error
    CHECK(cur->column_count() == 0);

    // ... and next to other select items it just disappears, so the caller gets
    // fewer columns than it asked for, with no indication that anything was wrong.
    auto mixed = exec(d, "SELECT t -> 'nokey', x FROM jp.t;");
    REQUIRE(mixed->is_success());
    CHECK(mixed->column_count() == 1); // correct: an error, or 2 columns
    CHECK(aliases(mixed) == std::set<std::string>{"x"});

    auto mixed3 = exec(d, "SELECT id, t -> 'nokey', x FROM jp.t;");
    REQUIRE(mixed3->is_success());
    CHECK(mixed3->column_count() == 2); // correct: an error, or 3 columns
    CHECK(aliases(mixed3) == std::set<std::string>{"id", "x"});

    // same for a dotted path handed to -> (which takes a single key, not a path)
    auto dotted = exec(d, "SELECT t -> 'a.b' FROM jp.t;");
    REQUIRE(dotted->is_success()); // correct: error, or expand "a/b"
    CHECK(dotted->column_count() == 0);
}

// The '/' that joins path segments internally is not reserved, so a top-level key
// spelled with a slash and a nested path are the SAME column: "a/b" and a.b
// collide. Either spelling reads back both rows.
TEST_CASE("integration::cpp::test_jsonb_support::bug_path_separator_collides_with_a_literal_key") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/sep");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.s ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.s (id, a.b) VALUES (1, 10);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.s (id, \"a/b\") VALUES (2, 20);")->is_success());

    auto star = exec(d, "SELECT * FROM jp.s ORDER BY id;");
    REQUIRE(star->is_success());
    // correct: two distinct columns — the nested path a.b, and the flat key "a/b"
    CHECK(aliases(star) == std::set<std::string>{"id", "a/b"});

    // both spellings address that one column, so each sees the other's row
    CHECK(i64_set(exec(d, "SELECT s #>> 'a.b' AS v FROM jp.s;"), "v") == std::set<int64_t>{10, 20});
    CHECK(i64_set(exec(d, "SELECT s ->> 'a/b' AS v FROM jp.s;"), "v") == std::set<int64_t>{10, 20});
}

// In postgres a missing key yields NULL (for ->>) or false (for ?). Here it is a
// hard error, which makes the existence operators unusable for their one purpose:
// you cannot ask "does this row have key K" unless K is already known to exist.
TEST_CASE("integration::cpp::test_jsonb_support::bug_missing_key_errors_instead_of_null_or_false") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/missing");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK_FALSE(exec(d, "SELECT t ->> 'nokey' AS v FROM jp.t;")->is_success()); // correct: NULL for every row
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t ? 'nokey';")->is_success()); // correct: false -> no rows
    // one absent key poisons an any-of that another key already satisfies
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t ?| '{x,nokey}';")->is_success()); // correct: rows 1,2
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t ?& '{x,nokey}';")->is_success()); // correct: no rows

    // ? cannot see an intermediate key either, even though it is a real prefix
    // of two stored columns ("a/b", "a/c") and postgres would report it present.
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t ? 'a';")->is_success()); // correct: all rows

    // and IS NULL cannot be used to bridge the gap
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' IS NULL;")->is_success()); // correct: row 4
}

// [A] The key operand of a jsonb operator is a literal: a bare string/number, a
// cast of one, or NULL. A cast is transparent (it names the same key as the bare
// literal), a numeric key is stringified, a NULL key is a clean error, and none
// of these crash the process — the three things get_str_value used to get wrong.
TEST_CASE("integration::cpp::test_jsonb_support::key_operand_literals") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/key_operand");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    SECTION("a cast on a scalar key names the same column as the bare key") {
        // ->> takes a single key: ('x'::text) is exactly 'x'.
        auto cast = exec(d, "SELECT id, t ->> ('x'::text) AS v FROM jp.t ORDER BY id;");
        REQUIRE(cast->is_success());
        CHECK(str(cast, "v", 0) == "p");
        CHECK(str(cast, "v", 1) == "q");
        CHECK(is_null(cast, "v", 2));
        // identical to the un-cast spelling
        auto plain = exec(d, "SELECT id, t ->> 'x' AS v FROM jp.t ORDER BY id;");
        REQUIRE(plain->is_success());
        CHECK(str(plain, "v", 0) == "p");
    }

    SECTION("a cast on a path key splits into segments just like the bare path") {
        // #>> takes a whole path: ('a.b'::text) splits to a/b.
        auto cast = exec(d, "SELECT id, t #>> ('a.b'::text) AS v FROM jp.t ORDER BY id;");
        REQUIRE(cast->is_success());
        CHECK(i64(cast, "v", 0) == 10);
        CHECK(i64(cast, "v", 1) == 30);
        // ->> is single-key, so ('a.b'::text) is the literal key "a.b" (no such column)
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
        // (1::bool) used to segfault; now the key is the text "1" (no such column).
        // Scalar form errors cleanly; the table-valued form is covered by the
        // zero-match-expand case. Either way: no crash.
        CHECK_FALSE(exec(d, "SELECT t ->> (1::bool) AS v FROM jp.t;")->is_success());
        auto expand = exec(d, "SELECT t -> (1::bool) FROM jp.t;");
        REQUIRE(expand->is_success()); // key "1" matches nothing -> zero-match expand
    }
}

// A cast over a navigated value does not convert it: ::text on an integer leaf
// leaves it BIGINT. (Compare with bug_cast_nav_in_arithmetic_is_garbage, where
// the same no-op cast additionally corrupts the arithmetic that follows.)
TEST_CASE("integration::cpp::test_jsonb_support::bug_cast_over_navigation_is_a_noop") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/cast_noop");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    auto cur = exec(d, "SELECT (t #>> 'a.c')::text AS v FROM jp.t ORDER BY id;");
    REQUIRE(cur->is_success());
    // correct: STRING_LITERAL holding "20"
    CHECK(type_of(cur, "v") == components::types::logical_type::BIGINT);
    CHECK(i64(cur, "v", 0) == 20);
}

// Inside a join, scalar navigation is side-aware but EXPANSION is not: it
// resolves the path against one schema only, so the moment both joined tables
// carry the same subtree name it fails with "path not found" — for a path that
// exists on both sides. Key deletion inside a join is broken outright.
TEST_CASE("integration::cpp::test_jsonb_support::bug_expand_inside_join_is_side_blind") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/join_expand");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, lv, d.e, d.f) VALUES (1, 100, 111, 112), (2, 200, 222, 223);")
                ->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, mv, d.e, d.f) VALUES (1, 10, 11, 12), (2, 20, 22, 23);")->is_success());

    // Scalar navigation resolves each side correctly — this is the part that works.
    CHECK(i64_set(exec(d, "SELECT m #>> 'd.e' AS v FROM jp.l JOIN jp.m ON l.k = m.k;"), "v") ==
          std::set<int64_t>{11, 22});
    CHECK(i64_set(exec(d, "SELECT l #>> 'd.e' AS v FROM jp.l JOIN jp.m ON l.k = m.k;"), "v") ==
          std::set<int64_t>{111, 222});

    // Expansion of the very same subtree fails, from either side.
    // correct: two columns e, f holding 11,12 / 22,23 (resp. 111,112 / 222,223).
    CHECK_FALSE(exec(d, "SELECT m -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;")->is_success());
    CHECK_FALSE(exec(d, "SELECT l -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;")->is_success());

    // Key deletion inside a join cannot resolve any column at all.
    CHECK_FALSE(exec(d, "SELECT m - 'd' FROM jp.l JOIN jp.m ON l.k = m.k;")->is_success());
}

// Expansion DOES work inside a join as long as the subtree name belongs to only
// one of the joined tables — which is what makes the failure above a resolution
// bug rather than a missing feature.
TEST_CASE("integration::cpp::test_jsonb_support::expand_in_join_with_unique_subtree") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/join_expand_ok");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, lv) VALUES (1, 100), (2, 200);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, d.e, d.f) VALUES (1, 11, 12), (2, 22, 23);")->is_success());

    auto cur = exec(d, "SELECT m -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
    REQUIRE(cur->is_success());
    CHECK(aliases(cur) == std::set<std::string>{"e", "f"});
    CHECK(i64_set(cur, "e") == std::set<int64_t>{11, 22});
    CHECK(i64_set(cur, "f") == std::set<int64_t>{12, 23});
}

// With three or more joined tables, a column name shared by several of them
// silently resolves to the LEFTMOST table — so a navigation that names the third
// table returns the first table's values. (The same holds for a plain qualified
// column, so the root cause is join name resolution rather than jsonb; it is
// pinned here because navigation has no way to disambiguate at all.)
TEST_CASE("integration::cpp::test_jsonb_support::bug_three_table_join_takes_leftmost_value") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/join3");
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

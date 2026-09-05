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
//   SELECT CASE WHEN t #>> 'a.b' = 10 THEN 1 ELSE 0 END -- only when the leaf has a NULL row;
//     FROM t;                                           --   reproduces on plain columns too (general 3VL bug)
//
// FIXED (were crashes/wrong results, now pinned as correct below):
//   [A] get_str_value hardening — a cast key is transparent, a NULL key is a clean
//       error, and neither `t -> (1::bool)` nor `t ->> NULL` crashes any more.
//   [B] one path<->column codec (components/expressions/jsonb_path.hpp) — the split
//       (operand -> segments) and join (segments -> "a/b" name) live in one place,
//       shared by navigation, existence and the INSERT flattener.
//   [G] INSERT target flattening keeps every segment (a.b.c -> "a/b/c") and renders
//       a subscript target (arr[0] -> "arr/0") instead of crashing on it.
//   [C] delete accepts the text-array form (t - '{a,b}') and removes every listed
//       subtree, not just a single key.
//   [E] existence over a missing key is absent (false), not a hard error: a truly
//       absent key folds to constant false and never poisons a '?|' any-of.
//   [D] table-valued expand/delete are well-behaved: expanding a path that matches
//       no column is a "path not found" error (not a silently vanished select
//       item); inside a join expand and delete resolve against the side the
//       operator named (not blindly across both, which was ambiguous); and under
//       GROUP BY / an aggregate they are a clean rejection, not a segfault (the
//       group branch never expands them).
//   [F] a cast over a scalar navigation used in arithmetic ((t #>> 'a.c')::bigint
//       + 1) reads the navigated column per row, instead of folding the navigation
//       into an uninitialized constant parameter (per-run garbage on every row).
//   [H] a NULL written to a not-yet-existing column of a computing table is an
//       absent key: it carries no type, so the column is dropped after schema
//       reconciliation instead of reaching storage as an all-NA column (segfault).
//   [I] INSERT ... SELECT lands each projected value in its target column: the
//       target names are stamped onto the streamed columns before the name-based
//       append, instead of appending a row where the plain columns came out NULL.

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
    CHECK(is_null(cur, "x", 2));   // row 3 never supplied x
    CHECK(is_null(cur, "a/b", 3)); // row 4 never supplied a.b
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

    SECTION("navigation works through a table alias, which hides the base name") {
        CHECK(i64(exec(d, "SELECT tt #>> 'a.b' AS v FROM jp.t AS tt ORDER BY id;"), "v", 0) == 10);
        // An alias replaces the relation name, so the base name no longer reaches the table
        // as in PostgreSQL, which answers such a reference with a hint to use the alias instead
        CHECK_FALSE(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t AS tt ORDER BY id;")->is_success());
    }

    SECTION("keys are case-sensitive") { CHECK_FALSE(exec(d, "SELECT t ->> 'X' AS v FROM jp.t;")->is_success()); }
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
    auto config = make_test_config("/tmp/test_jsonb_matrix/persist", true);
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

// [I] INSERT ... SELECT lands each projected value in its target column. The
// append is name-based, and a projection need not carry the target names
// (SELECT 5, 55), so the target names are stamped on the streamed columns in
// target order. It used to skip that step and append a row in which even the
// plain columns were NULL (the projection-named columns matched nothing).
TEST_CASE("integration::cpp::test_jsonb_support::insert_select_maps_projection_to_target_columns") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/insert_select");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    // rows 1..4: a/b = 10,30,50,(absent); a/c = 20,40,60,70

    auto ins = exec(d, "INSERT INTO jp.t (id, a.b) SELECT 5, 55;");
    REQUIRE(ins->is_success());
    CHECK(ins->size() == 1);

    auto cur = exec(d, "SELECT * FROM jp.t ORDER BY id;");
    REQUIRE(cur->is_success());
    CHECK(cur->size() == 5);
    // the new row carries its values, not NULLs: id=5, a/b=55, and the columns
    // the projection did not name (a/c, x) are absent/null for it
    CHECK(i64(cur, "id", 4) == 5);
    CHECK(i64(cur, "a/b", 4) == 55);
    CHECK(is_null(cur, "a/c", 4));
    CHECK(is_null(cur, "x", 4));
    // navigation sees the appended leaf too (row 4 keeps a/b absent -> null)
    CHECK(i64_set(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t;"), "v") == std::set<int64_t>{10, 30, 50, 55});

    // a SELECT from another table, projected out of target order, still routes
    // each value by target position (a.b <- w, id <- k), not by source name
    REQUIRE(exec(d, "CREATE TABLE jp.src ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.src (k, w) VALUES (100, 200);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.t (a.b, id) SELECT w, k FROM jp.src;")->is_success());
    CHECK(ids(exec(d, "SELECT id FROM jp.t;")) == std::set<int64_t>{1, 2, 3, 4, 5, 100});
    CHECK(i64(exec(d, "SELECT t #>> 'a.b' AS v FROM jp.t WHERE id = 100;"), "v", 0) == 200);
}

// A jsonb scalar navigation lowers to a plain read of its flattened column, so a
// same-table comparison of two navigations is an ordinary column-vs-column
// predicate: it returns exactly the rows whose two leaves are equal, with SQL
// NULL semantics (an absent leaf never matches). A path naming no column at all
// is still a clean error, as for any other navigation (see
// navigation_over_missing_key_still_errors). This was pinned as an unsupported
// rejection until the predicate/scan layer was generalized upstream.
TEST_CASE("integration::cpp::test_jsonb_support::compare_two_navigations") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/nav_cmp");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());
    // rows 2 and 3 have a.b == a.c; row 4 leaves a.b absent (NULL)
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
        // nor on the left of IN / LIKE
        "SELECT id FROM jp.t WHERE t #>> 'a.b' IN (10, 30);",
        "SELECT id FROM jp.t WHERE t ->> 'x' LIKE 'p%';",
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

// A table-valued operator (expand '->'/'#>', delete '-'/'#-') is lowered to
// per-column get_field only on the plain SELECT path. Under GROUP BY — or a bare
// aggregate, which routes through the same group branch — it is never expanded, so
// an un-expanded node used to reach physical execution and SEGFAULT the process.
// It must be a clean rejection instead (expanding one row into several columns has
// no meaning under grouping).
TEST_CASE("integration::cpp::test_jsonb_support::table_valued_op_rejected_under_grouping") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/gb_reject");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.gb ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.gb (g, a.b, x) VALUES (1, 1, 9), (1, 2, 90);")->is_success());

    // clean errors, not crashes — with an explicit GROUP BY ...
    CHECK_FALSE(exec(d, "SELECT g, gb -> 'a' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb #> 'a' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb - 'x' FROM jp.gb GROUP BY g;")->is_success());
    CHECK_FALSE(exec(d, "SELECT gb #- 'a.b' FROM jp.gb GROUP BY g;")->is_success());
    // ... and with a bare aggregate (routes through the group branch too)
    CHECK_FALSE(exec(d, "SELECT gb -> 'a', COUNT(x) FROM jp.gb;")->is_success());

    // a genuine aggregate over the same table is unaffected
    auto ok = exec(d, "SELECT g, COUNT(x) AS n FROM jp.gb GROUP BY g;");
    REQUIRE(ok->is_success());
    CHECK(ok->size() == 1);
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

// [F] A cast applied to a navigated value, then used in arithmetic, reads the
// navigated column per row like every other operand. It used to fall through to
// the constant-parameter path, which folded the navigation expression into an
// uninitialized parameter: a per-run-varying garbage constant repeated on every
// row. Both halves were individually correct — only the two together corrupted.
TEST_CASE("integration::cpp::test_jsonb_support::cast_nav_in_arithmetic_reads_the_column") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/cast_arith");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    // rows: a/c = 20, 40, 60, 70 (present on every row)

    // both halves are individually correct
    auto cast_only = exec(d, "SELECT id, (t #>> 'a.c')::bigint AS v FROM jp.t ORDER BY id;");
    REQUIRE(cast_only->is_success());
    CHECK(i64(cast_only, "v", 0) == 20);

    auto arith_only = exec(d, "SELECT id, (t #>> 'a.c') + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(arith_only->is_success());
    CHECK(i64(arith_only, "v", 0) == 21);

    // together: now the per-row value, not a repeated garbage constant
    auto both = exec(d, "SELECT id, (t #>> 'a.c')::bigint + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(both->is_success());
    CHECK(i64(both, "v", 0) == 21);
    CHECK(i64(both, "v", 1) == 41);
    CHECK(i64(both, "v", 2) == 61);
    CHECK(i64(both, "v", 3) == 71);

    // the cast composes on either side of the operator, still per row
    auto both2 = exec(d, "SELECT id, 100 - (t #>> 'a.b')::bigint AS v FROM jp.t WHERE id < 3 ORDER BY id;");
    REQUIRE(both2->is_success());
    CHECK(i64(both2, "v", 0) == 90); // 100 - 10
    CHECK(i64(both2, "v", 1) == 70); // 100 - 30

    // the same shape over a plain column was always fine — this was nav-specific
    auto plain = exec(d, "SELECT id, (id)::bigint + 1 AS v FROM jp.t ORDER BY id;");
    REQUIRE(plain->is_success());
    CHECK(i64(plain, "v", 0) == 2);
}

// [G] An INSERT target of arbitrary depth flattens to the full slash-joined
// column name — every interior segment is kept, and the path written is the path
// read back. The write side and the read side share one codec, so any depth and
// either spelling (dotted or pg-array) round-trip.
TEST_CASE("integration::cpp::test_jsonb_support::deep_path_insert_keeps_all_segments") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/depth");
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

    // the exact path that was written reads back, both spellings, both operators
    CHECK(i64(exec(d, "SELECT d #>> 'a.b.c' AS v FROM jp.d WHERE id = 1;"), "v", 0) == 111);
    CHECK(i64(exec(d, "SELECT d #>> '{a,b,c}' AS v FROM jp.d WHERE id = 1;"), "v", 0) == 111);
    CHECK(i64(exec(d, "SELECT d #>> 'p.q.r.s.t' AS v FROM jp.d WHERE id = 2;"), "v", 0) == 222);
    // the truncated path nobody wrote is (correctly) absent now
    CHECK_FALSE(exec(d, "SELECT d #>> 'a.c' AS v FROM jp.d;")->is_success());

    // partial expansion through the deep path
    auto expand = exec(d, "SELECT d -> 'a' -> 'b' FROM jp.d WHERE id = 1;");
    REQUIRE(expand->is_success());
    CHECK(aliases(expand) == std::set<std::string>{"c"});
    CHECK(i64(expand, "c", 0) == 111);
}

// [G] A subscript target such as arr[0] flattens like any other segment
// (arr[0] -> "arr/0") instead of dereferencing the A_Indices node as a string,
// which used to throw an uncaught std::exception.
TEST_CASE("integration::cpp::test_jsonb_support::subscript_insert_target") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/subscript");
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
    // and it reads back through the path spelling
    CHECK(i64(exec(d, "SELECT d #>> 'arr.0' AS v FROM jp.d;"), "v", 0) == 7);
}

// [H] A NULL written to a column that does not yet exist has no type to create
// the column from. On a schemaless computing table that is simply an absent key:
// the column is not materialized (it used to build an all-NA column and segfault
// the insert), and it comes into existence — with the earlier rows null — only
// once some row supplies a concrete value.
TEST_CASE("integration::cpp::test_jsonb_support::insert_null_into_new_column_is_absent") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/null_insert");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());

    // NULL into a brand-new flattened column: no crash, and the value it carried
    // (x) is stored while the all-null a.b key is simply absent.
    REQUIRE(exec(d, "INSERT INTO jp.t (a.b, x) VALUES (NULL, 'z');")->is_success());
    auto only_x = exec(d, "SELECT * FROM jp.t;");
    REQUIRE(only_x->is_success());
    CHECK(aliases(only_x) == std::set<std::string>{"x"});

    // NULL then a concrete value in the same insert: the column springs into
    // existence and every earlier all-null row reads back null, not a zero.
    REQUIRE(exec(d, "CREATE TABLE jp.u ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.u (id, v) VALUES (1, NULL), (2, NULL), (3, 7);")->is_success());
    auto u = exec(d, "SELECT id, v FROM jp.u ORDER BY id;");
    REQUIRE(u->is_success());
    CHECK(i64(u, "id", 0) == 1);
    CHECK(is_null(u, "v", 0));
    CHECK(is_null(u, "v", 1));
    CHECK(i64(u, "v", 2) == 7);

    // an all-null single-column insert has nothing to store and no column to make
    REQUIRE(exec(d, "CREATE TABLE jp.w ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.w (a.b) VALUES (NULL);")->is_success());
    CHECK(exec(d, "SELECT * FROM jp.w;")->size() == 0);
}

// [C] `- '{a,b}'` is the postgres text-array form of key deletion (`jsonb - text[]`):
// it removes SEVERAL top-level keys at once. The bare `- 'key'` form still removes
// one key, and `#- 'a.b'` still deletes a nested path — the three are distinct.
TEST_CASE("integration::cpp::test_jsonb_support::delete_key_array_form") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/del_arr");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.t ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.t (id, a.b, a.c, x, y) VALUES (1, 10, 20, 'p', 'q');")->is_success());

    // single key (unchanged)
    CHECK(aliases(exec(d, "SELECT t - 'x' FROM jp.t;")) == std::set<std::string>{"id", "a/b", "a/c", "y"});

    // one-element array removes that one key
    CHECK(aliases(exec(d, "SELECT t - '{x}' FROM jp.t;")) == std::set<std::string>{"id", "a/b", "a/c", "y"});

    // several keys removed together
    CHECK(aliases(exec(d, "SELECT t - '{x,y}' FROM jp.t;")) == std::set<std::string>{"id", "a/b", "a/c"});

    // a key that names a subtree removes the whole subtree, mixed with a leaf key
    CHECK(aliases(exec(d, "SELECT t - '{x,a}' FROM jp.t;")) == std::set<std::string>{"id", "y"});

    // the empty array deletes nothing; an unknown key deletes nothing (no error)
    CHECK(aliases(exec(d, "SELECT t - '{}' FROM jp.t;")) == std::set<std::string>{"id", "a/b", "a/c", "x", "y"});
    CHECK(aliases(exec(d, "SELECT t - '{nokey}' FROM jp.t;")) == std::set<std::string>{"id", "a/b", "a/c", "x", "y"});

    // #- with an array is still a single nested PATH delete, not multi-key
    CHECK(aliases(exec(d, "SELECT t #- '{a,b}' FROM jp.t;")) == std::set<std::string>{"id", "a/c", "x", "y"});

    // values of the surviving columns are intact after a multi-key delete
    auto surv = exec(d, "SELECT t - '{a}' FROM jp.t;");
    REQUIRE(surv->is_success());
    CHECK(aliases(surv) == std::set<std::string>{"id", "x", "y"});
    CHECK(str(surv, "x", 0) == "p");
    CHECK(str(surv, "y", 0) == "q");
}

// Expanding a path that matches no column does not error — the expansion item is
// silently ERASED from the select list. On its own that yields a zero-column
// result; alongside other items it silently changes the arity of the answer.
// [D] Expand ('->'/'#>') names a specific object; a key that matches no column is
// a "path not found" error, exactly like the scalar form. It used to be erased to
// nothing, so the select item silently vanished and the caller got fewer columns
// than it asked for, with no indication that anything went wrong.
TEST_CASE("integration::cpp::test_jsonb_support::zero_match_expand_is_an_error") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/zero_exp");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // the scalar operator errors, as it always has
    CHECK_FALSE(exec(d, "SELECT t ->> 'nokey' AS v FROM jp.t;")->is_success());

    // the table-valued one now errors too, instead of producing zero columns
    CHECK_FALSE(exec(d, "SELECT t -> 'nokey' FROM jp.t;")->is_success());

    // ... and it no longer disappears silently from among other select items
    CHECK_FALSE(exec(d, "SELECT t -> 'nokey', x FROM jp.t;")->is_success());
    CHECK_FALSE(exec(d, "SELECT id, t -> 'nokey', x FROM jp.t;")->is_success());

    // a dotted path handed to -> (which takes a single key, not a path) names the
    // literal key "a.b", which no column matches -> error, not a vanished column
    CHECK_FALSE(exec(d, "SELECT t -> 'a.b' FROM jp.t;")->is_success());

    // a key that DOES match still expands, so the guard is scoped to true misses
    CHECK(aliases(exec(d, "SELECT t -> 'a' FROM jp.t;")) == std::set<std::string>{"b", "c"});
}

// LIMITATION of the flattened representation (documented, not a fix target here).
// A computing-table column name and a nested path share one namespace, and the
// path separator '/' is a legal identifier character, so the nested path a.b and a
// column whose literal name is "a/b" are the same storage column "a/b" — a value
// written one way is visible the other way. Making them distinct (postgres keeps
// the jsonb key "a/b" separate from a nested a->b) would require percent-escaping
// EVERY column identifier on read and write system-wide — including regular tables
// and DDL, well outside jsonb — so we do not do it here. The one codec that owns
// the mapping (jsonb_path.hpp) documents the same reservation. This case pins the
// behavior so a future escaping change is a deliberate, visible flip rather than a
// silent one.
TEST_CASE("integration::cpp::test_jsonb_support::flattened_name_and_nested_path_share_storage") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/sep");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.s ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.s (id, a.b) VALUES (1, 10);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.s (id, \"a/b\") VALUES (2, 20);")->is_success());

    auto star = exec(d, "SELECT * FROM jp.s ORDER BY id;");
    REQUIRE(star->is_success());
    // one shared column "a/b", not two
    CHECK(aliases(star) == std::set<std::string>{"id", "a/b"});

    // both spellings address that one column, so each sees the other's row
    CHECK(i64_set(exec(d, "SELECT s #>> 'a.b' AS v FROM jp.s;"), "v") == std::set<int64_t>{10, 20});
    CHECK(i64_set(exec(d, "SELECT s ->> 'a/b' AS v FROM jp.s;"), "v") == std::set<int64_t>{10, 20});
}

// [E] Existence over a missing key follows postgres 3VL instead of hard-erroring,
// which is what makes '?'/'?|'/'?&' usable: you can ask "does this row have key K"
// without K being known to exist. A truly absent key is false; an intermediate
// object key (a prefix of stored columns) is present iff a child is; and one
// absent key no longer poisons an any-of another key satisfies.
TEST_CASE("integration::cpp::test_jsonb_support::existence_over_missing_key") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/missing");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);
    // rows: 1(a/b=10,a/c=20,x=p) 2(a/b=30,a/c=40,x=q) 3(a/b=50,a/c=60) 4(a/c=70)

    // a key present in some rows
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'x';")) == std::set<int64_t>{1, 2});
    // a truly absent key -> false for every row (no error)
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ? 'nokey';")->size() == 0);
    CHECK(exec(d, "SELECT id FROM jp.t WHERE NOT (t ? 'nokey');")->size() == 4);

    // one absent key no longer poisons an any-of / all-of
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?| '{x,nokey}';")) == std::set<int64_t>{1, 2});
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ?& '{x,nokey}';")->size() == 0);
    CHECK(exec(d, "SELECT id FROM jp.t WHERE t ?| '{nokey,nokey2}';")->size() == 0);

    // an intermediate object key 'a' (prefix of a/b, a/c) is PRESENT where a child
    // is non-null — a/b or a/c is set for every row, so all four rows
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ? 'a';")) == std::set<int64_t>{1, 2, 3, 4});
    CHECK(ids(exec(d, "SELECT id FROM jp.t WHERE t ?& '{a,x}';")) == std::set<int64_t>{1, 2});

    // a mistyped REGULAR column IS NULL still errors — only jsonb keys are lenient
    REQUIRE(exec(d, "CREATE TABLE jp.r (id BIGINT, v BIGINT);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.r (id, v) VALUES (1, 5);")->is_success());
    CHECK_FALSE(exec(d, "SELECT id FROM jp.r WHERE nosuchcol IS NULL;")->is_success());
}

// STILL a hard error, deferred (documented): scalar navigation over an absent key
// should yield SQL NULL, and `<nav> IS NULL` should be a boolean. Both need the
// select-list / compare paths to synthesize a typed NULL leaf for a flagged key,
// a larger change than the existence rewrite; pinned so the deferral is visible.
TEST_CASE("integration::cpp::test_jsonb_support::navigation_over_missing_key_still_errors") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/missing_nav");
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK_FALSE(exec(d, "SELECT t ->> 'nokey' AS v FROM jp.t;")->is_success());           // deferred: NULL every row
    CHECK_FALSE(exec(d, "SELECT t #>> 'no.key' AS v FROM jp.t;")->is_success());          // deferred: NULL every row
    CHECK_FALSE(exec(d, "SELECT id FROM jp.t WHERE t #>> 'a.b' IS NULL;")->is_success()); // deferred: row 4
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
        // Both the scalar and the table-valued form error cleanly on the miss.
        CHECK_FALSE(exec(d, "SELECT t ->> (1::bool) AS v FROM jp.t;")->is_success());
        CHECK_FALSE(exec(d, "SELECT t -> (1::bool) FROM jp.t;")->is_success());
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

// [D] Inside a join a jsonb operator names one table (its base), and the base's
// side disambiguates a subtree name both joined tables carry. Expansion and key
// deletion used to be side-blind — they matched columns from both sides, so every
// produced column was ambiguous ("path not found") — while scalar navigation was
// already side-aware. Now all three resolve to the side the operator named.
TEST_CASE("integration::cpp::test_jsonb_support::expand_inside_join_is_side_aware") {
    auto config = make_test_config("/tmp/test_jsonb_matrix/join_expand");
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(exec(d, "CREATE DATABASE jp;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.l ();")->is_success());
    REQUIRE(exec(d, "CREATE TABLE jp.m ();")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.l (k, lv, d.e, d.f) VALUES (1, 100, 111, 112), (2, 200, 222, 223);")->is_success());
    REQUIRE(exec(d, "INSERT INTO jp.m (k, mv, d.e, d.f) VALUES (1, 10, 11, 12), (2, 20, 22, 23);")->is_success());

    // Scalar navigation resolves each side correctly — this always worked.
    CHECK(i64_set(exec(d, "SELECT m #>> 'd.e' AS v FROM jp.l JOIN jp.m ON l.k = m.k;"), "v") ==
          std::set<int64_t>{11, 22});
    CHECK(i64_set(exec(d, "SELECT l #>> 'd.e' AS v FROM jp.l JOIN jp.m ON l.k = m.k;"), "v") ==
          std::set<int64_t>{111, 222});

    // Expansion of the shared subtree now works from either side, picking that
    // side's values into the two rerooted columns e, f.
    auto me = exec(d, "SELECT m -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
    REQUIRE(me->is_success());
    CHECK(aliases(me) == std::set<std::string>{"e", "f"});
    CHECK(i64_set(me, "e") == std::set<int64_t>{11, 22});
    CHECK(i64_set(me, "f") == std::set<int64_t>{12, 23});

    auto le = exec(d, "SELECT l -> 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
    REQUIRE(le->is_success());
    CHECK(i64_set(le, "e") == std::set<int64_t>{111, 222});
    CHECK(i64_set(le, "f") == std::set<int64_t>{112, 223});

    // Key deletion resolves against the named side too: m minus subtree d keeps
    // exactly m's remaining columns (k, mv), not l's.
    auto md = exec(d, "SELECT m - 'd' FROM jp.l JOIN jp.m ON l.k = m.k;");
    REQUIRE(md->is_success());
    CHECK(aliases(md) == std::set<std::string>{"k", "mv"});
    CHECK(i64_set(md, "mv") == std::set<int64_t>{10, 20});
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

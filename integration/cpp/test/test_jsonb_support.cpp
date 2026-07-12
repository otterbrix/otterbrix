#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

// Characterization suite for the JSONB operator surface: probes every operator
// in every clause context (supported, rejected, and undefined-behavior edges)
// against computing tables and regular tables, and prints one grep-able
// "[JSONB] <id> | <verdict> | ..." line per probe so the run output doubles as
// a support matrix. CHECK()s pin the behavior observed at the time of writing;
// probes marked "empirical" had no prior coverage anywhere in the repo.

static const database_name_t jp_db = "jsp";

namespace {

    struct probe_t {
        bool ok{false};
        std::string error;
        size_t rows{0};
        size_t cols{0};
        std::set<std::string> aliases;
    };

    template<typename Dispatcher>
    probe_t probe(Dispatcher* dispatcher, const std::string& id, const std::string& sql) {
        probe_t p;
        auto session = otterbrix::session_id_t();
        components::cursor::cursor_t_ptr cur;
        try {
            cur = dispatcher->execute_sql(session, sql);
        } catch (const std::exception& e) {
            p.error = std::string("uncaught exception: ") + e.what();
            std::cout << "[JSONB] " << id << " | EXC | " << p.error << " | " << sql << std::endl;
            return p;
        } catch (...) {
            p.error = "uncaught non-std exception";
            std::cout << "[JSONB] " << id << " | EXC | " << p.error << " | " << sql << std::endl;
            return p;
        }
        if (!cur) {
            p.error = "null cursor";
            std::cout << "[JSONB] " << id << " | NULLCUR | " << sql << std::endl;
            return p;
        }
        if (cur->is_success()) {
            p.ok = true;
            p.rows = cur->size();
            p.cols = cur->column_count();
            if (!cur->chunks().empty()) {
                const auto& chunk = cur->chunks().front();
                for (size_t c = 0; c < chunk.column_count(); ++c) {
                    p.aliases.insert(std::string(chunk.data[c].type().alias()));
                }
            }
            std::ostringstream a;
            for (const auto& s : p.aliases) {
                a << s << ",";
            }
            std::cout << "[JSONB] " << id << " | OK | rows=" << p.rows << " cols=" << p.cols << " aliases={" << a.str()
                      << "} | " << sql << std::endl;
        } else {
            p.error = cur->get_error().what;
            std::cout << "[JSONB] " << id << " | ERR | " << p.error << " | " << sql << std::endl;
        }
        return p;
    }

} // namespace

// ---------------------------------------------------------------------------
// 1. Baseline: the documented supported set (mirrors test_computed_schema.cpp,
//    kept minimal — regressions here mean the whole matrix moved).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::baseline_supported") {
    auto config = test_create_config("/tmp/test_jsonb_support/baseline");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.b ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.b (a.b, a.c, x) VALUES (1, 2, 9), (10, 20, 90);").ok);

    CHECK(probe(d, "B1 scalar ->> top-level", "SELECT b ->> 'x' AS xx FROM jsp.b ORDER BY x;").ok);
    CHECK(probe(d, "B2 scalar -> ->> nested", "SELECT b -> 'a' ->> 'b' AS v FROM jsp.b ORDER BY x;").ok);
    CHECK(probe(d, "B3 #>> dotted", "SELECT b #>> 'a.c' AS c FROM jsp.b;").ok);
    CHECK(probe(d, "B4 #>> pg-array", "SELECT b #>> '{a,c}' AS c FROM jsp.b;").ok);
    CHECK(probe(d, "B5 nav in WHERE", "SELECT x FROM jsp.b WHERE b -> 'a' ->> 'b' = 10;").rows == 1);
    CHECK(probe(d, "B6 expand ->", "SELECT b -> 'a' FROM jsp.b;").aliases == std::set<std::string>{"b", "c"});
    CHECK(probe(d, "B7 delete -", "SELECT b - 'x' FROM jsp.b;").aliases == std::set<std::string>{"a/b", "a/c"});
    CHECK(probe(d, "B8 delete #- dotted", "SELECT b #- 'a.b' FROM jsp.b;").aliases == std::set<std::string>{"a/c", "x"});
    CHECK(probe(d, "B9 exists ?", "SELECT x FROM jsp.b WHERE b ? 'x';").rows == 2);
}

// ---------------------------------------------------------------------------
// 2. Scalar navigation edges (empirical)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::scalar_nav_edges") {
    auto config = test_create_config("/tmp/test_jsonb_support/nav_edges");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.n ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.n (a.b, x) VALUES (1, 9), (2, 90);").ok);

    // Key absent from schema errors (find_types fails), like '?'.
    CHECK_FALSE(probe(d, "N1 ->> missing key", "SELECT n ->> 'nope' FROM jsp.n;").ok);
    CHECK_FALSE(probe(d, "N2 -> ->> missing nested", "SELECT n -> 'a' ->> 'nope' FROM jsp.n;").ok);
    // Qualified column base instead of bare table name (output alias is the full path "a/b").
    CHECK(probe(d, "N3 qualified base n.a ->>", "SELECT n.a ->> 'b' FROM jsp.n;").ok);
    // Table alias as base.
    CHECK(probe(d, "N4 alias base", "SELECT t ->> 'x' FROM jsp.n AS t;").ok);
    // Integer key (path segment '0') — no such column: error, no crash.
    CHECK_FALSE(probe(d, "N5 integer key", "SELECT n -> 'a' ->> 0 FROM jsp.n;").ok);
    // Spaces inside a PG-array path are trimmed.
    CHECK(probe(d, "N6 pg-array with spaces", "SELECT n #>> '{ a , b }' FROM jsp.n;").ok);
    // Quoted segments inside a PG-array PATH are NOT dequoted (path becomes '"a"/"b"'),
    // unlike the '?|'/'?&' key lists which do dequote.
    CHECK_FALSE(probe(d, "N7 pg-array quoted", "SELECT n #>> '{\"a\",\"b\"}' FROM jsp.n;").ok);
    // Empty path errors ("empty jsonb path").
    CHECK_FALSE(probe(d, "N8 empty pg-array path", "SELECT n #>> '{}' FROM jsp.n;").ok);
    // OPERATOR() syntax reaches the same generic Op machinery.
    CHECK(probe(d, "N9 OPERATOR(->>) syntax", "SELECT n OPERATOR(->>) 'x' FROM jsp.n;").ok);
    // '::' binds tighter than the operators, so the cast lands on the key
    // literal; a non-boolean cast keeps the argument as the key ('x' here).
    CHECK(probe(d, "N10 :: on last literal", "SELECT n ->> 'x'::string FROM jsp.n;").rows == 2);
    CHECK(probe(d, "N10b ::text key", "SELECT n -> 'a' ->> 'b'::text FROM jsp.n;").rows == 2);
    // Parenthesized chain + plain cast.
    CHECK(probe(d, "N11 (chain)::bigint", "SELECT (n ->> 'x')::bigint FROM jsp.n;").ok);
    // ParamRef as path segment: the literal segment "$1" is a missing path.
    CHECK_FALSE(probe(d, "N12 param path segment", "SELECT n -> $1 FROM jsp.n;").ok);
    // Arithmetic over a nav chain in the SELECT list.
    CHECK(probe(d, "N13 nav + 1", "SELECT (n ->> 'x') + 1 AS x1 FROM jsp.n ORDER BY x;").ok);
    // CASE with nav in condition and result.
    CHECK(probe(d, "N14 CASE over nav", "SELECT CASE WHEN (n ->> 'x') > 10 THEN 1 ELSE 0 END FROM jsp.n;").ok);
    // ORDER BY: bare nav is rejected; nav inside arithmetic is allowed.
    CHECK_FALSE(probe(d, "N15 bare ORDER BY nav", "SELECT x FROM jsp.n ORDER BY n ->> 'x';").ok);
    CHECK(probe(d, "N16 ORDER BY nav+0", "SELECT x FROM jsp.n ORDER BY (n ->> 'x') + 0;").ok);
    // Boolean-cast keys: only string literals are valid bool spellings; a
    // non-string arg used to reinterpret the Value union as a char* (segfault).
    CHECK_FALSE(probe(d, "N17 (1::bool) key", "SELECT n ->> (1::bool) FROM jsp.n;").ok);
    {
        // 'true'::boolean maps to key "true" (it used to become "false").
        auto p = probe(d, "N18 ('true'::boolean) key", "SELECT n ->> ('true'::boolean) FROM jsp.n;");
        CHECK_FALSE(p.ok);
        CHECK(p.error.find("'true'") != std::string::npos);
    }
    {
        auto p = probe(d, "N19 ? TRUE literal", "SELECT x FROM jsp.n WHERE n ? TRUE;");
        CHECK_FALSE(p.ok);
        CHECK(p.error.find("'true'") != std::string::npos);
    }
    // NULL as a key is a clean literal error, not a misleading one.
    CHECK_FALSE(probe(d, "N20 -> NULL key", "SELECT n -> NULL FROM jsp.n;").ok);
}

// ---------------------------------------------------------------------------
// 3. Existence operator edges (empirical: multi-key, empty, missing)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::exists_edges") {
    auto config = test_create_config("/tmp/test_jsonb_support/exists_edges");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.e ();").ok);
    // r1: x only; r2: y only; r3: both; r4: neither (k is the common column).
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.e (k, x) VALUES (1, 10);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.e (k, y) VALUES (2, 20);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.e (k, x, y) VALUES (3, 30, 40);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.e (k) VALUES (4);").ok);

    // Multi-key any-of / all-of — the union_or/union_and paths, never tested before.
    CHECK(probe(d, "E1 ?| multi-key", "SELECT k FROM jsp.e WHERE e ?| '{x,y}' ORDER BY k;").rows == 3);
    CHECK(probe(d, "E2 ?& multi-key", "SELECT k FROM jsp.e WHERE e ?& '{x,y}' ORDER BY k;").rows == 1);
    // Degenerate empty lists: '?|' vacuously false (0 rows), '?&' vacuously true (all rows).
    CHECK(probe(d, "E3 ?| empty list", "SELECT k FROM jsp.e WHERE e ?| '{}';").rows == 0);
    CHECK(probe(d, "E4 ?& empty list", "SELECT k FROM jsp.e WHERE e ?& '{}';").rows == 4);
    // Missing-key existence: known gap — errors instead of returning false.
    CHECK_FALSE(probe(d, "E5 ? missing key", "SELECT k FROM jsp.e WHERE e ? 'nope';").ok);
    // One missing key poisons the whole '?|' even though 'x' exists.
    CHECK_FALSE(probe(d, "E6 ?| one missing one present", "SELECT k FROM jsp.e WHERE e ?| '{x,nope}';").ok);
    // Existence under a missing prefix errors too.
    CHECK_FALSE(probe(d, "E7 ? under missing prefix", "SELECT k FROM jsp.e WHERE e -> 'nope' ? 'x';").ok);
    // '?' in the SELECT list is rejected ("Unknown A_Expr kind in field clause").
    CHECK_FALSE(probe(d, "E8 ? in SELECT list", "SELECT e ? 'x' FROM jsp.e;").ok);
}

// ---------------------------------------------------------------------------
// 4. Expand / delete edges (empirical: pg-array forms, zero-match, multi-type)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::expand_delete_edges") {
    auto config = test_create_config("/tmp/test_jsonb_support/exp_del_edges");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.ed ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.ed (a.b, a.c, x) VALUES (1, 2, 9);").ok);

    // PG-array path forms of the table-valued ops (only dotted form was tested).
    CHECK(probe(d, "D1 #- pg-array", "SELECT ed #- '{a,b}' FROM jsp.ed;").aliases ==
          std::set<std::string>{"a/c", "x"});
    CHECK(probe(d, "D2 #> multi-element", "SELECT ed #> '{a,b}' FROM jsp.ed;").aliases == std::set<std::string>{"b"});
    // Expanding a missing prefix errors like scalar navigation does.
    CHECK_FALSE(probe(d, "D3 expand zero-match", "SELECT ed -> 'nope' FROM jsp.ed;").ok);
    // Deleting a missing prefix is a no-op (jsonb semantics): keeps everything.
    CHECK(probe(d, "D4 delete zero-match", "SELECT ed - 'nope' FROM jsp.ed;").cols == 3);
    // Deleting EVERY column is legal (empty object per row): rows survive with
    // zero columns.
    {
        REQUIRE(probe(d, "setup", "CREATE TABLE jsp.donly ();").ok);
        REQUIRE(probe(d, "setup", "INSERT INTO jsp.donly (a.b, a.c) VALUES (1, 2);").ok);
        auto p = probe(d, "D11 delete everything", "SELECT donly - 'a' FROM jsp.donly;");
        CHECK(p.ok);
        CHECK(p.rows == 1);
        CHECK(p.cols == 0);
    }
    // Expand on a never-inserted computing table errors like any column
    // reference does there (the schema is empty).
    {
        REQUIRE(probe(d, "setup", "CREATE TABLE jsp.eempty ();").ok);
        CHECK_FALSE(probe(d, "D12 expand on empty table", "SELECT eempty -> 'a' FROM jsp.eempty;").ok);
        CHECK_FALSE(probe(d, "D12b column on empty table", "SELECT x FROM jsp.eempty;").ok);
    }
    // Expand mixed with an ordinary column in the same SELECT list.
    CHECK(probe(d, "D5 expand + column", "SELECT x, ed -> 'a' FROM jsp.ed;").aliases ==
          std::set<std::string>{"b", "c", "x"});
    // Two table-valued ops in one list compose (union of their column sets).
    CHECK(probe(d, "D6 two expands", "SELECT ed -> 'a', ed - 'a' FROM jsp.ed;").aliases ==
          std::set<std::string>{"b", "c", "x"});
    // Expand in scalar position is a transform error ("returns a table...").
    CHECK_FALSE(probe(d, "D7 -> in WHERE", "SELECT x FROM jsp.ed WHERE ed -> 'a' = 1;").ok);
    // Delete via '-' with a non-table LHS stays arithmetic (fails on types here).
    CHECK_FALSE(probe(d, "D8 x - 'a' arithmetic", "SELECT x - 'a' FROM jsp.ed;").ok);

    // Multi-type interactions: survivors/expansion including a multi-type name.
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.mt ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.mt (a.b, x, v) VALUES (1, 9, 10);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.mt (v) VALUES ('s');").ok);
    // delete whose survivors include multi-type 'v' errors (variant resolution fails).
    CHECK_FALSE(probe(d, "D9 delete leaves multitype", "SELECT mt - 'x' FROM jsp.mt;").ok);
    // expand over a subtree containing a multi-type leaf errors too.
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.mte ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.mte (a.b, x) VALUES (1, 9);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.mte (a.b) VALUES ('s');").ok);
    CHECK_FALSE(probe(d, "D10 expand multitype subtree", "SELECT mte -> 'a' FROM jsp.mte;").ok);
}

// ---------------------------------------------------------------------------
// 5. Parse-only / rejected operators — expect clean error cursors, no crashes
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::rejected_operators") {
    auto config = test_create_config("/tmp/test_jsonb_support/rejected");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.r ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.r (a.b, x) VALUES (1, 9);").ok);

    CHECK_FALSE(probe(d, "R1 @> containment", "SELECT x FROM jsp.r WHERE r @> '{\"x\": 9}';").ok);
    CHECK_FALSE(probe(d, "R2 <@ contained-by", "SELECT x FROM jsp.r WHERE r <@ '{\"x\": 9}';").ok);
    CHECK_FALSE(probe(d, "R3 || concat", "SELECT r || r FROM jsp.r;").ok);
    CHECK_FALSE(probe(d, "R4 @? jsonpath", "SELECT x FROM jsp.r WHERE r @? '$.x';").ok);
    CHECK_FALSE(probe(d, "R5 @@ jsonpath", "SELECT x FROM jsp.r WHERE r @@ '$.x == 9';").ok);
    CHECK_FALSE(probe(d, "R6 #- in WHERE", "SELECT x FROM jsp.r WHERE r #- 'a.b' = 1;").ok);
    // '-' in WHERE is arithmetic: the table name doesn't resolve as a column.
    CHECK_FALSE(probe(d, "R7 - table in WHERE", "SELECT x FROM jsp.r WHERE r - 'x' = 1;").ok);
    CHECK_FALSE(probe(d, "R8 ->> as bare boolean", "SELECT x FROM jsp.r WHERE r ->> 'x';").ok);
}

// ---------------------------------------------------------------------------
// 6. Clause contexts: GROUP BY / aggregates / RETURNING / HAVING / DML WHERE
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::clause_contexts") {
    auto config = test_create_config("/tmp/test_jsonb_support/clauses");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.c ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.c (g, a.b, x) VALUES (1, 1, 9), (1, 2, 90), (2, 3, 900);").ok);

    // GROUP BY a nav chain is rejected.
    CHECK_FALSE(probe(d, "C1 GROUP BY nav", "SELECT c ->> 'g' FROM jsp.c GROUP BY c ->> 'g';").ok);
    // Nav as aggregate argument is rejected ("unable to parse value").
    CHECK_FALSE(probe(d, "C2 COUNT(nav)", "SELECT COUNT(c ->> 'x') FROM jsp.c;").ok);
    CHECK_FALSE(probe(d, "C3 SUM(nav)", "SELECT SUM(c ->> 'x') FROM jsp.c;").ok);
    // Scalar nav in SELECT alongside GROUP BY on a plain column: rejected (the
    // group output schema doesn't carry the navigated column).
    CHECK_FALSE(probe(d, "C4 nav in grouped SELECT", "SELECT g, c ->> 'x' FROM jsp.c GROUP BY g;").ok);
    // HAVING with a nav operand works.
    CHECK(probe(d, "C5 HAVING nav", "SELECT g FROM jsp.c GROUP BY g HAVING (c ->> 'g') > 1;").ok);
    // RETURNING is rejected.
    CHECK_FALSE(probe(d, "C6 RETURNING nav", "INSERT INTO jsp.c (g, x) VALUES (3, 1) RETURNING c ->> 'g';").ok);
    // DML WHERE contexts (should work — same predicate path as SELECT WHERE).
    CHECK(probe(d, "C7 DELETE WHERE nav", "DELETE FROM jsp.c WHERE c -> 'a' ->> 'b' = 3;").ok);
    CHECK(probe(d, "C8 UPDATE WHERE nav", "UPDATE jsp.c SET x = 0 WHERE c -> 'a' ->> 'b' = 2;").ok);
    // Derived table / CTE: fails — but the plain-column control fails identically,
    // so this is a general computing-table derived-table limitation, not jsonb-specific.
    CHECK_FALSE(probe(d, "C9 nav in derived table", "SELECT v FROM (SELECT c ->> 'x' AS v FROM jsp.c) AS s;").ok);
    CHECK_FALSE(probe(d, "C9b control plain column", "SELECT v FROM (SELECT x AS v FROM jsp.c) AS s2;").ok);
    CHECK_FALSE(probe(d, "C10 nav in CTE", "WITH s3 AS (SELECT c ->> 'x' AS v FROM jsp.c) SELECT v FROM s3;").ok);
}

// ---------------------------------------------------------------------------
// 7. JOINs (empirical: plumbed via key sides, never tested)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::join_contexts") {
    auto config = test_create_config("/tmp/test_jsonb_support/joins");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.jl ();").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.jr ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.jl (a.b, k) VALUES (1, 1), (2, 2);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.jr (c.d, k) VALUES (10, 1), (20, 3);").ok);

    CHECK(probe(d, "J1 nav in ON", "SELECT jl.k FROM jsp.jl JOIN jsp.jr ON jl ->> 'k' = jr ->> 'k';").rows == 1);
    CHECK(probe(d,
                "J2 nav both sides SELECT",
                "SELECT jl -> 'a' ->> 'b', jr -> 'c' ->> 'd' FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k;")
              .ok);
    CHECK(probe(d, "J3 expand over join", "SELECT jl -> 'a' FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k;").aliases ==
          std::set<std::string>{"b"});
    // The table-valued ops are side-aware over JOINs: they act only on their
    // base table's columns and keep/skip the other side's untouched, even for
    // same-named columns ('k' exists on both sides here).
    {
        auto p = probe(d, "J4 delete over join", "SELECT jl - 'a' FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k;");
        CHECK(p.ok);
        CHECK(p.cols == 3); // jl.k + jr.{c/d, k}
        CHECK(p.aliases == std::set<std::string>{"c/d", "k"});
    }
    {
        auto p = probe(d, "J4b delete right side", "SELECT jr - 'c' FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k;");
        CHECK(p.ok);
        CHECK(p.cols == 3); // jl.{a/b, k} + jr.k
        CHECK(p.aliases == std::set<std::string>{"a/b", "k"});
    }
    {
        auto p = probe(d, "J4c delete shared name", "SELECT jl - 'k' FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k;");
        CHECK(p.ok);
        CHECK(p.cols == 3); // jl.a/b + ALL of jr — jr.k must survive jl's delete
        CHECK(p.aliases == std::set<std::string>{"a/b", "c/d", "k"});
    }
    // Expanding a shared name only touches the base table's side.
    CHECK(probe(d, "J4d expand shared name", "SELECT jl -> 'k' FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k;").cols == 1);
    CHECK(probe(d, "J5 exists over join", "SELECT jl.k FROM jsp.jl JOIN jsp.jr ON jl.k = jr.k WHERE jl ? 'k';").ok);
}

// ---------------------------------------------------------------------------
// 8. SQL/JSON standard constructs — expect zero support (errors, not crashes)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::sql_json_standard") {
    auto config = test_create_config("/tmp/test_jsonb_support/std");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.s ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.s (x) VALUES (1);").ok);

    CHECK_FALSE(probe(d,
                      "S1 JSON_TABLE full syntax",
                      "SELECT * FROM JSON_TABLE('{\"a\":1}', '$' COLUMNS (a INT PATH '$.a')) AS jt;")
                    .ok);
    CHECK_FALSE(probe(d, "S2 JSON_TABLE as func", "SELECT * FROM JSON_TABLE('{}');").ok);
    CHECK_FALSE(probe(d, "S3 JSON_VALUE", "SELECT JSON_VALUE(s, '$.x') FROM jsp.s;").ok);
    CHECK_FALSE(probe(d, "S4 JSON_QUERY", "SELECT JSON_QUERY(s, '$.x') FROM jsp.s;").ok);
    CHECK_FALSE(probe(d, "S5 JSON_EXISTS", "SELECT JSON_EXISTS(s, '$.x') FROM jsp.s;").ok);
    CHECK_FALSE(probe(d, "S6 json_object", "SELECT json_object('a', 1);").ok);
    CHECK_FALSE(probe(d, "S7 JSON_ARRAYAGG", "SELECT JSON_ARRAYAGG(x) FROM jsp.s;").ok);
    CHECK_FALSE(probe(d, "S8 IS JSON", "SELECT x IS JSON FROM jsp.s;").ok);
    // Unknown type names in casts do NOT error — the literal passes through untyped.
    CHECK(probe(d, "S9 ::jsonb cast", "SELECT '{\"a\":1}'::jsonb;").ok);
    CHECK(probe(d, "S10 CAST AS json", "SELECT CAST('{\"a\":1}' AS json);").ok);
    CHECK_FALSE(probe(d, "S11 FROM t -> 'a'", "SELECT * FROM jsp.s -> 'a';").ok);
    // jsonpath / unnest adjacents.
    CHECK_FALSE(probe(d, "S12 jsonb_each", "SELECT * FROM jsonb_each(s) AS e;").ok);
    CHECK_FALSE(probe(d, "S13 UNNEST", "SELECT * FROM UNNEST(s) AS u;").ok);
}

// ---------------------------------------------------------------------------
// 9. Regular (fixed-schema, relkind='r') tables — validator has no relkind
//    check, so do the operators work there? (empirical)
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::regular_table") {
    auto config = test_create_config("/tmp/test_jsonb_support/regular");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.reg (x BIGINT, y TEXT);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.reg (x, y) VALUES (1, 'a'), (2, 'b');").ok);

    // All operators work on regular tables too — the validator expansion has no
    // relkind check and fixed-schema columns carry their names as aliases.
    CHECK(probe(d, "G1 ->> on regular", "SELECT reg ->> 'x' FROM jsp.reg;").ok);
    CHECK(probe(d, "G2 -> expand on regular", "SELECT reg -> 'x' FROM jsp.reg;").aliases ==
          std::set<std::string>{"x"});
    CHECK(probe(d, "G3 - delete on regular", "SELECT reg - 'x' FROM jsp.reg;").aliases == std::set<std::string>{"y"});
    CHECK(probe(d, "G4 ? on regular", "SELECT x FROM jsp.reg WHERE reg ? 'y';").rows == 2);
    CHECK(probe(d, "G5 nav WHERE on regular", "SELECT x FROM jsp.reg WHERE reg ->> 'x' = 2;").rows == 1);
}

// ---------------------------------------------------------------------------
// 10. INSERT path depth: (a.b.c) must flatten to "a/b/c" with every segment
//     kept (it used to drop the middle segments, yielding "a/c").
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::insert_depth") {
    auto config = test_create_config("/tmp/test_jsonb_support/depth");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.dp ();").ok);
    CHECK(probe(d, "P1 INSERT depth-3 target", "INSERT INTO jsp.dp (a.b.c, x) VALUES (7, 1);").ok);
    CHECK(probe(d, "P2 resulting schema", "SELECT * FROM jsp.dp;").aliases == std::set<std::string>{"a/b/c", "x"});
    CHECK(probe(d, "P3 #>> '{a,b,c}'", "SELECT dp #>> '{a,b,c}' FROM jsp.dp;").rows == 1);
    CHECK_FALSE(probe(d, "P4 #>> '{a,c}' no longer matches", "SELECT dp #>> '{a,c}' FROM jsp.dp;").ok);
    // Expand reroots the whole remaining subtree.
    CHECK(probe(d, "P5 -> 'a' expand", "SELECT dp -> 'a' FROM jsp.dp;").aliases == std::set<std::string>{"b/c"});
    CHECK(probe(d, "P6 -> 'a' -> 'b' expand", "SELECT dp -> 'a' -> 'b' FROM jsp.dp;").aliases ==
          std::set<std::string>{"c"});
    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "SELECT dp -> 'a' -> 'b' ->> 'c' AS v FROM jsp.dp;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 7);
    }
    // Depth 4 keeps all segments too.
    CHECK(probe(d, "P7 INSERT depth-4 target", "INSERT INTO jsp.dp (q.r.s.t) VALUES (5);").ok);
    {
        auto p = probe(d, "P8 depth-4 schema", "SELECT * FROM jsp.dp;");
        CHECK(p.aliases == std::set<std::string>{"a/b/c", "q/r/s/t", "x"});
    }
    // Subscript segments (A_Indices) flatten via their index string instead of
    // being reinterpreted as string Values (which threw std::logic_error).
    {
        auto p1 = probe(d, "P9 INSERT (arr[1].b)", "INSERT INTO jsp.dp (arr[1].b) VALUES (3);");
        auto p2 = probe(d, "P10 subscript schema", "SELECT * FROM jsp.dp;");
        CHECK(p1.ok);
        CHECK(p2.aliases.count("arr/1/b") == 1);
    }
}

// ---------------------------------------------------------------------------
// 11. Persistence: jsonb over disk+WAL recovery (empirical — all existing
//     jsonb tests run with disk/wal off; dotted-column WAL replay untested).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::persistence") {
    auto config = test_create_config("/tmp/test_jsonb_support/persist");
    test_clear_directory(config);
    // disk + wal stay ON (defaults) — that's the point.

    std::cout << "[JSONB] W0 phase 1: insert + query with disk/wal on" << std::endl;
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
        REQUIRE(probe(d, "setup", "CREATE TABLE jsp.w ();").ok);
        CHECK(probe(d, "W1 dotted INSERT (wal on)", "INSERT INTO jsp.w (a.b, a.c, x) VALUES (1, 2, 9), (10, 20, 90);")
                  .ok);
        CHECK(probe(d, "W2 nav before restart", "SELECT w -> 'a' ->> 'b' AS b FROM jsp.w ORDER BY x;").rows == 2);
        CHECK(probe(d, "W3 expand before restart", "SELECT w -> 'a' FROM jsp.w;").aliases ==
              std::set<std::string>{"b", "c"});
    }
    std::cout << "[JSONB] W4 phase 2: restart (WAL replay)" << std::endl;
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        // Dotted-column schema and data survive WAL replay; the operators keep working.
        CHECK(probe(d, "W5 SELECT * after restart", "SELECT * FROM jsp.w;").aliases ==
              std::set<std::string>{"a/b", "a/c", "x"});
        CHECK(probe(d, "W6 nav after restart", "SELECT w -> 'a' ->> 'b' AS b FROM jsp.w ORDER BY x;").rows == 2);
        CHECK(probe(d, "W7 expand after restart", "SELECT w -> 'a' FROM jsp.w;").aliases ==
              std::set<std::string>{"b", "c"});
        CHECK(probe(d, "W8 exists after restart", "SELECT x FROM jsp.w WHERE w ? 'x';").rows == 2);
    }
}

// GROUP BY + table-valued op is rejected at validation (it used to slip
// through un-expanded and segfault in physical execution).
TEST_CASE("integration::cpp::test_jsonb_support::groupby_tablevalued_rejected") {
    auto config = test_create_config("/tmp/test_jsonb_support/gb_reject");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.gb ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.gb (g, a.b, x) VALUES (1, 1, 9), (1, 2, 90);").ok);

    CHECK_FALSE(probe(d, "X1 expand + GROUP BY", "SELECT g, gb -> 'a' FROM jsp.gb GROUP BY g;").ok);
    CHECK_FALSE(probe(d, "X2 delete + GROUP BY", "SELECT gb - 'x' FROM jsp.gb GROUP BY g;").ok);
    CHECK_FALSE(probe(d, "X2b #> + GROUP BY", "SELECT gb #> 'a' FROM jsp.gb GROUP BY g;").ok);
    // Aggregates route into the group branch even with no written GROUP BY.
    CHECK_FALSE(probe(d, "X2d expand + COUNT", "SELECT gb -> 'a', COUNT(x) FROM jsp.gb;").ok);
    // The same operators without GROUP BY keep working on the same table.
    CHECK(probe(d, "X2c expand control", "SELECT gb -> 'a' FROM jsp.gb;").aliases == std::set<std::string>{"b"});
}

// jsonb operators are not assignable expressions in UPDATE SET: the full-name
// operator dispatch rejects them cleanly.
TEST_CASE("integration::cpp::test_jsonb_support::update_set_jsonb_rejected") {
    auto config = test_create_config("/tmp/test_jsonb_support/update_set");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.u1 ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.u1 (a.b, x) VALUES (1, 9);").ok);

    CHECK_FALSE(probe(d, "X3 SET x = nav", "UPDATE jsp.u1 SET x = u1 -> 'a' ->> 'b' WHERE x = 9;").ok);
    CHECK_FALSE(probe(d, "X4 SET x = #>> path", "UPDATE jsp.u1 SET x = u1 #>> 'a.b' WHERE x = 9;").ok);
    CHECK_FALSE(probe(d, "X5 SET x = ?", "UPDATE jsp.u1 SET x = u1 ? 'x' WHERE x = 9;").ok);
    // Data untouched by the rejected updates.
    {
        auto p = probe(d, "X6 SELECT after rejects", "SELECT * FROM jsp.u1;");
        CHECK(p.ok);
        CHECK(p.rows == 1);
    }
}

// A jsonb navigation chain as an index expression is rejected like any other
// expression index element.
TEST_CASE("integration::cpp::test_jsonb_support::expression_index_jsonb") {
    auto config = test_create_config("/tmp/test_jsonb_support/expr_idx");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.ix ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.ix (a.b, x) VALUES (1, 9);").ok);

    CHECK_FALSE(probe(d, "X7 CREATE INDEX ((nav))", "CREATE INDEX jsonb_idx ON jsp.ix ((ix ->> 'x'));").ok);
    // Plain column index still works on a computing table with a registered schema.
    CHECK(probe(d, "X8 CREATE INDEX (x)", "CREATE INDEX plain_idx ON jsp.ix (x);").ok);
}

// A column reference as a jsonb key ('cr -> x') is a per-row dynamic key the
// flattened-column model cannot express: it must be a clean error in every
// build mode (it used to assert in Debug and "work" by accident in Release,
// where the compiled-out assert fell through to returning the column name).
TEST_CASE("integration::cpp::test_jsonb_support::columnref_key_rejected") {
    auto config = test_create_config("/tmp/test_jsonb_support/colref_key");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.cr ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.cr (a.b, x) VALUES (1, 9);").ok);

    CHECK_FALSE(probe(d, "X20 -> columnref key", "SELECT cr -> x FROM jsp.cr;").ok);
    CHECK_FALSE(probe(d, "X21 ->> columnref key", "SELECT cr ->> x FROM jsp.cr;").ok);
    CHECK_FALSE(probe(d, "X22 ? columnref key", "SELECT x FROM jsp.cr WHERE cr ? x;").ok);
    // Literal keys keep working.
    CHECK(probe(d, "X23 literal key control", "SELECT cr ->> 'x' FROM jsp.cr;").rows == 1);
}


// Chained (3+-table) JOINs: the transformer's binary left/right key sides do
// not align with the merged schema's inner-join stamps, so the expansion's
// side filter must fall back to side-blind matching instead of mis-reporting
// a present path as missing.
TEST_CASE("integration::cpp::test_jsonb_support::chained_join_contexts") {
    auto config = test_create_config("/tmp/test_jsonb_support/chained_join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.t1 ();").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.t2 ();").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.t3 ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.t1 (k) VALUES (1);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.t2 (k, a.b, a.c) VALUES (1, 10, 20);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.t3 (k) VALUES (1);").ok);

    CHECK(probe(d,
                "V1a expand middle table",
                "SELECT t2 -> 'a' FROM jsp.t1 JOIN jsp.t2 ON t1.k = t2.k JOIN jsp.t3 ON t2.k = t3.k;")
              .aliases == std::set<std::string>{"b", "c"});
    CHECK(probe(d,
                "V1c nav middle table",
                "SELECT t2 -> 'a' ->> 'b' FROM jsp.t1 JOIN jsp.t2 ON t1.k = t2.k JOIN jsp.t3 ON t2.k = t3.k;")
              .rows == 1);
    // Shared-name resolution across 3+ tables is a pre-existing engine
    // limitation (binary side stamps can't distinguish three tables): even a
    // plain qualified column on the outermost table is ambiguous. Pinned here
    // as clean errors, not crashes.
    CHECK_FALSE(probe(d,
                      "V1b delete middle table (shared 'k' survivors)",
                      "SELECT t2 - 'a' FROM jsp.t1 JOIN jsp.t2 ON t1.k = t2.k JOIN jsp.t3 ON t2.k = t3.k;")
                    .ok);
    // V1f flipped when qualified JOIN keys became side-aware (the chained-join
    // qualified-key-binding fix): a plain qualified column on the outermost
    // table now resolves.
    CHECK(probe(d,
                "V1f qualified col t3",
                "SELECT t3.k FROM jsp.t1 JOIN jsp.t2 ON t1.k = t2.k JOIN jsp.t3 ON t2.k = t3.k;")
              .rows == 1);
    CHECK(probe(d,
                "V1e qualified col t2 control",
                "SELECT t2.k FROM jsp.t1 JOIN jsp.t2 ON t1.k = t2.k JOIN jsp.t3 ON t2.k = t3.k;")
              .rows == 1);
}

// ---------------------------------------------------------------------------
// 15. BUG — silent wrong result AND data corruption (found by the differential
//     + oracle fuzzer). A per-row ABSENT leaf key extracts as NULL in a
//     projection but a WHERE / DELETE / UPDATE comparison reads the absent
//     flattened column as 0. Every predicate that 0 satisfies (= 0, > -1, < 1,
//     >= 0, ...) therefore wrongly matches the keyless rows — and DELETE/UPDATE
//     mutate them. These CHECKs pin the CURRENT (wrong) behavior; each
//     "correct:" comment states what a fixed engine must return. Flip them when
//     the coercion is fixed (absent must be SQL NULL, excluded by every compare).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::absent_key_coerced_to_zero") {
    auto config = test_create_config("/tmp/test_jsonb_support/absent_zero");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.co ();").ok);
    // id=1 has x=5, id=2 has NO x, id=3 has x=0.
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.co (id, x) VALUES (1, 5);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.co (id) VALUES (2);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.co (id, x) VALUES (3, 0);").ok);

    // Projection is correct: the keyless row (id=2) extracts as NULL.
    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "SELECT co ->> 'x' AS v FROM jsp.co WHERE id = 2;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).is_null()); // absent -> NULL in projection (correct)
    }

    // ...but WHERE coerces the absent column to 0: id=2 leaks into every
    // predicate that 0 satisfies.
    CHECK(probe(d, "G-eq0", "SELECT id FROM jsp.co WHERE co ->> 'x' = 0;").rows == 2);   // correct: 1 (id=3 only)
    CHECK(probe(d, "G-gtm1", "SELECT id FROM jsp.co WHERE co ->> 'x' > -1;").rows == 3); // correct: 2 (id=1,3)
    CHECK(probe(d, "G-lt1", "SELECT id FROM jsp.co WHERE co ->> 'x' < 1;").rows == 2);   // correct: 1 (id=3 only)
    CHECK(probe(d, "G-ge0", "SELECT id FROM jsp.co WHERE co ->> 'x' >= 0;").rows == 3);  // correct: 2 (id=1,3)
    // Predicates 0 does NOT satisfy correctly exclude the keyless row.
    CHECK(probe(d, "G-ne0", "SELECT id FROM jsp.co WHERE co ->> 'x' <> 0;").rows == 1);  // correct
    CHECK(probe(d, "G-eq5", "SELECT id FROM jsp.co WHERE co ->> 'x' = 5;").rows == 1);   // correct control

    // The root coercion is general (a plain absent-per-row column coerces too),
    // but jsonb is hit harder: a plain column has an IS NULL escape hatch to
    // filter the keyless rows back out, while IS NULL over a nav operand is
    // rejected outright — so for jsonb there is no workaround.
    CHECK(probe(d, "G-plainisnull", "SELECT id FROM jsp.co WHERE x IS NULL;").rows == 1);       // works for plain col
    CHECK_FALSE(probe(d, "G-navisnull", "SELECT id FROM jsp.co WHERE co ->> 'x' IS NULL;").ok); // rejected for nav

    // DELETE corruption: also removes the keyless row.
    {
        REQUIRE(probe(d, "setup", "CREATE TABLE jsp.cod ();").ok);
        REQUIRE(probe(d, "setup", "INSERT INTO jsp.cod (id, x) VALUES (1, 5);").ok);
        REQUIRE(probe(d, "setup", "INSERT INTO jsp.cod (id) VALUES (2);").ok);
        REQUIRE(probe(d, "setup", "INSERT INTO jsp.cod (id, x) VALUES (3, 0);").ok);
        CHECK(probe(d, "G-del", "DELETE FROM jsp.cod WHERE cod ->> 'x' = 0;").ok);
        // correct: survivors {1,2}; actual: only {1} — id=2 (no x) was wrongly deleted.
        CHECK(probe(d, "G-del-survivors", "SELECT id FROM jsp.cod;").rows == 1);
    }
    // UPDATE corruption: also clobbers the keyless row.
    {
        REQUIRE(probe(d, "setup", "CREATE TABLE jsp.cou ();").ok);
        REQUIRE(probe(d, "setup", "INSERT INTO jsp.cou (id, x) VALUES (1, 5);").ok);
        REQUIRE(probe(d, "setup", "INSERT INTO jsp.cou (id) VALUES (2);").ok);
        CHECK(probe(d, "G-upd", "UPDATE jsp.cou SET id = 99 WHERE cou ->> 'x' > -1;").ok);
        // correct: only id=1 -> 99 (one row); actual: BOTH rows -> 99.
        CHECK(probe(d, "G-upd-affected", "SELECT id FROM jsp.cou WHERE id = 99;").rows == 2);
    }
}

// ---------------------------------------------------------------------------
// 16. General GREEN coverage: value-level assertions over the supported operator
//     surface on a fully-populated table (every row has every key, so the
//     absent-key coercion above is not in play). Locks the correct results so a
//     regression in any operator is caught, not just a crash.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::supported_surface_green") {
    auto config = test_create_config("/tmp/test_jsonb_support/green");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.g ();").ok);
    REQUIRE(probe(d, "setup",
                  "INSERT INTO jsp.g (k, a.b, a.c, x) VALUES (1,10,11,100),(2,20,21,200),(3,30,31,300);")
                .ok);

    auto rows3 = [&](const char* sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    // Scalar / nested / path extract — three spellings agree, values exact.
    {
        auto cur = rows3("SELECT g ->> 'x' AS v FROM jsp.g ORDER BY k;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        CHECK(cur->value(0, 0).value<int64_t>() == 100);
        CHECK(cur->value(0, 1).value<int64_t>() == 200);
        CHECK(cur->value(0, 2).value<int64_t>() == 300);
    }
    {
        auto cur = rows3("SELECT g -> 'a' ->> 'b' AS v FROM jsp.g ORDER BY k;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 10);
        CHECK(cur->value(0, 2).value<int64_t>() == 30);
    }
    {
        auto pg = rows3("SELECT g #>> '{a,c}' AS v FROM jsp.g ORDER BY k;");
        auto dot = rows3("SELECT g #>> 'a.c' AS v FROM jsp.g ORDER BY k;");
        REQUIRE(pg->is_success());
        REQUIRE(dot->is_success());
        CHECK(pg->value(0, 0).value<int64_t>() == 11);
        CHECK(pg->value(0, 1).value<int64_t>() == 21);
        CHECK(pg->value(0, 2).value<int64_t>() == 31);
        CHECK(dot->value(0, 0).value<int64_t>() == 11);
        CHECK(dot->value(0, 1).value<int64_t>() == 21);
        CHECK(dot->value(0, 2).value<int64_t>() == 31);
    }
    // Arithmetic over a nav chain.
    {
        auto cur = rows3("SELECT (g ->> 'x') + 1 AS v FROM jsp.g ORDER BY k;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 101);
        CHECK(cur->value(0, 2).value<int64_t>() == 301);
    }
    // WHERE nav equality selects the exact row.
    CHECK(probe(d, "GR-where", "SELECT k FROM jsp.g WHERE g -> 'a' ->> 'b' = 20;").rows == 1);
    // Existence (leaf) — three spellings, all three rows.
    CHECK(probe(d, "GR-ex", "SELECT k FROM jsp.g WHERE g ? 'x';").rows == 3);
    CHECK(probe(d, "GR-exall", "SELECT k FROM jsp.g WHERE g ?& '{x,k}';").rows == 3);
    CHECK(probe(d, "GR-exany", "SELECT k FROM jsp.g WHERE g ?| '{x,k}';").rows == 3);
    // Expand two ways -> same column set.
    CHECK(probe(d, "GR-exp", "SELECT g -> 'a' FROM jsp.g;").aliases == std::set<std::string>{"b", "c"});
    CHECK(probe(d, "GR-exp2", "SELECT g #> '{a}' FROM jsp.g;").aliases == std::set<std::string>{"b", "c"});
    // Delete key / delete path -> correct survivor column sets.
    CHECK(probe(d, "GR-del", "SELECT g - 'x' FROM jsp.g;").aliases == std::set<std::string>{"k", "a/b", "a/c"});
    CHECK(probe(d, "GR-delp", "SELECT g #- 'a.b' FROM jsp.g;").aliases == std::set<std::string>{"k", "a/c", "x"});
    // Aggregate over a nav wrapped in arithmetic (the supported aggregate form).
    {
        auto cur = rows3("SELECT SUM((g ->> 'x') + 0) AS s FROM jsp.g;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 600);
    }
    // ORDER BY a nav expression (descending) — exact order.
    {
        auto cur = rows3("SELECT x FROM jsp.g ORDER BY (g ->> 'x') + 0 DESC;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        CHECK(cur->value(0, 0).value<int64_t>() == 300);
        CHECK(cur->value(0, 1).value<int64_t>() == 200);
        CHECK(cur->value(0, 2).value<int64_t>() == 100);
    }
}

// ---------------------------------------------------------------------------
// 17. BUG — jsonb-specific silent wrong result (found by the diverse-lens
//     workflow, compose slice). A cast on a jsonb NAV operand that is then used
//     as an operand of binary arithmetic reads uninitialized memory: the query
//     SUCCEEDS but returns a garbage (pointer-like, per-run-varying) value. Each
//     piece works alone — the cast alone, the arithmetic alone, and the SAME
//     cast-in-arithmetic on a PLAIN column — so the defect is specific to the
//     jsonb nav operand under cast+arithmetic. Pins the CURRENT (wrong) behavior
//     by asserting the result is not the correct value; flip to == 11 on fix.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::cast_nav_in_arithmetic_garbage") {
    auto config = test_create_config("/tmp/test_jsonb_support/cast_arith");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.t ();").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.t (x) VALUES (10);").ok);

    auto run = [&](const char* sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    // Controls that are all correct.
    { auto c = run("SELECT (t ->> 'x')::bigint AS v FROM jsp.t;"); REQUIRE(c->is_success());
      CHECK(c->value(0, 0).value<int64_t>() == 10); }        // cast alone
    { auto c = run("SELECT (t ->> 'x') + 1 AS v FROM jsp.t;"); REQUIRE(c->is_success());
      CHECK(c->value(0, 0).value<int64_t>() == 11); }        // arithmetic alone
    { auto c = run("SELECT (x)::bigint + 1 AS v FROM jsp.t;"); REQUIRE(c->is_success());
      CHECK(c->value(0, 0).value<int64_t>() == 11); }        // plain-column cast-in-arith (proves jsonb-specific)
    // BUG: cast on the nav operand inside arithmetic — succeeds, returns garbage.
    {
        auto c = run("SELECT (t ->> 'x')::bigint + 1 AS v FROM jsp.t;");
        REQUIRE(c->is_success());
        CHECK(c->value(0, 0).value<int64_t>() != 11);        // correct: == 11
    }
}

// ---------------------------------------------------------------------------
// 18. BUG — jsonb-specific silent wrong result (workflow joins slice). In a
//     chained (3+-table) JOIN where a leaf column name is shared across sides,
//     a jsonb nav binds the key to the LEFT subtree and silently returns the
//     wrong table's value, while the equivalent PLAIN qualified column resolves
//     to the aliased table (side-aware). This is the documented side-blind
//     fallback (commit 38af2927) surfacing as a silent wrong VALUE rather than
//     the acknowledged ambiguity error. Pins current behavior.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_jsonb_support::chained_join_shared_name_side_blind") {
    auto config = test_create_config("/tmp/test_jsonb_support/chained_shared");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(probe(d, "setup", "CREATE DATABASE jsp;").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.l ();").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.m ();").ok);
    REQUIRE(probe(d, "setup", "CREATE TABLE jsp.r ();").ok);
    // Each side carries its own 'v': l.v=10, m.v=20, r.v=30.
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.l (k, v) VALUES (1, 10);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.m (k, v) VALUES (1, 20);").ok);
    REQUIRE(probe(d, "setup", "INSERT INTO jsp.r (k, v) VALUES (1, 30);").ok);

    auto run = [&](const char* sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    // Plain qualified column is side-aware: m.v resolves to 20.
    {
        auto c = run("SELECT m.v FROM jsp.l JOIN jsp.m ON l.k = m.k JOIN jsp.r ON m.k = r.k;");
        REQUIRE(c->is_success());
        CHECK(c->value(0, 0).value<int64_t>() == 20);
    }
    // A 2-table jsonb nav is also correct: m ->> 'v' resolves to 20.
    {
        auto c = run("SELECT m ->> 'v' AS v FROM jsp.l JOIN jsp.m ON l.k = m.k;");
        REQUIRE(c->is_success());
        CHECK(c->value(0, 0).value<int64_t>() == 20);
    }
    // BUG: the 3-table jsonb nav is side-blind — returns l.v (10), not m.v (20).
    {
        auto c = run("SELECT m ->> 'v' AS v FROM jsp.l JOIN jsp.m ON l.k = m.k JOIN jsp.r ON m.k = r.k;");
        REQUIRE(c->is_success());
        CHECK(c->value(0, 0).value<int64_t>() == 10);   // correct: == 20
    }
}


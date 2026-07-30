#include "test_config.hpp"
#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <string>
#include <vector>

// Three-valued logic in the IN-MEMORY predicate evaluator (simple_predicate / function_predicate),
// the path taken when a WHERE / join / DML condition is NOT pushed down to the storage scan --
// i.e. it involves arithmetic, a function (LIKE / regex), or a plain-list IN, optionally under NOT.
//
// A NULL operand makes a comparison UNKNOWN, not FALSE; the two diverge under NOT. A 2VL evaluator
// that collapses UNKNOWN to FALSE lets `NOT (...)` resurrect a NULL row. These tests pin the SQL
// answers for every combinator (=, <>, <, >, arithmetic, LIKE, IN, AND, OR, NOT) over a NULL row,
// across WHERE, DELETE, UPDATE, join, and CHECK.

namespace {
    using test_helpers::exec;

    // Sorted list of column-0 (id) values of a successful query.
    template<typename D>
    std::vector<int64_t> ids(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        std::vector<int64_t> out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            auto v = c->value(0, r);
            REQUIRE_FALSE(v.is_null());
            out.push_back(v.template value<int64_t>());
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    using L = std::vector<int64_t>;

    // Seed t(id, x, s) with a NULL row (id=2) among numeric + text values.
    template<typename D>
    void seed(D* d) {
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT, s TEXT);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x, s) VALUES (1, 5, 'apple');"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x, s) VALUES (2, NULL, NULL);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x, s) VALUES (3, 0, 'banana');"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x, s) VALUES (4, 10, 'cherry');"));
    }
} // namespace

// -------------------------------------------------------------------------------------------------
//  Arithmetic operand under NOT -- the in-memory path (arithmetic is not pushed down).
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::pred3vl::arithmetic_not") {
    auto config = test_helpers::make_test_config(test_temp_path("p3vl/arith_not"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // positive controls: only the TRUE row(s), NULL never matches.
    CHECK(ids(d, "SELECT id FROM m.t WHERE x + 1 = 6;") == L{1});
    CHECK(ids(d, "SELECT id FROM m.t WHERE x * 2 = 20;") == L{4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE 100 - x = 95;") == L{1});

    // NOT over an arithmetic comparison: NULL stays UNKNOWN -> excluded, not resurrected.
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x + 1 = 6);") == L{3, 4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x - 1 = 4);") == L{3, 4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x * 2 = 20);") == L{1, 3});
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (100 - x = 95);") == L{3, 4});
    // <> under NOT collapses to =, NULL still excluded.
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x + 1 <> 6);") == L{1});
    // ordering comparisons under NOT.
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x + 1 > 6);") == L{1, 3}); // x+1<=6 -> x in {5,0}
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x + 1 <= 6);") == L{4});   // x+1>6 -> x=10
}

// -------------------------------------------------------------------------------------------------
//  Function (LIKE) operand under NOT -- function_predicate path.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::pred3vl::like_not") {
    auto config = test_helpers::make_test_config(test_temp_path("p3vl/like_not"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(ids(d, "SELECT id FROM m.t WHERE s LIKE 'a%';") == L{1});
    CHECK(ids(d, "SELECT id FROM m.t WHERE s LIKE '%a%';") == L{1, 3}); // apple, banana

    // NOT LIKE / NOT (LIKE): the NULL text row is UNKNOWN, never resurrected.
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (s LIKE 'a%');") == L{3, 4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE s NOT LIKE 'a%';") == L{3, 4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (s LIKE '%a%');") == L{4}); // only cherry lacks 'a'
}

// -------------------------------------------------------------------------------------------------
//  IN / NOT IN over a plain list -- union_or(eq) / union_and(ne), and NOT(IN).
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::pred3vl::in_not") {
    auto config = test_helpers::make_test_config(test_temp_path("p3vl/in_not"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(ids(d, "SELECT id FROM m.t WHERE x IN (5, 10);") == L{1, 4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE x NOT IN (5, 10);") == L{3}); // 0 only; NULL excluded
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x IN (5, 10));") == L{3});
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x IN (0));") == L{1, 4});
    // NULL element in the list makes an otherwise-FALSE membership UNKNOWN (still excluded here).
    CHECK(ids(d, "SELECT id FROM m.t WHERE x IN (5, 10, NULL);") == L{1, 4});
}

// -------------------------------------------------------------------------------------------------
//  NOT distributed over AND / OR (De Morgan) with a NULL operand.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::pred3vl::not_over_and_or") {
    auto config = test_helpers::make_test_config(test_temp_path("p3vl/demorgan"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // NOT (P OR Q): id2 is (U OR U)=U -> NOT U -> excluded (2VL wrongly includes it).
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x + 1 = 6 OR s LIKE 'b%');") == L{4});
    // NOT (P AND Q): id2 is (U AND FALSE)=FALSE -> NOT TRUE -> included.
    CHECK(ids(d, "SELECT id FROM m.t WHERE NOT (x + 1 = 6 AND id = 1);") == L{2, 3, 4});
    // positive control (no NOT): OR of the TRUE rows.
    CHECK(ids(d, "SELECT id FROM m.t WHERE x + 1 = 6 OR s LIKE 'b%';") == L{1, 3});
    // AND mixing arithmetic with a plain column.
    CHECK(ids(d, "SELECT id FROM m.t WHERE x + 1 = 6 AND id = 1;") == L{1});
    CHECK(ids(d, "SELECT id FROM m.t WHERE x + 1 = 6 AND id = 2;") == L{});
}

// -------------------------------------------------------------------------------------------------
//  DELETE / UPDATE consumers: only definitely-TRUE rows are affected.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::pred3vl::delete_update") {
    auto config = test_helpers::make_test_config(test_temp_path("p3vl/dml"), true, true);
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        seed(d);

        // DELETE WHERE NOT(arith): removes the definitely-TRUE rows {3,4}; NULL row survives.
        REQUIRE(okq(d, "DELETE FROM m.t WHERE NOT (x + 1 = 6);"));
        CHECK(ids(d, "SELECT id FROM m.t;") == L{1, 2});
        CHECK(ids(d, "SELECT id FROM m.t WHERE x IS NULL;") == L{2});
    }
    {
        test_spaces space(config); // restart: recovery keeps the same rows.
        auto* d = space.dispatcher();
        CHECK(ids(d, "SELECT id FROM m.t;") == L{1, 2});
    }

    auto cfg2 = test_helpers::make_test_config(test_temp_path("p3vl/dml_upd"));
    test_spaces space(cfg2);
    auto* d = space.dispatcher();
    seed(d);
    // UPDATE WHERE NOT(LIKE): updates only {3,4}; NULL row untouched.
    REQUIRE(okq(d, "UPDATE m.t SET x = 999 WHERE NOT (s LIKE 'a%');"));
    CHECK(ids(d, "SELECT id FROM m.t WHERE x = 999;") == L{3, 4});
    CHECK(ids(d, "SELECT id FROM m.t WHERE x IS NULL;") == L{2}); // id=2 still NULL, not updated
}

// -------------------------------------------------------------------------------------------------
//  CHECK constraint consumer: a NULL operand is UNKNOWN, which PASSES a CHECK (permits), even
//  under NOT. Only a definitely-FALSE row is rejected.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::pred3vl::check_not") {
    auto config = test_helpers::make_test_config(test_temp_path("p3vl/check"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));

    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "ALTER TABLE m.t ADD CONSTRAINT cc CHECK (NOT (x = 5));"));
    CHECK(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 0);"));       // NOT(FALSE)=TRUE -> ok
    CHECK_FALSE(okq(d, "INSERT INTO m.t (id, x) VALUES (2, 5);")); // NOT(TRUE)=FALSE -> rejected
    CHECK(okq(d, "INSERT INTO m.t (id, x) VALUES (3, NULL);"));    // NOT(UNKNOWN)=UNKNOWN -> ok
    CHECK(ids(d, "SELECT id FROM m.t;") == L{1, 3});

    // Plain comparison CHECK: NULL passes, FALSE rejected.
    REQUIRE(okq(d, "CREATE TABLE m.u (id INT, x BIGINT);"));
    REQUIRE(okq(d, "ALTER TABLE m.u ADD CONSTRAINT cc CHECK (x <> 5);"));
    CHECK(okq(d, "INSERT INTO m.u (id, x) VALUES (1, 0);"));
    CHECK_FALSE(okq(d, "INSERT INTO m.u (id, x) VALUES (2, 5);"));
    CHECK(okq(d, "INSERT INTO m.u (id, x) VALUES (3, NULL);"));
    CHECK(ids(d, "SELECT id FROM m.u;") == L{1, 3});
}

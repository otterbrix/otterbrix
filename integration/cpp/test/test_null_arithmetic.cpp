#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

// SQL NULL propagation through arithmetic (three-valued logic, arithmetic half).
//
// Any arithmetic operation with a NULL operand yields NULL: `NULL + 1` is NULL, not 1;
// `5 / NULL` is NULL, not a division-by-zero trap. Two independent engines evaluate
// arithmetic and both must agree:
//   - the scalar logical_value_t path (post-aggregates, CASE, constant folding),
//   - the vectorized components/vector/arithmetic.cpp path (SELECT list, UPDATE SET).
//
// Before the fix, the scalar op<> template substituted a zero for the NULL operand — so
// `NULL + 1` was 1 and `5 / NULL` divided by zero (a SIGFPE on integer types) — and the
// vector kernels never intersected the operand validity masks into the output, so a NULL
// row's meaningless payload byte (0) was published as a real, valid result.

namespace {

    using opt = std::optional<int64_t>;

    template<typename D>
    bool ok(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        return c && c->is_success();
    }

    template<typename D>
    size_t rows(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        REQUIRE(c);
        REQUIRE(c->is_success());
        return c->size();
    }

    // Read column `col` of every row as an optional<int64_t> (nullopt == SQL NULL).
    template<typename D>
    std::vector<opt> read_col(D* d, const std::string& sql, uint64_t col = 0) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        std::vector<opt> out;
        REQUIRE(c);
        REQUIRE(c->is_success());
        for (uint64_t r = 0; r < c->size(); ++r) {
            auto v = c->value(col, r);
            out.push_back(v.is_null() ? opt{} : opt{v.template value<int64_t>()});
        }
        return out;
    }

    // id=1 -> x=5 ; id=2 -> x NULL ; id=3 -> x=0
    template<typename D>
    void seed(D* d, const std::string& table) {
        REQUIRE(ok(d, "CREATE TABLE " + table + " (id INT, x BIGINT);"));
        REQUIRE(ok(d, "INSERT INTO " + table + " (id, x) VALUES (1, 5);"));
        REQUIRE(ok(d, "INSERT INTO " + table + " (id, x) VALUES (2, NULL);"));
        REQUIRE(ok(d, "INSERT INTO " + table + " (id, x) VALUES (3, 0);"));
    }

} // namespace

TEST_CASE("integration::cpp::null_arith::projection_propagates_null") {
    auto config = test_create_config(integration_fixture_path("test_null_arith/proj"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE na;"));
    seed(d, "na.t");

    // The NULL row (id=2) must project NULL for every arithmetic result.
    CHECK(read_col(d, "SELECT x + 1 FROM na.t ORDER BY id;") == std::vector<opt>{6, {}, 1});
    CHECK(read_col(d, "SELECT x - 1 FROM na.t ORDER BY id;") == std::vector<opt>{4, {}, -1});
    CHECK(read_col(d, "SELECT x * 2 FROM na.t ORDER BY id;") == std::vector<opt>{10, {}, 0});
    CHECK(read_col(d, "SELECT -x FROM na.t ORDER BY id;") == std::vector<opt>{-5, {}, 0});
    CHECK(read_col(d, "SELECT 100 - x FROM na.t ORDER BY id;") == std::vector<opt>{95, {}, 100});
    CHECK(read_col(d, "SELECT x + x FROM na.t ORDER BY id;") == std::vector<opt>{10, {}, 0});
}

TEST_CASE("integration::cpp::null_arith::division_by_null_does_not_crash") {
    auto config = test_create_config(integration_fixture_path("test_null_arith/div"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE na;"));
    seed(d, "na.t");

    // Division by zero is an ERROR, not a NULL
    CHECK_FALSE(ok(d, "SELECT 10 / x FROM na.t ORDER BY id;"));
    CHECK_FALSE(ok(d, "SELECT 10 % x FROM na.t ORDER BY id;"));
    // x / <non-null> keeps the NULL operand NULL.
    CHECK(read_col(d, "SELECT x / 2 FROM na.t ORDER BY id;") == std::vector<opt>{2, {}, 0});
}

TEST_CASE("integration::cpp::null_arith::arithmetic_in_where_excludes_null") {
    auto config = test_create_config(integration_fixture_path("test_null_arith/where"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE na;"));
    seed(d, "na.t");

    // x=0 -> 0+1=1 TRUE ; x NULL -> NULL+1=NULL, =1 is UNKNOWN (excluded) ; x=5 -> 6=1 FALSE.
    // A UNKNOWN comparison and a FALSE comparison both exclude the row here, so this positive
    // form already isolates the arithmetic NULL propagation. The NOT form (where UNKNOWN and
    // FALSE diverge) is a property of the three-valued predicate evaluator, covered by the
    // in-memory predicate 3VL tests, not by arithmetic.
    CHECK(rows(d, "SELECT id FROM na.t WHERE x + 1 = 1;") == 1); // id=3 only
    CHECK(rows(d, "SELECT id FROM na.t WHERE x * 2 = 0;") == 1); // id=3 only
}

TEST_CASE("integration::cpp::null_arith::update_set_expression_keeps_null") {
    auto config = test_create_config(integration_fixture_path("test_null_arith/upd"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE na;"));
    seed(d, "na.t");

    // UPDATE t SET x = x + 1 must leave the NULL row NULL (NULL + 1 is NULL), not write a 1.
    REQUIRE(ok(d, "UPDATE na.t SET x = x + 1;"));
    CHECK(read_col(d, "SELECT x FROM na.t ORDER BY id;") == std::vector<opt>{6, {}, 1});
    CHECK(rows(d, "SELECT id FROM na.t WHERE x IS NULL;") == 1); // id=2 still NULL
}

TEST_CASE("integration::cpp::null_arith::aggregate_arithmetic_propagates_null") {
    auto config = test_create_config(integration_fixture_path("test_null_arith/agg"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE na;"));
    // group 1: a has values, b all NULL ; group 2: both have values.
    REQUIRE(ok(d, "CREATE TABLE na.g (k INT, a BIGINT, b BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO na.g (k, a, b) VALUES (1, 3, NULL);"));
    REQUIRE(ok(d, "INSERT INTO na.g (k, a, b) VALUES (1, 4, NULL);"));
    REQUIRE(ok(d, "INSERT INTO na.g (k, a, b) VALUES (2, 5, 10);"));
    REQUIRE(ok(d, "INSERT INTO na.g (k, a, b) VALUES (2, 5, 20);"));

    // SUM(b) over the all-NULL group is NULL, so SUM(a)+SUM(b) is NULL for group 1, 40 for group 2.
    CHECK(read_col(d, "SELECT SUM(a) + SUM(b) FROM na.g GROUP BY k ORDER BY k;", 0) == std::vector<opt>{{}, 40});
    // SUM(a) / SUM(b): group 1 divides by a NULL SUM -> NULL, not a crash; group 2 -> 10/30 = 0.
    CHECK(read_col(d, "SELECT SUM(a) / SUM(b) FROM na.g GROUP BY k ORDER BY k;", 0) == std::vector<opt>{{}, 0});
}

TEST_CASE("integration::cpp::null_arith::literal_null_folds_to_null") {
    auto config = test_create_config(integration_fixture_path("test_null_arith/fold"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE na;"));
    seed(d, "na.t");

    // A constant-folded arithmetic with a NULL literal is NULL, so the comparison is UNKNOWN
    // for every row and nobody is selected.
    CHECK(rows(d, "SELECT id FROM na.t WHERE 1 + NULL = 1;") == 0);
    CHECK(rows(d, "SELECT id FROM na.t WHERE x = 1 + NULL;") == 0);
}

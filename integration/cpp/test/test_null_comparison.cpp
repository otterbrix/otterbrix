#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

// SQL three-valued logic for value comparisons that are NOT part of the WHERE predicate tree:
// CASE WHEN, GROUP BY CASE, HAVING, and CHECK constraints. A comparison over a NULL operand is
// UNKNOWN, so a CASE WHEN falls through, a HAVING group is dropped, and a CHECK passes (only a
// definitely-FALSE check is a violation).
//
// Before the fix these sites answered through logical_value_t::compare(), whose total order made
// NULL == anything TRUE (or, once ordering put NULLs last, some other definite answer) -- so
// `CASE WHEN x = 1` matched a NULL x, and `CHECK (x > 0)` rejected a NULL x.
//
// operator== is also exercised here: it is the structural equality used for DISTINCT, so two NULLs
// must collapse to one group while a NULL and a value stay distinct.

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

    template<typename D>
    void seed(D* d, const std::string& table) {
        REQUIRE(ok(d, "CREATE TABLE " + table + " (id INT, x BIGINT);"));
        REQUIRE(ok(d, "INSERT INTO " + table + " (id, x) VALUES (1, 5);"));
        REQUIRE(ok(d, "INSERT INTO " + table + " (id, x) VALUES (2, NULL);"));
        REQUIRE(ok(d, "INSERT INTO " + table + " (id, x) VALUES (3, 1);"));
    }
} // namespace

TEST_CASE("integration::cpp::null_cmp::case_when_falls_through_on_null") {
    auto config = test_create_config("/tmp/test_null_cmp/case");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE nc;"));
    seed(d, "nc.t"); // rows: x = 5, NULL, 1

    // SUM over a CASE that scores 1 when the WHEN matches. A NULL operand makes the WHEN UNKNOWN,
    // which (like FALSE) falls through to the ELSE 0 -- so a NULL row never scores.
    //   x = 1 : only the x=1 row scores            -> 1
    //   x <> 1: only the x=5 row scores            -> 1  (NULL does NOT flip in under <>)
    //   x >= 1: the x=5 and x=1 rows score         -> 2  (NULL excluded)
    //   x <= 1: only the x=1 row scores            -> 1
    CHECK(read_col(d, "SELECT SUM(CASE WHEN x = 1 THEN 1 ELSE 0 END) FROM nc.t;") == std::vector<opt>{1});
    CHECK(read_col(d, "SELECT SUM(CASE WHEN x <> 1 THEN 1 ELSE 0 END) FROM nc.t;") == std::vector<opt>{1});
    CHECK(read_col(d, "SELECT SUM(CASE WHEN x >= 1 THEN 1 ELSE 0 END) FROM nc.t;") == std::vector<opt>{2});
    CHECK(read_col(d, "SELECT SUM(CASE WHEN x <= 1 THEN 1 ELSE 0 END) FROM nc.t;") == std::vector<opt>{1});
}

TEST_CASE("integration::cpp::null_cmp::having_drops_unknown_group") {
    auto config = test_create_config("/tmp/test_null_cmp/having");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE nc;"));
    // group 1: all NULL (SUM NULL) ; group 2: sums to 8.
    REQUIRE(ok(d, "CREATE TABLE nc.g (k INT, x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO nc.g (k, x) VALUES (1, NULL);"));
    REQUIRE(ok(d, "INSERT INTO nc.g (k, x) VALUES (1, NULL);"));
    REQUIRE(ok(d, "INSERT INTO nc.g (k, x) VALUES (2, 3);"));
    REQUIRE(ok(d, "INSERT INTO nc.g (k, x) VALUES (2, 5);"));

    // HAVING SUM(x) = 8: group 1's SUM is NULL -> UNKNOWN -> dropped; group 2 kept.
    CHECK(rows(d, "SELECT k FROM nc.g GROUP BY k HAVING SUM(x) = 8;") == 1);
    // HAVING SUM(x) <> 8: group 1 UNKNOWN -> still dropped (NOT does not resurrect it); group 2 FALSE.
    CHECK(rows(d, "SELECT k FROM nc.g GROUP BY k HAVING SUM(x) <> 8;") == 0);
    // HAVING SUM(x) > 0: group 1 UNKNOWN dropped; group 2 (8>0) kept.
    CHECK(rows(d, "SELECT k FROM nc.g GROUP BY k HAVING SUM(x) > 0;") == 1);
}

TEST_CASE("integration::cpp::null_cmp::check_constraint_passes_null") {
    auto config = test_create_config("/tmp/test_null_cmp/check");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE nc;"));
    REQUIRE(ok(d, "CREATE TABLE nc.ck (id INT, x BIGINT);"));
    REQUIRE(ok(d, "ALTER TABLE nc.ck ADD CONSTRAINT ck_pos CHECK (x > 0);"));

    CHECK(ok(d, "INSERT INTO nc.ck (id, x) VALUES (1, 5);"));        // 5 > 0 TRUE  -> accepted
    CHECK_FALSE(ok(d, "INSERT INTO nc.ck (id, x) VALUES (2, -1);")); // -1 > 0 FALSE -> rejected
    CHECK(ok(d, "INSERT INTO nc.ck (id, x) VALUES (3, NULL);"));     // NULL > 0 UNKNOWN -> accepted
    // Only rows 1 and 3 made it in.
    CHECK(rows(d, "SELECT id FROM nc.ck;") == 2);
    CHECK(rows(d, "SELECT id FROM nc.ck WHERE x IS NULL;") == 1);
}

TEST_CASE("integration::cpp::null_cmp::distinct_collapses_nulls") {
    auto config = test_create_config("/tmp/test_null_cmp/distinct");
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE nc;"));
    REQUIRE(ok(d, "CREATE TABLE nc.t (id INT, x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO nc.t (id, x) VALUES (1, 5);"));
    REQUIRE(ok(d, "INSERT INTO nc.t (id, x) VALUES (2, NULL);"));
    REQUIRE(ok(d, "INSERT INTO nc.t (id, x) VALUES (3, NULL);"));
    REQUIRE(ok(d, "INSERT INTO nc.t (id, x) VALUES (4, 5);"));

    // DISTINCT treats all NULLs as one value: {5, NULL} -> 2 distinct rows.
    CHECK(rows(d, "SELECT DISTINCT x FROM nc.t;") == 2);
}

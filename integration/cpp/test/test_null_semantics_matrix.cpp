#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

// SQL-level coverage of three-valued NULL logic across column type, table shape, storage, and index.
// Only forms fixed on this branch are asserted; NOT over a non-column operand and plain-list NOT IN
// are still two-valued here and belong to the predicate-3VL unit.

namespace {

    using opt = std::optional<int64_t>;
    using test_helpers::exec;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    template<typename D>
    long nrows(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        return static_cast<long>(c->size());
    }

    template<typename D>
    std::vector<opt> coli(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        std::vector<opt> out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            auto v = c->value(0, r);
            out.push_back(v.is_null() ? opt{} : opt{v.template value<int64_t>()});
        }
        return out;
    }

    template<typename D>
    opt scal(D* d, const std::string& sql) {
        auto v = coli(d, sql);
        INFO(sql);
        REQUIRE(v.size() == 1);
        return v.front();
    }

    template<typename D>
    double scald(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        auto v = c->value(0, 0);
        REQUIRE_FALSE(v.is_null());
        return v.template value<double>();
    }

    // x is seeded {5, NULL, 0, 10, NULL} at ids {1..5} -- every CHECK below reads against this fixed set.
    template<typename D>
    void comparison_battery(D* d, const std::string& t) {
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 0;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 5;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 10;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 7;") == 0);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x <> 0;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x <> 5;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x > 0;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x >= 0;") == 3);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x < 10;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x <= 10;") == 3);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x < 0;") == 0);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x > 10;") == 0);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE NOT (x = 0);") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE NOT (x = 5);") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE NOT (x <> 0);") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE NOT (x >= 0);") == 0);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE NOT (x < 10);") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE NOT (x > 0);") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x IS NULL;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x IS NOT NULL;") == 3);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x >= 0 AND x <= 5;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x > 0 AND x < 10;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x >= 0 AND id = 3;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x >= 0 AND id = 2;") == 0);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x IS NULL AND id = 2;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 0 OR x = 10;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 7 OR x = 5;") == 1);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 0 OR id = 2;") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x IS NULL OR x = 0;") == 3);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x = 0 OR x IS NULL;") == 3);
        // aggregates (NULLs skipped; COUNT(*) counts rows)
        CHECK(scal(d, "SELECT COUNT(x) FROM " + t + ";") == opt{3});
        CHECK(scal(d, "SELECT COUNT(*) FROM " + t + ";") == opt{5});
        CHECK(scal(d, "SELECT COUNT(DISTINCT x) FROM " + t + ";") == opt{3});
        CHECK(scal(d, "SELECT SUM(x) FROM " + t + ";") == opt{15});
        CHECK(scal(d, "SELECT MIN(x) FROM " + t + ";") == opt{0});
        CHECK(scal(d, "SELECT MAX(x) FROM " + t + ";") == opt{10});
    // AVG on an integer column stays integer-typed, read here as int64.
        CHECK(scal(d, "SELECT AVG(x) FROM " + t + ";") == opt{5});
    }

    template<typename D>
    void seed_numeric(D* d, const std::string& t) {
        REQUIRE(okq(d, "INSERT INTO " + t + " (id, x) VALUES (1, 5);"));
        REQUIRE(okq(d, "INSERT INTO " + t + " (id, x) VALUES (2, NULL);"));
        REQUIRE(okq(d, "INSERT INTO " + t + " (id, x) VALUES (3, 0);"));
        REQUIRE(okq(d, "INSERT INTO " + t + " (id, x) VALUES (4, 10);"));
        REQUIRE(okq(d, "INSERT INTO " + t + " (id, x) VALUES (5, NULL);"));
    }

} // namespace

TEST_CASE("integration::cpp::null_matrix::comparisons_bigint") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/cmp_bigint"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    seed_numeric(d, "m.t");
    comparison_battery(d, "m.t");
}

TEST_CASE("integration::cpp::null_matrix::comparisons_integer") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/cmp_int"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x INTEGER);"));
    seed_numeric(d, "m.t");
    comparison_battery(d, "m.t");
}

TEST_CASE("integration::cpp::null_matrix::comparisons_double") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/cmp_double"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x DOUBLE PRECISION);"));
    seed_numeric(d, "m.t");
    // DOUBLE isn't read back as int, so this inlines row-count/aggregate checks instead of reusing
    // comparison_battery.
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x = 0;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x <> 0;") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x >= 0;") == 3);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE NOT (x = 0);") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE NOT (x >= 0);") == 0);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x IS NULL;") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x IS NOT NULL;") == 3);
    CHECK(scal(d, "SELECT COUNT(x) FROM m.t;") == opt{3});
    CHECK(scal(d, "SELECT COUNT(*) FROM m.t;") == opt{5});
    CHECK(scald(d, "SELECT SUM(x) FROM m.t;") == Catch::Approx(15.0));
    CHECK(scald(d, "SELECT MIN(x) FROM m.t;") == Catch::Approx(0.0));
    CHECK(scald(d, "SELECT MAX(x) FROM m.t;") == Catch::Approx(10.0));
    CHECK(scald(d, "SELECT AVG(x) FROM m.t;") == Catch::Approx(5.0));
}

TEST_CASE("integration::cpp::null_matrix::comparisons_text") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/cmp_text"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, s TEXT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, s) VALUES (1, 'ann');"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, s) VALUES (2, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, s) VALUES (3, 'bob');"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, s) VALUES (4, 'cat');"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, s) VALUES (5, NULL);"));

    CHECK(nrows(d, "SELECT id FROM m.t WHERE s = 'ann';") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s <> 'ann';") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s > 'bob';") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s >= 'bob';") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s < 'bob';") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE NOT (s = 'ann');") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE NOT (s <> 'ann');") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s IS NULL;") == 2);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s IS NOT NULL;") == 3);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE s = 'ann' OR id = 2;") == 2);
    CHECK(scal(d, "SELECT COUNT(s) FROM m.t;") == opt{3});
    CHECK(scal(d, "SELECT COUNT(*) FROM m.t;") == opt{5});
}

TEST_CASE("integration::cpp::null_matrix::comparisons_computing_table") {
    // Schemaless table: an omitted column reads as NULL.
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/cmp_comp"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.c ();"));
    REQUIRE(okq(d, "INSERT INTO m.c (id, x) VALUES (1, 5);"));
    REQUIRE(okq(d, "INSERT INTO m.c (id) VALUES (2);"));
    REQUIRE(okq(d, "INSERT INTO m.c (id, x) VALUES (3, 0);"));
    REQUIRE(okq(d, "INSERT INTO m.c (id, x) VALUES (4, 10);"));
    REQUIRE(okq(d, "INSERT INTO m.c (id) VALUES (5);"));
    comparison_battery(d, "m.c");
}

TEST_CASE("integration::cpp::null_matrix::comparisons_disk_and_restart") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/cmp_disk"));
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
        seed_numeric(d, "m.t");
        comparison_battery(d, "m.t");
    }
    {
        test_spaces space(config); // restart: state recovered from disk/WAL
        auto* d = space.dispatcher();
        comparison_battery(d, "m.t");
    }
}

TEST_CASE("integration::cpp::null_matrix::arithmetic_projection") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/arith_proj"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (2, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (3, 0);"));

    CHECK(coli(d, "SELECT x + 1 FROM m.t ORDER BY id;") == std::vector<opt>{6, {}, 1});
    CHECK(coli(d, "SELECT x - 1 FROM m.t ORDER BY id;") == std::vector<opt>{4, {}, -1});
    CHECK(coli(d, "SELECT x * 2 FROM m.t ORDER BY id;") == std::vector<opt>{10, {}, 0});
    CHECK(coli(d, "SELECT x / 2 FROM m.t ORDER BY id;") == std::vector<opt>{2, {}, 0});
    CHECK(coli(d, "SELECT x % 3 FROM m.t ORDER BY id;") == std::vector<opt>{2, {}, 0});
    CHECK(coli(d, "SELECT -x FROM m.t ORDER BY id;") == std::vector<opt>{-5, {}, 0});
    CHECK(coli(d, "SELECT 100 - x FROM m.t ORDER BY id;") == std::vector<opt>{95, {}, 100});
    CHECK(coli(d, "SELECT 1 + x FROM m.t ORDER BY id;") == std::vector<opt>{6, {}, 1});
    // Division by zero is an ERROR, not a NULL
    CHECK_FALSE(okq(d, "SELECT 10 / x FROM m.t ORDER BY id;"));
    CHECK_FALSE(okq(d, "SELECT 10 % x FROM m.t ORDER BY id;"));
    CHECK(coli(d, "SELECT (x + 1) * 2 FROM m.t ORDER BY id;") == std::vector<opt>{12, {}, 2});
    CHECK(coli(d, "SELECT x + x FROM m.t ORDER BY id;") == std::vector<opt>{10, {}, 0});
    CHECK(coli(d, "SELECT x * x - 1 FROM m.t ORDER BY id;") == std::vector<opt>{24, {}, -1});
}

TEST_CASE("integration::cpp::null_matrix::arithmetic_where_and_update") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/arith_wu"));
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (2, NULL);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (3, 0);"));

        CHECK(nrows(d, "SELECT id FROM m.t WHERE x + 1 = 1;") == 1);
        CHECK(nrows(d, "SELECT id FROM m.t WHERE x * 2 = 0;") == 1);
        CHECK(nrows(d, "SELECT id FROM m.t WHERE x - 5 = 0;") == 1);
        // Constant folding of a NULL literal makes the comparison UNKNOWN for all rows.
        CHECK(nrows(d, "SELECT id FROM m.t WHERE 1 + NULL = 1;") == 0);
        CHECK(nrows(d, "SELECT id FROM m.t WHERE x = 1 + NULL;") == 0);

        // UPDATE SET x = x + 1 must keep the NULL row NULL, not write a 1.
        REQUIRE(okq(d, "UPDATE m.t SET x = x + 1;"));
        CHECK(coli(d, "SELECT x FROM m.t ORDER BY id;") == std::vector<opt>{6, {}, 1});
        CHECK(nrows(d, "SELECT id FROM m.t WHERE x IS NULL;") == 1);
        CHECK(scal(d, "SELECT COUNT(x) FROM m.t;") == opt{2});
    }
    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        CHECK(coli(d, "SELECT x FROM m.t ORDER BY id;") == std::vector<opt>{6, {}, 1});
        CHECK(nrows(d, "SELECT id FROM m.t WHERE x IS NULL;") == 1);
    }
}

TEST_CASE("integration::cpp::null_matrix::arithmetic_aggregate") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/arith_agg"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.g (k INT, a BIGINT, b BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, a, b) VALUES (1, 3, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, a, b) VALUES (1, 4, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, a, b) VALUES (2, 5, 10);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, a, b) VALUES (2, 5, 20);"));

    CHECK(coli(d, "SELECT SUM(a) + SUM(b) FROM m.g GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 40});
    CHECK(coli(d, "SELECT SUM(a) - SUM(b) FROM m.g GROUP BY k ORDER BY k;") == std::vector<opt>{{}, -20});
    CHECK(coli(d, "SELECT SUM(a) / SUM(b) FROM m.g GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 0});
    CHECK(scal(d, "SELECT SUM(a + b) FROM m.g;") == opt{40});
}

TEST_CASE("integration::cpp::null_matrix::aggregates_grouped") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/agg_grp"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (k INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (k, x) VALUES (1, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.t (k, x) VALUES (1, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.t (k, x) VALUES (2, 7);"));
    REQUIRE(okq(d, "INSERT INTO m.t (k, x) VALUES (2, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.t (k, x) VALUES (3, 3);"));
    REQUIRE(okq(d, "INSERT INTO m.t (k, x) VALUES (3, 5);"));

    CHECK(coli(d, "SELECT COUNT(x) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{0, 1, 2});
    CHECK(coli(d, "SELECT COUNT(*) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{2, 2, 2});
    CHECK(coli(d, "SELECT COUNT(DISTINCT x) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{0, 1, 2});
    CHECK(coli(d, "SELECT SUM(x) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7, 8});
    CHECK(coli(d, "SELECT MIN(x) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7, 3});
    CHECK(coli(d, "SELECT MAX(x) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7, 5});
    CHECK(coli(d, "SELECT AVG(x) FROM m.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7, 4});

    REQUIRE(okq(d, "CREATE TABLE m.allnull (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.allnull (id, x) VALUES (1, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.allnull (id, x) VALUES (2, NULL);"));
    CHECK(scal(d, "SELECT COUNT(x) FROM m.allnull;") == opt{0});
    CHECK(scal(d, "SELECT COUNT(*) FROM m.allnull;") == opt{2});
    CHECK(scal(d, "SELECT COUNT(DISTINCT x) FROM m.allnull;") == opt{0});
    CHECK(scal(d, "SELECT SUM(x) FROM m.allnull;") == opt{});
    CHECK(scal(d, "SELECT MIN(x) FROM m.allnull;") == opt{});
    CHECK(scal(d, "SELECT MAX(x) FROM m.allnull;") == opt{});
    CHECK(scal(d, "SELECT AVG(x) FROM m.allnull;") == opt{}); // AVG over all-NULL is NULL, not 0
}

TEST_CASE("integration::cpp::null_matrix::case_when") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/case"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (2, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (3, 1);"));

    // A NULL operand makes the WHEN condition UNKNOWN -> falls through to ELSE, for every operator.
    CHECK(scal(d, "SELECT SUM(CASE WHEN x = 1 THEN 1 ELSE 0 END) FROM m.t;") == opt{1});
    CHECK(scal(d, "SELECT SUM(CASE WHEN x <> 1 THEN 1 ELSE 0 END) FROM m.t;") == opt{1});
    CHECK(scal(d, "SELECT SUM(CASE WHEN x > 1 THEN 1 ELSE 0 END) FROM m.t;") == opt{1});
    CHECK(scal(d, "SELECT SUM(CASE WHEN x >= 1 THEN 1 ELSE 0 END) FROM m.t;") == opt{2});
    CHECK(scal(d, "SELECT SUM(CASE WHEN x < 5 THEN 1 ELSE 0 END) FROM m.t;") == opt{1});
    CHECK(scal(d, "SELECT SUM(CASE WHEN x <= 1 THEN 1 ELSE 0 END) FROM m.t;") == opt{1});
    CHECK(scal(d, "SELECT SUM(CASE WHEN x = 999 THEN 1 ELSE 0 END) FROM m.t;") == opt{0});
}

TEST_CASE("integration::cpp::null_matrix::having") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/having"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.g (k INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, x) VALUES (1, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, x) VALUES (1, NULL);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, x) VALUES (2, 3);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, x) VALUES (2, 5);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, x) VALUES (3, 1);"));
    REQUIRE(okq(d, "INSERT INTO m.g (k, x) VALUES (3, 0);"));

    // The all-NULL group's SUM is NULL -> UNKNOWN for every HAVING comparison -> always dropped.
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) = 8;") == 1);
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) <> 8;") == 1);
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) > 0;") == 2);
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) >= 1;") == 2);
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) < 2;") == 1);
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) <= 8;") == 2);
    CHECK(nrows(d, "SELECT k FROM m.g GROUP BY k HAVING SUM(x) = 0;") == 0);
}

TEST_CASE("integration::cpp::null_matrix::check_constraints") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/check"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));

    // In a CHECK, a NULL operand is UNKNOWN and thus accepted; a definitely-false row is rejected.
    struct ck {
        std::string op;
        std::string good;
        std::string bad;
    };
    const std::vector<ck> cks = {
        {"x > 0", "5", "-1"},
        {"x >= 0", "0", "-1"},
        {"x < 10", "5", "10"},
        {"x <= 10", "10", "11"},
        {"x <> 0", "5", "0"},
    };
    int n = 0;
    for (const auto& c : cks) {
        const std::string t = "m.ck" + std::to_string(n++);
        REQUIRE(okq(d, "CREATE TABLE " + t + " (id INT, x BIGINT);"));
        REQUIRE(okq(d, "ALTER TABLE " + t + " ADD CONSTRAINT cc CHECK (" + c.op + ");"));
        CHECK(okq(d, "INSERT INTO " + t + " (id, x) VALUES (1, " + c.good + ");"));
        CHECK_FALSE(okq(d, "INSERT INTO " + t + " (id, x) VALUES (2, " + c.bad + ");"));
        CHECK(okq(d, "INSERT INTO " + t + " (id, x) VALUES (3, NULL);"));
        CHECK(nrows(d, "SELECT id FROM " + t + ";") == 2);
        CHECK(nrows(d, "SELECT id FROM " + t + " WHERE x IS NULL;") == 1);
    }
}

TEST_CASE("integration::cpp::null_matrix::distinct") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/distinct"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT, y BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, y) VALUES (1, 5, 1);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, y) VALUES (2, NULL, 1);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, y) VALUES (3, NULL, 2);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, y) VALUES (4, 5, 1);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, y) VALUES (5, NULL, 2);"));

    // DISTINCT treats all NULLs as one value: {5, NULL} -> 2 distinct.
    CHECK(nrows(d, "SELECT DISTINCT x FROM m.t;") == 2);
    // Multi-column DISTINCT: (5,1),(NULL,1),(NULL,2) -> 3 distinct rows.
    CHECK(nrows(d, "SELECT DISTINCT x, y FROM m.t;") == 3);
    // COUNT(DISTINCT) excludes NULL entirely.
    CHECK(scal(d, "SELECT COUNT(DISTINCT x) FROM m.t;") == opt{1});
}

TEST_CASE("integration::cpp::null_matrix::array_element") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/arr"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.a (id BIGINT, v INT[3]);"));
    REQUIRE(okq(d, "INSERT INTO m.a (id, v) VALUES (1, ARRAY[10, 20, 30]);"));
    REQUIRE(okq(d, "INSERT INTO m.a (id, v) VALUES (2, NULL);"));      // whole cell NULL
    REQUIRE(okq(d, "INSERT INTO m.a (id, v) VALUES (3, ARRAY[40]);")); // v[2],v[3] NULL-padded
    REQUIRE(okq(d, "INSERT INTO m.a (id, v) VALUES (4, ARRAY[50, 60, 70]);"));

    // Subscript of a NULL array cell projects NULL.
    CHECK(coli(d, "SELECT v[1] FROM m.a ORDER BY id;") == std::vector<opt>{10, {}, 40, 50});
    CHECK(coli(d, "SELECT v[2] FROM m.a ORDER BY id;") == std::vector<opt>{20, {}, {}, 60});
    CHECK(coli(d, "SELECT v[3] FROM m.a ORDER BY id;") == std::vector<opt>{30, {}, {}, 70});

    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[1] = 10;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[1] > 5;") == 3);
    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[3] = 30;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[1] IS NULL;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[1] IS NOT NULL;") == 3);
    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[3] IS NULL;") == 2);
    CHECK(nrows(d, "SELECT id FROM m.a WHERE v[3] IS NOT NULL;") == 2);
}

TEST_CASE("integration::cpp::null_matrix::index_parity") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/idx"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    seed_numeric(d, "m.t");
    REQUIRE(okq(d, "CREATE INDEX ix ON m.t (x);"));

    // Secondary index on x excludes NULL keys, so answers match the unindexed runs.
    comparison_battery(d, "m.t");
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x = 10;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x = 5;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x = 7;") == 0);
    CHECK(nrows(d, "SELECT id FROM m.t WHERE x IS NULL;") == 2);
}

TEST_CASE("integration::cpp::null_matrix::jsonb_absent_key") {
    auto config = test_helpers::make_test_config(integration_fixture_path("nmx/jsonb"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.j ();"));
    REQUIRE(okq(d, "INSERT INTO m.j (id, doc) VALUES (1, 5);"));
    REQUIRE(okq(d, "INSERT INTO m.j (id) VALUES (2);"));
    REQUIRE(okq(d, "INSERT INTO m.j (id, doc) VALUES (3, 0);"));

    // A comparison through a navigation to an absent key is UNKNOWN, exactly like a plain NULL.
    CHECK(nrows(d, "SELECT id FROM m.j WHERE doc = 0;") == 1);
    CHECK(nrows(d, "SELECT id FROM m.j WHERE doc >= 0;") == 2);
    CHECK(nrows(d, "SELECT id FROM m.j WHERE NOT (doc = 0);") == 1);
    CHECK(nrows(d, "SELECT id FROM m.j WHERE doc IS NULL;") == 1);
    CHECK(scal(d, "SELECT COUNT(doc) FROM m.j;") == opt{2});
}

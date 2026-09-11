#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

// Aggregate FILTER clause lowers to agg(CASE WHEN p THEN x END); UNKNOWN excludes a row like a NULL value would.

namespace {
    using test_helpers::exec;
    using opt = std::optional<int64_t>;
    using L = std::vector<opt>;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
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
} // namespace

// Distinguishes correct 3VL NOT semantics from a NOT-as-OR bug, which would sum the g=1 rows instead.
TEST_CASE("integration::cpp::aggregate_filter::scalar") {
    auto config = test_helpers::make_test_config(integration_fixture_path("aggregate_filter/scalar"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT, g INT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, g) VALUES (1,10,0),(2,20,1),(3,30,0),(4,NULL,1),(5,40,0);"));

    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 0) FROM m.t;") == L{80});
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE g = 0) FROM m.t;") == L{3});
    CHECK(coli(d, "SELECT COUNT(x) FILTER (WHERE g = 1) FROM m.t;") == L{1});
    CHECK(coli(d, "SELECT MIN(x) FILTER (WHERE g = 0) FROM m.t;") == L{10});
    CHECK(coli(d, "SELECT MAX(x) FILTER (WHERE g = 0) FROM m.t;") == L{40});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE x > 15) FROM m.t;") == L{90});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 0 AND x > 10) FROM m.t;") == L{70});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE NOT (g = 1)) FROM m.t;") == L{80});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE NOT (g = 0 AND x > 10)) FROM m.t;") == L{30});
}

// A NULL operand makes the filter UNKNOWN, which excludes the row.
TEST_CASE("integration::cpp::aggregate_filter::three_valued") {
    auto config = test_helpers::make_test_config(integration_fixture_path("aggregate_filter/tvl"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1,10),(2,NULL),(3,30);"));

    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE x = 10) FROM m.t;") == L{1});
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE x > 5) FROM m.t;") == L{2});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE x > 5) FROM m.t;") == L{40});
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE x = 999) FROM m.t;") == L{0});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE x = 999) FROM m.t;") == L{opt{}});
}

// The CASE-lowered result column must be typed from the branch (BIGINT), not row 0's NULL probe
// value, or the output vector gets built with the unsized NA sentinel type.
TEST_CASE("integration::cpp::aggregate_filter::first_row_null") {
    auto config = test_helpers::make_test_config(integration_fixture_path("aggregate_filter/frn"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT, g INT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, g) VALUES (1,NULL,0),(2,20,0),(3,NULL,1),(4,40,0);"));

    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 0) FROM m.t;") == L{60});
    CHECK(coli(d, "SELECT COUNT(x) FILTER (WHERE g = 0) FROM m.t;") == L{2});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 999) FROM m.t;") == L{opt{}});
    CHECK(coli(d, "SELECT MAX(x) FILTER (WHERE x > 25) FROM m.t;") == L{40});
}

// The filter applies per group, independently of the grouping.
TEST_CASE("integration::cpp::aggregate_filter::group_by") {
    auto config = test_helpers::make_test_config(integration_fixture_path("aggregate_filter/group"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (g INT, x BIGINT, flag INT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (g, x, flag) VALUES (1,10,1),(1,20,0),(2,30,1),(2,40,1);"));

    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE flag = 1) FROM m.t GROUP BY g ORDER BY g;") == L{10, 70});
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE flag = 1) FROM m.t GROUP BY g ORDER BY g;") == L{1, 2});
}

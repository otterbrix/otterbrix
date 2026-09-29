#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

// count(1) / sum(<literal>): the literal becomes a plan parameter, so the aggregate's only input is a constant slot.

namespace {
    using test_helpers::exec;
    // Cells as text, so a failing CHECK prints the values; "NULL" for a null cell.
    using L = std::vector<std::string>;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    template<typename D>
    L col(D* d, const std::string& sql, uint64_t column = 0) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        L out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            const auto v = c->value(column, r);
            out.push_back(v.is_null() ? std::string{"NULL"} : std::to_string(v.template value<int64_t>()));
        }
        return out;
    }

    template<typename D>
    void seed(D* d) {
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (id BIGINT, g BIGINT);"));
        REQUIRE(okq(d, "INSERT INTO m.t (id, g) VALUES (1,1),(2,1),(3,2);"));
        REQUIRE(okq(d, "CREATE TABLE m.e (id BIGINT);"));
    }
} // namespace

TEST_CASE("integration::cpp::constant_aggregates::scalar") {
    auto config = test_helpers::make_test_config(integration_fixture_path("constant_aggregates/scalar"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(col(d, "SELECT count(1) FROM m.t;") == L{"3"});
    CHECK(col(d, "SELECT count('x') FROM m.t;") == L{"3"});
    CHECK(col(d, "SELECT sum(2) FROM m.t;") == L{"6"});
    CHECK(col(d, "SELECT count(1) FROM m.t WHERE id > 1;") == L{"2"});
    CHECK(col(d, "SELECT count(DISTINCT 1) FROM m.t;") == L{"1"});
    CHECK(col(d, "SELECT count(1);") == L{"1"});
    CHECK(col(d, "SELECT sum(1);") == L{"1"});
}

TEST_CASE("integration::cpp::constant_aggregates::group_by") {
    auto config = test_helpers::make_test_config(integration_fixture_path("constant_aggregates/group_by"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const std::string sql = "SELECT g, count(1) AS c, sum(1) AS s FROM m.t GROUP BY g ORDER BY g;";
    CHECK(col(d, sql, 0) == L{"1", "2"});
    CHECK(col(d, sql, 1) == L{"2", "1"});
    CHECK(col(d, sql, 2) == L{"2", "1"});
}

TEST_CASE("integration::cpp::constant_aggregates::many_chunks") {
    auto config = test_helpers::make_test_config(integration_fixture_path("constant_aggregates/many_chunks"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.big (id BIGINT);"));
    auto seeded = test_helpers::seed_rows(d, "m.big", "id", 2500, [](unsigned i) {
        return "(" + std::to_string(i) + ")";
    });
    REQUIRE(seeded);
    REQUIRE(seeded->is_success());

    CHECK(col(d, "SELECT count(1) FROM m.big;") == L{"2500"});
    CHECK(col(d, "SELECT sum(1) FROM m.big;") == L{"2500"});
}

TEST_CASE("integration::cpp::constant_aggregates::empty_input") {
    auto config = test_helpers::make_test_config(integration_fixture_path("constant_aggregates/empty_input"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(col(d, "SELECT count(1) FROM m.e;") == L{"0"});
    CHECK(col(d, "SELECT sum(1) FROM m.e;") == L{"NULL"});
    CHECK(col(d, "SELECT count(1) FROM m.t WHERE id > 100;") == L{"0"});
    CHECK(col(d, "SELECT sum(1) FROM m.t WHERE id > 100;") == L{"NULL"});
}

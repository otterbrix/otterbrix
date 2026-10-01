#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/types/types.hpp>
#include <sstream>
#include <string>
#include <vector>

// avg/sum result types follow Trino: integer avg -> DOUBLE, integer sum -> BIGINT (overflow is an error),
// sum(decimal(p,s)) -> DECIMAL(38,s), avg(decimal(p,s)) -> DECIMAL(p,s), real/double keep their type.

namespace {
    using components::types::logical_type;
    using test_helpers::exec;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    std::string type_name(const components::types::complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::BIGINT:
                return "BIGINT";
            case logical_type::DOUBLE:
                return "DOUBLE";
            case logical_type::FLOAT:
                return "REAL";
            case logical_type::DECIMAL: {
                const auto* ext = type.extension_as<components::types::decimal_logical_type_extension>();
                return "DECIMAL(" + std::to_string(static_cast<unsigned>(ext->width())) + "," +
                       std::to_string(static_cast<unsigned>(ext->scale())) + ")";
            }
            default:
                return "type#" + std::to_string(static_cast<int>(type.type()));
        }
    }

    // "<column type>:<value>", so a failing CHECK shows both; a DECIMAL shows its unscaled payload.
    std::string text(const components::cursor::cursor_t_ptr& c, uint64_t column, uint64_t row) {
        const auto& type = c->type_data()[column];
        const auto v = c->value(column, row);
        std::string out = type_name(type) + ":";
        if (v.is_null()) {
            return out + "NULL";
        }
        std::ostringstream value;
        switch (type.type()) {
            case logical_type::BIGINT:
                value << v.value<int64_t>();
                break;
            case logical_type::DOUBLE:
                value << v.value<double>();
                break;
            case logical_type::FLOAT:
                value << v.value<float>();
                break;
            case logical_type::DECIMAL:
                switch (type.to_physical_type()) {
                    case components::types::physical_type::INT16:
                        value << v.value<int16_t>();
                        break;
                    case components::types::physical_type::INT32:
                        value << v.value<int32_t>();
                        break;
                    case components::types::physical_type::INT64:
                        value << v.value<int64_t>();
                        break;
                    default:
                        value << v.value<components::types::int128_t>();
                        break;
                }
                break;
            default:
                value << "?";
                break;
        }
        return out + value.str();
    }

    template<typename D>
    std::vector<std::string> column(D* d, const std::string& sql, uint64_t col = 0) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        INFO((c->is_success() ? std::string{} : std::string{c->get_error().what}));
        REQUIRE(c->is_success());
        std::vector<std::string> out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            out.push_back(text(c, col, r));
        }
        return out;
    }

    template<typename D>
    std::string cell(D* d, const std::string& sql, uint64_t col = 0) {
        auto values = column(d, sql, col);
        INFO(sql);
        REQUIRE(values.size() == 1);
        return values.front();
    }

    template<typename D>
    std::string explain(D* d, const std::string& sql) {
        auto c = exec(d, "EXPLAIN " + sql);
        REQUIRE(c);
        REQUIRE(c->is_success());
        std::string out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            const auto line = c->value(0, r);
            out += std::string(line.template value<std::string_view>()) + "\n";
        }
        return out;
    }

    template<typename D>
    void seed(D* d) {
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (g BIGINT, b BIGINT, i INT, s SMALLINT, y TINYINT, r REAL, f DOUBLE);"));
        REQUIRE(okq(d,
                    "INSERT INTO m.t (g, b, i, s, y, r, f) VALUES "
                    "(1, 1, 1, 1, 1, 1.0, 1.0), (1, 2, 2, 2, 2, 2.0, 2.0), (2, 3, 3, 3, 3, 3.0, 3.0), "
                    "(2, 4, 4, 4, 4, 4.0, 4.0), (2, 6, 6, 6, 6, 6.0, 6.0);"));
        REQUIRE(okq(d, "CREATE TABLE m.e (b BIGINT, i INT, d DECIMAL(10,2));"));
    }
} // namespace

TEST_CASE("integration::cpp::avg_sum_widening::avg_keeps_fraction") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/fraction"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(cell(d, "SELECT avg(b) FROM m.t;") == "DOUBLE:3.2");
    CHECK(cell(d, "SELECT avg(i) FROM m.t;") == "DOUBLE:3.2");
    CHECK(cell(d, "SELECT avg(s) FROM m.t;") == "DOUBLE:3.2");
    CHECK(cell(d, "SELECT avg(y) FROM m.t;") == "DOUBLE:3.2");
    CHECK(cell(d, "SELECT avg(r) FROM m.t;") == "REAL:3.2");
    CHECK(cell(d, "SELECT avg(f) FROM m.t;") == "DOUBLE:3.2");
}

TEST_CASE("integration::cpp::avg_sum_widening::sum_widens_integers") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/sum_widens"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    CHECK(cell(d, "SELECT sum(b) FROM m.t;") == "BIGINT:16");
    CHECK(cell(d, "SELECT sum(i) FROM m.t;") == "BIGINT:16");
    CHECK(cell(d, "SELECT sum(s) FROM m.t;") == "BIGINT:16");
    CHECK(cell(d, "SELECT sum(y) FROM m.t;") == "BIGINT:16");
    CHECK(cell(d, "SELECT sum(r) FROM m.t;") == "REAL:16");
    CHECK(cell(d, "SELECT sum(f) FROM m.t;") == "DOUBLE:16");
}

TEST_CASE("integration::cpp::avg_sum_widening::small_int_total") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/small_int"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.s (v SMALLINT, w INT);"));
    // 1500 rows: the fold crosses chunk boundaries.
    auto seeded = test_helpers::seed_rows(d, "m.s", "v, w", 1500, [](unsigned) {
        return std::string{"(200, 2000000)"};
    });
    REQUIRE(seeded);
    REQUIRE(seeded->is_success());

    CHECK(cell(d, "SELECT sum(v) FROM m.s;") == "BIGINT:300000");
    CHECK(cell(d, "SELECT avg(v) FROM m.s;") == "DOUBLE:200");
    CHECK(cell(d, "SELECT sum(w) FROM m.s;") == "BIGINT:3000000000");
    CHECK(cell(d, "SELECT avg(w) FROM m.s;") == "DOUBLE:2e+06");
}

TEST_CASE("integration::cpp::avg_sum_widening::bigint_sum_overflow_is_an_error") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/overflow"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (g BIGINT, b BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (g, b) VALUES (1, 9223372036854775807), (1, 1), (2, 5);"));

    for (const char* sql : {"SELECT sum(b) FROM m.t;",
                            "SELECT g, sum(b) FROM m.t GROUP BY g;",
                            "SELECT g, sum(b) FROM m.t GROUP BY g HAVING count(*) > 0;"}) {
        auto c = exec(d, sql);
        INFO(sql);
        REQUIRE(c);
        REQUIRE_FALSE(c->is_success());
        CHECK(c->get_error().type == core::error_code_t::arithmetics_failure);
        CHECK(std::string{c->get_error().what}.find("overflow") != std::string::npos);
    }
    // avg accumulates wider than the input, so the same rows average without overflowing.
    CHECK(cell(d, "SELECT avg(b) FROM m.t WHERE g = 2;") == "DOUBLE:5");
}

TEST_CASE("integration::cpp::avg_sum_widening::decimal") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/decimal"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (g BIGINT, x DECIMAL(10,2));"));
    REQUIRE(okq(d, "INSERT INTO m.t (g, x) VALUES (1, 1.00), (1, 2.00), (1, 2.00), (2, -1.25), (2, -2.50);"));

    CHECK(cell(d, "SELECT sum(x) FROM m.t;") == "DECIMAL(38,2):125");
    // 1.25 / 5 = 0.25
    CHECK(cell(d, "SELECT avg(x) FROM m.t;") == "DECIMAL(10,2):25");
    // 5.00 / 3 = 1.666.. -> 1.67; -3.75 / 2 = -1.875 -> -1.88 (half away from zero)
    const std::string grouped = "SELECT g, avg(x) AS a, sum(x) AS s FROM m.t GROUP BY g ORDER BY g;";
    CHECK(column(d, grouped, 1) == std::vector<std::string>{"DECIMAL(10,2):167", "DECIMAL(10,2):-188"});
    CHECK(column(d, grouped, 2) == std::vector<std::string>{"DECIMAL(38,2):500", "DECIMAL(38,2):-375"});
}

TEST_CASE("integration::cpp::avg_sum_widening::group_by") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/group_by"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const std::string sql = "SELECT g, avg(b) AS a, sum(i) AS s FROM m.t GROUP BY g ORDER BY g;";
    CHECK(column(d, sql, 1) == std::vector<std::string>{"DOUBLE:1.5", "DOUBLE:4.33333"});
    CHECK(column(d, sql, 2) == std::vector<std::string>{"BIGINT:3", "BIGINT:13"});
}

TEST_CASE("integration::cpp::avg_sum_widening::empty_input_is_typed_null") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/empty"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const std::string sql = "SELECT sum(b) AS sb, avg(b) AS ab, sum(i) AS si, avg(i) AS ai, sum(d) AS sd, "
                            "avg(d) AS ad FROM m.e;";
    const std::vector<std::string> expected{"BIGINT:NULL",
                                            "DOUBLE:NULL",
                                            "BIGINT:NULL",
                                            "DOUBLE:NULL",
                                            "DECIMAL(38,2):NULL",
                                            "DECIMAL(10,2):NULL"};
    for (uint64_t col = 0; col < expected.size(); ++col) {
        CHECK(cell(d, sql, col) == expected[col]);
    }
    CHECK(cell(d, "SELECT avg(b) FROM m.t WHERE b > 100;") == "DOUBLE:NULL");
    CHECK(column(d, "SELECT g, avg(b) FROM m.t WHERE b > 100 GROUP BY g;").empty());
}

// The owning disk agent reduces the table and the coordinator only finalizes; HAVING keeps the same query
// on the coordinator. Both must agree on types and values.
TEST_CASE("integration::cpp::avg_sum_widening::agent_reduce_matches_coordinator") {
    auto config = test_helpers::make_test_config(integration_fixture_path("avg_sum_widening/agent_reduce"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    const std::string pushed = "SELECT g, avg(i) AS a, sum(s) AS t FROM m.t GROUP BY g ORDER BY g;";
    const std::string local = "SELECT g, avg(i) AS a, sum(s) AS t FROM m.t GROUP BY g HAVING count(*) > 0 ORDER BY g;";
    const auto pushed_plan = explain(d, pushed);
    INFO(pushed_plan);
    REQUIRE(pushed_plan.find("Pushed Aggregate Scan") != std::string::npos);
    REQUIRE(explain(d, local).find("Pushed Aggregate Scan") == std::string::npos);

    const std::vector<std::string> averages{"DOUBLE:1.5", "DOUBLE:4.33333"};
    const std::vector<std::string> sums{"BIGINT:3", "BIGINT:13"};
    CHECK(column(d, pushed, 1) == averages);
    CHECK(column(d, pushed, 2) == sums);
    CHECK(column(d, local, 1) == averages);
    CHECK(column(d, local, 2) == sums);

    // Scalar form over no rows: the finalize step synthesizes the one row, typed from the plan.
    const std::string scalar_empty = "SELECT avg(i) AS a, sum(s) AS t FROM m.t WHERE g > 10;";
    REQUIRE(explain(d, scalar_empty).find("Finalize Aggregate") != std::string::npos);
    CHECK(cell(d, scalar_empty, 0) == "DOUBLE:NULL");
    CHECK(cell(d, scalar_empty, 1) == "BIGINT:NULL");
}

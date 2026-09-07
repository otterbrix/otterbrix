#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

// Two independent 3VL defects in CASE evaluation: NOT inside a WHEN was folded like union_or
// (so a NULL operand's UNKNOWN collapsed to FALSE before NOT could resurrect it), and a plain
// projection typed its output from row 0's first THEN, bad_alloc'ing when that value was NULL.

namespace {
    using test_helpers::exec;
    using opt = std::optional<int64_t>;
    using L = std::vector<opt>;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    // Column 0 of every row as optional<int64> (nullopt == NULL), in result order.
    template<typename D>
    std::vector<opt> coli(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        const std::string why = c->is_error() ? std::string{c->get_error().what.c_str()} : std::string{};
        INFO(why);
        REQUIRE(c->is_success());
        std::vector<opt> out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            auto v = c->value(0, r);
            out.push_back(v.is_null() ? opt{} : opt{v.template value<int64_t>()});
        }
        return out;
    }
} // namespace

TEST_CASE("integration::cpp::case3vl::not_in_when") {
    auto config = test_helpers::make_test_config(integration_fixture_path("case3vl/not_in_when"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5), (2, NULL), (3, 0), (4, 10);"));

    CHECK(coli(d, "SELECT CASE WHEN NOT (x = 5) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{0, 0, 1, 1});
    CHECK(coli(d, "SELECT CASE WHEN NOT (x > 0) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{0, 0, 1, 0});
    CHECK(coli(d, "SELECT CASE WHEN NOT (x = 5 OR x = 10) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{0, 0, 1, 0});
    CHECK(coli(d, "SELECT CASE WHEN NOT (x = 0 AND id = 3) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{1, 1, 0, 1});
    CHECK(coli(d, "SELECT CASE WHEN x = 5 THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{1, 0, 0, 0});
    CHECK(coli(d, "SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{1, 0, 0, 1});
}

TEST_CASE("integration::cpp::case3vl::projection_null_row0") {
    auto config = test_helpers::make_test_config(integration_fixture_path("case3vl/proj"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    // Row 0 (id=1) is the NULL row, so the first THEN (x) is NULL at row 0.
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, NULL), (2, 5), (3, 0);"));

    CHECK(coli(d, "SELECT CASE WHEN x >= 0 THEN x ELSE 99 END FROM m.t ORDER BY id;") == L{99, 5, 0});
    CHECK(coli(d, "SELECT CASE WHEN x = 5 THEN x END FROM m.t ORDER BY id;") == L{opt{}, 5, opt{}});
    CHECK(coli(d, "SELECT CASE WHEN x = 5 THEN x ELSE id END FROM m.t ORDER BY id;") == L{1, 5, 3});
    CHECK(coli(d, "SELECT CASE WHEN x = 777 THEN x END FROM m.t ORDER BY id;") == L{opt{}, opt{}, opt{}});
}

TEST_CASE("integration::cpp::case3vl::aggregate_case_not") {
    auto config = test_helpers::make_test_config(integration_fixture_path("case3vl/agg"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5), (2, NULL), (3, 0), (4, 10);"));

    CHECK(coli(d, "SELECT SUM(CASE WHEN NOT (x = 5) THEN 1 ELSE 0 END) FROM m.t;") == L{2});
    CHECK(coli(d, "SELECT SUM(CASE WHEN NOT (x > 0) THEN 1 ELSE 0 END) FROM m.t;") == L{1});
}

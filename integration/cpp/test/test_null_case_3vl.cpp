#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <optional>
#include <string>
#include <vector>

// Three-valued logic in the CASE expression evaluator (the bound layer's bound_case_t), used for a CASE in a
// projection or aggregate. Two independent defects:
//   1. a NOT inside a WHEN was ignored (union_not was folded like union_or), so `WHEN NOT (p)`
//      behaved as `WHEN (p)`; and a NULL operand was two-valued (UNKNOWN collapsed to FALSE, then
//      NOT could resurrect it).
//   2. a plain projection `SELECT CASE ... FROM t` typed its output vector from the first THEN at
//      row 0; when that value was NULL the vector took the untyped NA type and the first non-NULL
//      result bad_alloc'd.

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
        REQUIRE(c->is_success());
        std::vector<opt> out;
        for (uint64_t r = 0; r < c->size(); ++r) {
            auto v = c->value(0, r);
            out.push_back(v.is_null() ? opt{} : opt{v.template value<int64_t>()});
        }
        return out;
    }
} // namespace

// -------------------------------------------------------------------------------------------------
//  NOT inside a WHEN condition -- must negate, and a NULL operand makes the WHEN UNKNOWN (-> ELSE).
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::case3vl::not_in_when") {
    auto config = test_helpers::make_test_config(test_temp_path("case3vl/not_in_when"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5), (2, NULL), (3, 0), (4, 10);"));

    // NOT (x = 5): TRUE for {0,10}; x=5 -> NOT TRUE = FALSE -> ELSE; NULL -> UNKNOWN -> ELSE.
    CHECK(coli(d, "SELECT CASE WHEN NOT (x = 5) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{0, 0, 1, 1});
    // NOT (x > 0): only x=0 makes x>0 FALSE -> NOT TRUE.
    CHECK(coli(d, "SELECT CASE WHEN NOT (x > 0) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{0, 0, 1, 0});
    // NOT over OR: NOT (x=5 OR x=10) -> TRUE only for x=0; NULL row is (U OR U)=U -> ELSE.
    CHECK(coli(d, "SELECT CASE WHEN NOT (x = 5 OR x = 10) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{0, 0, 1, 0});
    // NOT over AND: NOT (x=0 AND id=3) -> FALSE only for the id=3 row; the NULL row is
    // (U AND FALSE)=FALSE -> NOT TRUE -> THEN.
    CHECK(coli(d, "SELECT CASE WHEN NOT (x = 0 AND id = 3) THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{1, 1, 0, 1});
    // Positive control (no NOT): the plain WHEN still matches only the definitely-TRUE rows.
    CHECK(coli(d, "SELECT CASE WHEN x = 5 THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{1, 0, 0, 0});
    CHECK(coli(d, "SELECT CASE WHEN x > 0 THEN 1 ELSE 0 END FROM m.t ORDER BY id;") == L{1, 0, 0, 1});
}

// -------------------------------------------------------------------------------------------------
//  Projection CASE where the first THEN is NULL at row 0 -- output type must come from a real value.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::case3vl::projection_null_row0") {
    auto config = test_helpers::make_test_config(test_temp_path("case3vl/proj"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    // Row 0 (id=1) is the NULL row, so the first THEN (x) is NULL at row 0.
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, NULL), (2, 5), (3, 0);"));

    // THEN references x: row 0's THEN is NULL, but rows 1..2 must still produce typed values.
    CHECK(coli(d, "SELECT CASE WHEN x >= 0 THEN x ELSE 99 END FROM m.t ORDER BY id;") == L{99, 5, 0});
    // THEN yields NULL at row 0 (no match, no ELSE) then a real value later.
    CHECK(coli(d, "SELECT CASE WHEN x = 5 THEN x END FROM m.t ORDER BY id;") == L{opt{}, 5, opt{}});
    // ELSE supplies the row-0 value; THEN supplies later ones.
    CHECK(coli(d, "SELECT CASE WHEN x = 5 THEN x ELSE id END FROM m.t ORDER BY id;") == L{1, 5, 3});
    // All-NULL result column (never matches, no ELSE): a well-formed all-NULL projection.
    CHECK(coli(d, "SELECT CASE WHEN x = 777 THEN x END FROM m.t ORDER BY id;") == L{opt{}, opt{}, opt{}});
}

// -------------------------------------------------------------------------------------------------
//  CASE inside an aggregate over a NULL row (the SUM(CASE ...) shape) keeps working with NOT.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::case3vl::aggregate_case_not") {
    auto config = test_helpers::make_test_config(test_temp_path("case3vl/agg"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1, 5), (2, NULL), (3, 0), (4, 10);"));

    // SUM over NOT(x=5): scores 1 for x in {0,10}; NULL never scores. -> 2
    CHECK(coli(d, "SELECT SUM(CASE WHEN NOT (x = 5) THEN 1 ELSE 0 END) FROM m.t;") == L{2});
    // SUM over NOT(x>0): scores 1 for x=0 only. -> 1
    CHECK(coli(d, "SELECT SUM(CASE WHEN NOT (x > 0) THEN 1 ELSE 0 END) FROM m.t;") == L{1});
}

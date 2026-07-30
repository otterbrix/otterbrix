#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/tests/temp_dir.hpp>
#include <optional>
#include <string>
#include <vector>

// Aggregate FILTER clause:  agg(x) FILTER (WHERE p).  The parser captures the predicate into
// FuncCall.agg_filter, but the transformer dropped it, so the FILTER was silently ignored and the
// aggregate ran over every row.  It is lowered to  agg(CASE WHEN p THEN x END)  (and COUNT(*) to
// COUNT(CASE WHEN p THEN 1 END)); every supported aggregate skips NULLs, so rows failing p are
// excluded.  The predicate reuses the SQL three-valued CASE-WHEN evaluator, so an UNKNOWN (NULL
// operand) excludes the row.

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
//  Scalar aggregates with FILTER over a whole table.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::aggregate_filter::scalar") {
    auto config = test_helpers::make_test_config(test_temp_path("aggregate_filter/scalar"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT, g INT);"));
    // x:                  10    20    30    NULL   40
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, g) VALUES (1,10,0),(2,20,1),(3,30,0),(4,NULL,1),(5,40,0);"));

    // SUM(x) FILTER (WHERE g = 0): rows 1,3,5 -> 10+30+40 = 80  (unfiltered SUM would be 100).
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 0) FROM m.t;") == L{80});
    // COUNT(*) FILTER (WHERE g = 0): 3 rows have g=0.
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE g = 0) FROM m.t;") == L{3});
    // COUNT(x) FILTER (WHERE g = 1): rows 2 and 4 have g=1, but x is NULL in row 4 -> COUNT(x)=1.
    CHECK(coli(d, "SELECT COUNT(x) FILTER (WHERE g = 1) FROM m.t;") == L{1});
    // MIN / MAX FILTER over g=0 -> {10,30,40}.
    CHECK(coli(d, "SELECT MIN(x) FILTER (WHERE g = 0) FROM m.t;") == L{10});
    CHECK(coli(d, "SELECT MAX(x) FILTER (WHERE g = 0) FROM m.t;") == L{40});
    // A predicate on x itself: SUM(x) FILTER (WHERE x > 15) -> 20+30+40 = 90; NULL row excluded.
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE x > 15) FROM m.t;") == L{90});
    // Compound predicate: SUM(x) FILTER (WHERE g = 0 AND x > 10) -> 30+40 = 70.
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 0 AND x > 10) FROM m.t;") == L{70});
    // Negated predicate (SQL 3VL: NOT must negate, not behave like OR): rows with g<>1 are 1,3,5 ->
    // 10+30+40 = 80.  A NOT-as-OR fork instead sums the g=1 rows (20 + NULL) = 20.
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE NOT (g = 1)) FROM m.t;") == L{80});
    // NOT over a compound predicate: NOT (g = 0 AND x > 10) is TRUE for rows 1,2 (and UNKNOWN for the
    // NULL-x row 4, which is excluded) -> 10+20 = 30.
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE NOT (g = 0 AND x > 10)) FROM m.t;") == L{30});
}

// -------------------------------------------------------------------------------------------------
//  Three-valued logic: a NULL operand makes the filter UNKNOWN, which excludes the row.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::aggregate_filter::three_valued") {
    auto config = test_helpers::make_test_config(test_temp_path("aggregate_filter/tvl"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT);"));
    REQUIRE(okq(d, "INSERT INTO m.t (id, x) VALUES (1,10),(2,NULL),(3,30);"));

    // FILTER (WHERE x = 10): row 2 is (NULL = 10) = UNKNOWN -> excluded. Only row 1 qualifies.
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE x = 10) FROM m.t;") == L{1});
    // FILTER (WHERE x > 5): rows 1,3 (NULL excluded) -> COUNT 2, SUM 40.
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE x > 5) FROM m.t;") == L{2});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE x > 5) FROM m.t;") == L{40});
    // No row qualifies: COUNT -> 0, SUM -> NULL (empty aggregate).
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE x = 999) FROM m.t;") == L{0});
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE x = 999) FROM m.t;") == L{opt{}});
}

// -------------------------------------------------------------------------------------------------
//  The aggregated column is NULL in the FIRST row.  The CASE-lowered result column must still be
//  typed from the branch (BIGINT), not from row 0's (NULL) probe value, or the output vector is
//  built with the unsized NA sentinel type.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::aggregate_filter::first_row_null") {
    auto config = test_helpers::make_test_config(test_temp_path("aggregate_filter/frn"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (id INT, x BIGINT, g INT);"));
    // Row 0 (id=1) has x = NULL.
    REQUIRE(okq(d, "INSERT INTO m.t (id, x, g) VALUES (1,NULL,0),(2,20,0),(3,NULL,1),(4,40,0);"));

    // SUM(x) FILTER (WHERE g = 0): rows 1,2,4 pass g=0, but row 1's x is NULL -> 20+40 = 60.
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 0) FROM m.t;") == L{60});
    // COUNT(x) FILTER (WHERE g = 0): NULL x not counted -> 2.
    CHECK(coli(d, "SELECT COUNT(x) FILTER (WHERE g = 0) FROM m.t;") == L{2});
    // A filter no row satisfies -> SUM over an all-NULL column -> NULL (must not fault on typing).
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE g = 999) FROM m.t;") == L{opt{}});
    // MAX(x) FILTER (WHERE x > 25): only row 4 (x=40); row 1 NULL excluded -> 40.
    CHECK(coli(d, "SELECT MAX(x) FILTER (WHERE x > 25) FROM m.t;") == L{40});
}

// -------------------------------------------------------------------------------------------------
//  FILTER combined with GROUP BY: the filter applies per group, independently of the grouping.
// -------------------------------------------------------------------------------------------------
TEST_CASE("integration::cpp::aggregate_filter::group_by") {
    auto config = test_helpers::make_test_config(test_temp_path("aggregate_filter/group"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(okq(d, "CREATE DATABASE m;"));
    REQUIRE(okq(d, "CREATE TABLE m.t (g INT, x BIGINT, flag INT);"));
    // g=1: x in {10,20} flag {1,0};  g=2: x in {30,40} flag {1,1}
    REQUIRE(okq(d, "INSERT INTO m.t (g, x, flag) VALUES (1,10,1),(1,20,0),(2,30,1),(2,40,1);"));

    // SUM(x) FILTER (WHERE flag = 1) grouped by g, ordered by g:
    //   g=1 -> only x=10 (flag=1) -> 10;  g=2 -> 30+40 -> 70
    CHECK(coli(d, "SELECT SUM(x) FILTER (WHERE flag = 1) FROM m.t GROUP BY g ORDER BY g;") == L{10, 70});
    // COUNT(*) FILTER per group: g=1 -> 1, g=2 -> 2.
    CHECK(coli(d, "SELECT COUNT(*) FILTER (WHERE flag = 1) FROM m.t GROUP BY g ORDER BY g;") == L{1, 2});
}

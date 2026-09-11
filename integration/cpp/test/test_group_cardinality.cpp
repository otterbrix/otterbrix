#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>
#include <string>
#include <vector>

namespace {
    using test_helpers::exec;
    using L = std::vector<int64_t>;

    template<typename D>
    bool okq(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        return c && c->is_success();
    }

    // One column of every row as int64, in result order.
    template<typename D>
    L coli(D* d, const std::string& sql, uint64_t column = 0) {
        auto c = exec(d, sql);
        REQUIRE(c);
        INFO(sql);
        REQUIRE(c->is_success());
        L out;
        for (uint64_t row = 0; row < c->size(); ++row) {
            out.push_back(c->value(column, row).template value<int64_t>());
        }
        return out;
    }

    // Rejected BY THE GROUPING RULE -- not merely rejected, which a typo in the query would
    // also satisfy.
    template<typename D>
    bool rejected(D* d, const std::string& sql) {
        auto c = exec(d, sql);
        INFO(sql);
        if (!c || !c->is_error()) {
            return false;
        }
        INFO(std::string(c->get_error().what.c_str()));
        return c->get_error().type == core::error_code_t::sql_parse_error;
    }

    template<typename D>
    void seed(D* d) {
        REQUIRE(okq(d, "CREATE DATABASE m;"));
        REQUIRE(okq(d, "CREATE TABLE m.t (a INT, b INT, z INT);"));
        // a=1: two rows, a=2: three rows -- so a key and a reduction over the same group differ.
        REQUIRE(okq(d, "INSERT INTO m.t (a, b, z) VALUES (1,10,100),(1,20,200),(2,30,300),(2,40,400),(2,50,500);"));
    }
} // namespace

TEST_CASE("integration::cpp::group_cardinality::accepts") {
    auto config = test_helpers::make_test_config(integration_fixture_path("group_cardinality/accepts"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // A grouping key reduces to one value per group, so it composes with a reduction in a single
    // expression.  a=1 -> 1 + 2, a=2 -> 2 + 3.
    CHECK(coli(d, "SELECT a + count(b) FROM m.t GROUP BY a ORDER BY a;") == L{3, 5});
    // The same mix beside the bare key: both columns have to come back right.
    CHECK(coli(d, "SELECT a, a + count(b) FROM m.t GROUP BY a ORDER BY a;") == L{1, 2});
    CHECK(coli(d, "SELECT a, a + count(b) FROM m.t GROUP BY a ORDER BY a;", 1) == L{3, 5});
    // Reduction first: a=1 -> 300 + 1, a=2 -> 1200 + 2.
    CHECK(coli(d, "SELECT sum(z) + a FROM m.t GROUP BY a ORDER BY a;") == L{301, 1202});
    // Inside a reduction any column is legal, key or not: the reduction consumes the rows.
    CHECK(coli(d, "SELECT count(a + z) FROM m.t GROUP BY a ORDER BY a;") == L{2, 3});
    CHECK(coli(d, "SELECT sum(z) FROM m.t GROUP BY a ORDER BY a;") == L{300, 1200});
    // A non-key column reduced in HAVING, which is post-aggregation like the target list.
    CHECK(coli(d, "SELECT a FROM m.t GROUP BY a HAVING count(z) > 0 ORDER BY a;") == L{1, 2});
    // GROUP BY with no reduction at all is deduplication, and stays grouped.
    CHECK(coli(d, "SELECT a FROM m.t GROUP BY a ORDER BY a;") == L{1, 2});
    // A constant is one value per group by construction.
    CHECK(coli(d, "SELECT 1 FROM m.t GROUP BY a;") == L{1, 1});
    // Post-aggregate arithmetic over a reduction alone.
    CHECK(coli(d, "SELECT count(b) * 2 FROM m.t GROUP BY a ORDER BY a;") == L{4, 6});
}

TEST_CASE("integration::cpp::group_cardinality::rejects") {
    auto config = test_helpers::make_test_config(integration_fixture_path("group_cardinality/rejects"));
    test_spaces space(config);
    auto* d = space.dispatcher();
    seed(d);

    // A bare non-key column is one value per ROW, so a grouped query cannot project it: alone,
    CHECK(rejected(d, "SELECT b FROM m.t GROUP BY a;"));
    // beside a reduction,
    CHECK(rejected(d, "SELECT z, count(b) FROM m.t GROUP BY a;"));
    // or mixed into one expression with a reduction.
    CHECK(rejected(d, "SELECT z + count(b) FROM m.t GROUP BY a;"));
    // A reduction in the target list groups the query even without GROUP BY, and then there is no
    // key for a bare column to match.
    CHECK(rejected(d, "SELECT a, count(b) FROM m.t;"));
}

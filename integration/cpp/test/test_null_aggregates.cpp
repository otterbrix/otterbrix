#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <optional>
#include <string>
#include <vector>

// SQL NULL semantics for aggregates.
//
// COUNT(x) counts non-NULL values, so over an all-NULL (or empty) group it is 0 — never NULL.
// Every other aggregate (SUM/MIN/MAX/AVG) skips NULLs and is NULL over an all-NULL group.
// COUNT(*) counts rows regardless of NULLs. These must agree across the grouped, non-grouped,
// vectorized (single numeric column) and non-vectorized (DISTINCT) paths.
//
// Before the fix, the grouped-aggregate finalizer returned NA for any state that never saw a
// non-NULL value — taking that branch before the COUNT case — so COUNT over an all-NULL group
// was reported as NULL, disagreeing with COUNT(DISTINCT x) and COUNT(*), which return 0.

namespace {

    using opt = std::optional<int64_t>;

    template<typename D>
    bool ok(D* d, const std::string& sql) {
        auto s = otterbrix::session_id_t();
        auto c = d->execute_sql(s, sql);
        return c && c->is_success();
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
    opt scalar(D* d, const std::string& sql) {
        auto v = read_col(d, sql);
        REQUIRE(v.size() == 1);
        return v.front();
    }

} // namespace

TEST_CASE("integration::cpp::null_agg::count_over_all_null_group_is_zero") {
    auto config = test_create_config(integration_fixture_path("test_null_agg/grp"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE ag;"));
    // group 1: x all NULL ; group 2: x has one value and one NULL.
    REQUIRE(ok(d, "CREATE TABLE ag.t (k INT, x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO ag.t (k, x) VALUES (1, NULL);"));
    REQUIRE(ok(d, "INSERT INTO ag.t (k, x) VALUES (1, NULL);"));
    REQUIRE(ok(d, "INSERT INTO ag.t (k, x) VALUES (2, 7);"));
    REQUIRE(ok(d, "INSERT INTO ag.t (k, x) VALUES (2, NULL);"));

    // COUNT(x): group 1 -> 0 (not NULL) ; group 2 -> 1.
    CHECK(read_col(d, "SELECT COUNT(x) FROM ag.t GROUP BY k ORDER BY k;") == std::vector<opt>{0, 1});
    // COUNT(*): counts rows including NULL rows: group 1 -> 2 ; group 2 -> 2.
    CHECK(read_col(d, "SELECT COUNT(*) FROM ag.t GROUP BY k ORDER BY k;") == std::vector<opt>{2, 2});
    // COUNT(DISTINCT x): group 1 -> 0 ; group 2 -> 1. Must agree with COUNT(x).
    CHECK(read_col(d, "SELECT COUNT(DISTINCT x) FROM ag.t GROUP BY k ORDER BY k;") == std::vector<opt>{0, 1});

    // SUM/MIN/MAX/AVG skip NULLs and are NULL over the all-NULL group, defined over group 2.
    CHECK(read_col(d, "SELECT SUM(x) FROM ag.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7});
    CHECK(read_col(d, "SELECT MIN(x) FROM ag.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7});
    CHECK(read_col(d, "SELECT MAX(x) FROM ag.t GROUP BY k ORDER BY k;") == std::vector<opt>{{}, 7});
}

TEST_CASE("integration::cpp::null_agg::count_over_all_null_table_is_zero") {
    auto config = test_create_config(integration_fixture_path("test_null_agg/nogrp"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE ag;"));
    REQUIRE(ok(d, "CREATE TABLE ag.t (id INT, x BIGINT);"));
    REQUIRE(ok(d, "INSERT INTO ag.t (id, x) VALUES (1, NULL);"));
    REQUIRE(ok(d, "INSERT INTO ag.t (id, x) VALUES (2, NULL);"));

    // Ungrouped aggregate over an all-NULL column.
    CHECK(scalar(d, "SELECT COUNT(x) FROM ag.t;") == opt{0});
    CHECK(scalar(d, "SELECT COUNT(*) FROM ag.t;") == opt{2});
    CHECK(scalar(d, "SELECT COUNT(DISTINCT x) FROM ag.t;") == opt{0});
    CHECK(scalar(d, "SELECT SUM(x) FROM ag.t;") == opt{});
    CHECK(scalar(d, "SELECT MIN(x) FROM ag.t;") == opt{});
    CHECK(scalar(d, "SELECT MAX(x) FROM ag.t;") == opt{});
}

// COUNT(DISTINCT) keeps a seen-set keyed by logical_value_t::hash(): a hash of the storage pointer would count
// every equal LIST, STRUCT, INTERVAL or TIME_TZ copy as one more distinct value.
TEST_CASE("integration::cpp::null_agg::count_distinct_over_list_struct_interval_and_timetz") {
    auto config = test_create_config(integration_fixture_path("test_null_agg/distinct_composite"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* d = space.dispatcher();
    REQUIRE(ok(d, "CREATE DATABASE dc;"));
    REQUIRE(ok(d, "CREATE TYPE pair_t AS (px INT, py INT);"));
    REQUIRE(ok(d, "CREATE TABLE dc.t (l INT[], s pair_t, iv INTERVAL, tz TIMETZ);"));
    REQUIRE(ok(d,
               "INSERT INTO dc.t (l, s, iv, tz) VALUES "
               "(ARRAY[1,2], ROW(1,2), INTERVAL '1 day', TIMETZ '08:00:00+00:00'), "
               "(ARRAY[1,2], ROW(1,2), INTERVAL '1 day', TIMETZ '08:00:00+00:00'), "
               "(ARRAY[3], ROW(3,4), INTERVAL '2 day', TIMETZ '09:00:00+00:00');"));
    CHECK(scalar(d, "SELECT COUNT(DISTINCT l) FROM dc.t;").value_or(-1) == 2);
    CHECK(scalar(d, "SELECT COUNT(DISTINCT s) FROM dc.t;").value_or(-1) == 2);
    CHECK(scalar(d, "SELECT COUNT(DISTINCT iv) FROM dc.t;").value_or(-1) == 2);
    CHECK(scalar(d, "SELECT COUNT(DISTINCT tz) FROM dc.t;").value_or(-1) == 2);
}

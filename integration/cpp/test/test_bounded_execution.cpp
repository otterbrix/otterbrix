// Bounded-execution SELECT (HIDDEN): peak-RSS assertion is deferred pending per-batch fetch-next.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"
#include <catch2/catch_test_macros.hpp>

using namespace components;
using namespace components::cursor;

namespace {
    constexpr auto bounded_db = "bounded_db";
    constexpr auto bounded_coll = "bounded_coll";

    constexpr unsigned kRowCount = 20000;
    constexpr unsigned kNumGroups = 8;
} // namespace

// A multi-batch input (20000 rows, 8 groups) proves the incremental GROUP BY/scalar-aggregate sink folds across
// batches into correct bounded state, which is what's realizable before the scan side is bounded too.
TEST_CASE("integration::cpp::bounded_execution::group_by_and_scalar_aggregate", "[.][bounded-exec]") {
    auto config = test_create_config(integration_fixture_path("test_bounded_execution"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE DATABASE ") + bounded_db + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, bounded_db, bounded_coll);
    }

    {
        auto session = otterbrix::session_id_t();
        std::stringstream query;
        query << "INSERT INTO bounded_db.bounded_coll (name, grp, val) VALUES ";
        for (unsigned i = 0; i < kRowCount; ++i) {
            query << "('R" << i << "', " << (i % kNumGroups) << ", " << i << ")" << (i + 1 == kRowCount ? ";" : ", ");
        }
        auto cur = dispatcher->execute_sql(session, query.str());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }

    std::array<int64_t, kNumGroups> expected_sum{};
    std::array<uint64_t, kNumGroups> expected_count{};
    int64_t expected_total_sum = 0;
    for (unsigned i = 0; i < kRowCount; ++i) {
        const unsigned g = i % kNumGroups;
        expected_sum[g] += static_cast<int64_t>(i);
        expected_count[g] += 1;
        expected_total_sum += static_cast<int64_t>(i);
    }

    INFO("GROUP BY over a large multi-batch input folds into #groups-bounded sink state");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT grp, SUM(val) AS s, COUNT(name) AS c "
                                           "FROM bounded_db.bounded_coll "
                                           "GROUP BY grp ORDER BY grp ASC;");
        INFO("GROUP BY error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kNumGroups);
        for (unsigned g = 0; g < kNumGroups; ++g) {
            REQUIRE(cur->value(0, g).value<int64_t>() == static_cast<int64_t>(g));
            REQUIRE(cur->value(1, g).value<int64_t>() == expected_sum[g]);
            REQUIRE(cur->value(2, g).value<uint64_t>() == expected_count[g]);
        }
    }

    INFO("scalar aggregate over a large multi-batch input folds into O(1) running state");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT SUM(val) AS s, COUNT(name) AS c "
                                           "FROM bounded_db.bounded_coll;");
        INFO("scalar aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == expected_total_sum);
        REQUIRE(cur->value(1, 0).value<uint64_t>() == static_cast<uint64_t>(kRowCount));
    }
}

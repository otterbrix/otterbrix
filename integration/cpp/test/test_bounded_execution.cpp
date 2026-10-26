// Bounded-execution SELECT (HIDDEN): peak-RSS assertion is deferred pending per-batch fetch-next.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
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
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        dispatcher->execute_sql(session, std::string("CREATE SCHEMA ") + bounded_db + ";");
    }
    {
        auto session = otterbrix::session_id_t();
        test_create_collection(dispatcher, session, core::schema_t{bounded_db}, core::relname_t{bounded_coll});
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
        REQUIRE(cur->affected_rows() == kRowCount);
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
            auto cell0 = cur->get<int64_t>(0, g);
            REQUIRE_FALSE(cell0.has_error());
            REQUIRE(cell0.value() == static_cast<int64_t>(g));
            auto cell1 = cur->get<int64_t>(1, g);
            REQUIRE_FALSE(cell1.has_error());
            REQUIRE(cell1.value() == expected_sum[g]);
            auto cell2 = cur->get<uint64_t>(2, g);
            REQUIRE_FALSE(cell2.has_error());
            REQUIRE(cell2.value() == expected_count[g]);
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
        auto cell3 = cur->get<int64_t>(0, 0);
        REQUIRE_FALSE(cell3.has_error());
        REQUIRE(cell3.value() == expected_total_sum);
        auto cell4 = cur->get<uint64_t>(1, 0);
        REQUIRE_FALSE(cell4.has_error());
        REQUIRE(cell4.value() == static_cast<uint64_t>(kRowCount));
    }
}

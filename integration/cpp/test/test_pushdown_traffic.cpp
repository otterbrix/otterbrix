// Verifies pushdown shrinks agent->coordinator mailbox traffic; value correctness is asserted
// separately, in test_aggregate_pushdown_e2e.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <services/disk/agent_disk.hpp>
#include <sstream>

using namespace test_helpers;

namespace {
    // Exceeds DEFAULT_VECTOR_CAPACITY (1024) so the scan spans multiple batches.
    constexpr unsigned kRowCount = 5000;

    constexpr int64_t kFilterLo = 1000;
    constexpr unsigned kGroups = 4;

    void seed(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& fq_table) {
        auto cur = seed_rows(dispatcher, fq_table, "id, g, v", kRowCount, [](unsigned i) {
            std::stringstream s;
            s << "(" << i << ", " << (i % kGroups) << ", " << i << ")";
            return s.str();
        });
        INFO("seed error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRowCount);
    }
} // namespace

TEST_CASE("integration::cpp::pushdown_traffic::scalar_ships_one_row") {
    auto config = make_test_config(integration_fixture_path("test_pushdown_traffic/scalar"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TrafficDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TrafficDb.t (id bigint, g bigint, v bigint);")->is_success());
    seed(dispatcher, "TrafficDb.t");

    const uint64_t matched = kRowCount - static_cast<unsigned>(kFilterLo);

    services::disk::reset_pushdown_reply_rows();
    {
        std::stringstream q;
        q << "SELECT SUM(v) AS s, COUNT(*) AS c FROM TrafficDb.t WHERE v >= " << kFilterLo << ";";
        auto cur = exec(dispatcher, q.str());
        INFO("scalar aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // Value correctness lives in test_aggregate_pushdown_e2e; this file only asserts traffic.
    }
    const uint64_t reply_rows = services::disk::pushdown_reply_rows();

    REQUIRE(reply_rows == 1);
    REQUIRE(reply_rows < matched);
}

TEST_CASE("integration::cpp::pushdown_traffic::grouped_ships_one_row_per_group") {
    auto config = make_test_config(integration_fixture_path("test_pushdown_traffic/grouped"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TrafficDb;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TrafficDb.t (id bigint, g bigint, v bigint);")->is_success());
    seed(dispatcher, "TrafficDb.t");

    const uint64_t matched = kRowCount - static_cast<unsigned>(kFilterLo);

    services::disk::reset_pushdown_reply_rows();
    {
        std::stringstream q;
        q << "SELECT g, COUNT(*) AS c FROM TrafficDb.t WHERE v >= " << kFilterLo << " GROUP BY g ORDER BY g ASC;";
        auto cur = exec(dispatcher, q.str());
        INFO("grouped aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kGroups);
        // Per-group value correctness lives in test_aggregate_pushdown_e2e.
    }
    const uint64_t reply_rows = services::disk::pushdown_reply_rows();

    REQUIRE(reply_rows == kGroups);
    REQUIRE(reply_rows < matched);
}

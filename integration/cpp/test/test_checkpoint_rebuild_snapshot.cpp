#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/index/manager_index.hpp>

#include <cstdint>
#include <string>
#include <string_view>

// The rebuild a CHECKPOINT owes must read the ALL-COMMITTED snapshot, not the checkpointing statement's own:
// repopulate_table clears every store before refilling it, so a row committed by a neighbour session after
// that snapshot would be invisible to the refill scan and silently dropped from the index while the table
// keeps it -- indexed lookups would answer a SUBSET, which no downstream filter can repair (same contract as
// test_index_delete_horizon).

namespace {
    constexpr int64_t kRows = 2000;
    constexpr int64_t kNewId = 5000;
    constexpr int64_t kNewKey = 50000; // k = 10*id for the seed, so 50000 is outside its range

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }
} // namespace

TEST_CASE("integration::cpp::checkpoint_rebuild_snapshot::a_commit_after_the_statements_snapshot_stays_indexed") {
    auto config = test_create_config(integration_fixture_path("test_checkpoint_rebuild_snapshot"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    // Far above anything this case writes, so an automatic round can't silently repair the very loss under test.
    config.wal.auto_checkpoint_threshold_bytes = 1024ull * 1024ull * 1024ull;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec_fresh = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec_fresh("CREATE DATABASE sdb;")->is_success());
    REQUIRE(exec_fresh("CREATE TABLE sdb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec_fresh("CREATE INDEX k_idx ON sdb.t (k);")->is_success());
    for (int64_t start = 1; start <= kRows; start += 500) {
        std::string sql = "INSERT INTO sdb.t (id, k) VALUES ";
        for (int64_t i = start; i < start + 500 && i <= kRows; ++i) {
            if (i != start) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        sql += ";";
        REQUIRE(exec_fresh(sql)->is_success());
    }

    const std::string indexed = "SELECT id FROM sdb.t WHERE k = " + std::to_string(kNewKey) + ";";
    const std::string control = "SELECT k FROM sdb.t WHERE id = " + std::to_string(kNewId) + ";";

    INFO("the indexed query must be an Index Scan, or this file tests a full scan");
    {
        auto plan = exec_fresh("EXPLAIN " + indexed);
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed query:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }
    INFO("and the control must NOT be, or the control proves nothing");
    {
        auto plan = exec_fresh("EXPLAIN " + control);
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the control query:\n" << text);
        REQUIRE(text.find("Index Scan") == std::string::npos);
    }

    auto session_a = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(session_a, "BEGIN;")->is_success());
    {
        // BEGIN alone pins nothing; this read is what fixes A's snapshot below B's commit.
        auto pin = d->execute_sql(session_a, "SELECT COUNT(id) AS c FROM sdb.t;");
        REQUIRE(pin->is_success());
        REQUIRE(pin->size() == 1);
        REQUIRE(pin->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRows));
    }

    REQUIRE(exec_fresh("INSERT INTO sdb.t (id, k) VALUES (" + std::to_string(kNewId) + ", " + std::to_string(kNewKey) +
                       ");")
                ->is_success());

    INFO("before the round, BOTH routes answer B's row for a fresh session");
    {
        auto cur = exec_fresh(indexed);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }
    {
        auto cur = exec_fresh(control);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
    }

    services::index::reset_index_repopulations();

    REQUIRE(d->execute_sql(session_a, "CHECKPOINT;")->is_success());
    REQUIRE(d->execute_sql(session_a, "COMMIT;")->is_success());

    INFO("NOT VACUOUS: the round must have cleared-and-refilled at least one index");
    REQUIRE(services::index::index_repopulations() > 0);

    INFO("the full scan is the truth: B's commit is real after the round");
    {
        auto cur = exec_fresh(control);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == kNewKey);
    }

    INFO("the indexed route must still be an Index Scan after the round");
    {
        auto plan = exec_fresh("EXPLAIN " + indexed);
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed query:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }

    INFO("and the index must agree with the full scan");
    {
        auto cur = exec_fresh(indexed);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == kNewId);
    }
}

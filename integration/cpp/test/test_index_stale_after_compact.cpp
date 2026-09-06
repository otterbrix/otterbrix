#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/cursor/cursor.hpp>

#include <services/disk/agent_disk.hpp>
#include <services/index/manager_index.hpp>

#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>

// compact() renumbers every surviving row's physical id; a stale index entry then silently
// returns the wrong row or none. Two call paths reach compact() (CHECKPOINT statement, WAL
// auto-checkpoint), one case below per path. EXPLAIN assertions are load-bearing: without them
// a case can pass via a full scan instead of the index. Crash methodology: test_index_rebuild_crash.cpp.

namespace {

    // 3 row groups (> row_group_size 1024 each); deleting the middle third shifts the tail
    // by a full 1000 ids so a stale index cannot accidentally still name the right row.
    constexpr int64_t kRows = 3000;
    constexpr int64_t kDeleteFrom = 1001; // inclusive
    constexpr int64_t kDeleteTo = 2000;   // inclusive
    // A SURVIVOR from the tail: its physical row id moves from 2999 to 1999.
    constexpr int64_t kSurvivorId = 3000;
    constexpr int64_t kSurvivorKey = 10 * kSurvivorId;

    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

    std::string plan_text(const components::cursor::cursor_t_ptr& cur) {
        std::string out;
        for (std::size_t r = 0; r < cur->size(); ++r) {
            auto v = cur->value(0, r);
            out += std::string(v.value<std::string_view>());
            out += '\n';
        }
        return out;
    }

    // INDEXED leg: k carries k_idx.
    std::string indexed_query() { return "SELECT id FROM sdb.t WHERE k = " + std::to_string(kSurvivorKey) + ";"; }

    // UNINDEXED control leg: same table, same row, a column no index covers.
    std::string control_query() { return "SELECT k FROM sdb.t WHERE id = " + std::to_string(kSurvivorId) + ";"; }

    // Same pair against the second case's database.
    std::string auto_indexed_query() { return "SELECT id FROM adb.t WHERE k = " + std::to_string(kSurvivorKey) + ";"; }
    std::string auto_control_query() { return "SELECT k FROM adb.t WHERE id = " + std::to_string(kSurvivorId) + ";"; }

} // namespace

TEST_CASE("integration::cpp::index_stale_after_compact::a_crash_after_a_compacting_checkpoint_keeps_the_index") {
    auto config = test_create_config(integration_fixture_path("test_index_stale_after_compact/orig"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    const std::filesystem::path crash_dir = integration_fixture_path("test_index_stale_after_compact/crashed");

    {
        test_spaces space(config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        REQUIRE(exec("CREATE DATABASE sdb;")->is_success());
        REQUIRE(exec("CREATE TABLE sdb.t (id bigint, k bigint);")->is_success());
        REQUIRE(exec("CREATE INDEX k_idx ON sdb.t (k);")->is_success());

        for (int64_t start = 1; start <= kRows; start += 500) {
            std::string sql = "INSERT INTO sdb.t (id, k) VALUES ";
            for (int64_t i = start; i < start + 500 && i <= kRows; ++i) {
                if (i != start) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
            }
            sql += ";";
            REQUIRE(exec(sql)->is_success());
        }

        INFO("the middle third goes, so the compaction has 1000 ids of shift to hand out");
        REQUIRE(exec("DELETE FROM sdb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                     " AND id <= " + std::to_string(kDeleteTo) + ";")
                    ->is_success());

        INFO("CHECKPOINT: this is what compacts the table and renumbers every surviving row");
        REQUIRE(exec("CHECKPOINT;")->is_success());

        INFO("before the crash the index is right; the question this case asks is whether the restart keeps it");
        {
            auto plan = exec("EXPLAIN " + indexed_query());
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO("pre-crash plan for the indexed query:\n" << text);
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        {
            auto cur = exec(indexed_query());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == kSurvivorId);
        }

        // kill -9 happens here: the compacted .otbx is durable, the rebuilt index is not.
        copy_dir_as_crash(config.main_path, crash_dir);
    } // the destructor's CHECKPOINT runs against the ORIGINAL directory only

    auto crash_config = test_create_config(crash_dir);
    crash_config.wal.on = true;
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* d = space.dispatcher();
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };

        INFO("CONTROL -- the unindexed leg proves the ROW survived the crash");
        {
            auto plan = exec("EXPLAIN " + control_query());
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO("post-crash plan for the unindexed control query:\n" << text);
            REQUIRE(text.find("Index Scan") == std::string::npos);
        }
        {
            auto cur = exec(control_query());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == kSurvivorKey);
        }

        INFO("the survivor must still be reachable THROUGH THE INDEX, or the file is a full-scan test");
        {
            auto plan = exec("EXPLAIN " + indexed_query());
            REQUIRE(plan->is_success());
            const auto text = plan_text(plan);
            INFO("post-crash plan for the indexed query:\n" << text);
            // If the crash caught the index mid-rebuild, bootstrap_index_sync finds nothing to
            // open and the query silently falls back to a Seq Scan instead of failing here.
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        {
            auto cur = exec(indexed_query());
            REQUIRE(cur->is_success());
            // Only commit_inserts' force_flush() before the statement returns makes this
            // survive the crash; without it clear() leaves a 0-row answer.
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == kSurvivorId);
        }

        INFO("and a key from the deleted middle must still be absent");
        {
            auto cur = exec("SELECT id FROM sdb.t WHERE k = " + std::to_string(10 * kDeleteFrom) + ";");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 0);
        }
    }
    std::filesystem::remove_all(crash_dir);
}

// Same defect via manager_wal_replicate_t::run_auto_checkpoint: it mirrored the statement's
// compact steps but not its index rebuild, so an automatic round renumbers indexed tables while
// their indexes keep pre-compact ids. No crash needed. table_checkpoints() guards against a
// vacuous pass if no automatic round ran.
TEST_CASE("integration::cpp::index_stale_after_compact::the_wal_auto_checkpoint_rebuilds_what_it_renumbers") {
    auto config = test_create_config(integration_fixture_path("test_index_stale_after_compact/auto"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;
    // Small enough that a handful of multi-row statements trips it.
    config.wal.auto_checkpoint_threshold_bytes = 8 * 1024;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE adb;")->is_success());
    REQUIRE(exec("CREATE TABLE adb.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX k_idx ON adb.t (k);")->is_success());
    for (int64_t start = 1; start <= kRows; start += 500) {
        std::string sql = "INSERT INTO adb.t (id, k) VALUES ";
        for (int64_t i = start; i < start + 500 && i <= kRows; ++i) {
            if (i != start) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        sql += ";";
        REQUIRE(exec(sql)->is_success());
    }
    REQUIRE(exec("DELETE FROM adb.t WHERE id >= " + std::to_string(kDeleteFrom) +
                 " AND id <= " + std::to_string(kDeleteTo) + ";")
                ->is_success());
    REQUIRE(exec("CHECKPOINT;")->is_success());

    INFO("baseline: after the STATEMENT checkpoint the index is right, so what follows is the automatic one");
    {
        auto cur = exec(auto_indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == kSurvivorId);
    }

    services::disk::reset_table_checkpoints();
    services::index::reset_index_repopulations();

    // Deletes from the FRONT each round (not the tail) -- only that gives the next compaction
    // ids to shift, so an automatic round can be observed.
    int64_t next = 100000;
    int64_t doomed = 1;
    for (int round = 0; round < 200 && services::disk::table_checkpoints() == 0; ++round) {
        std::string sql = "INSERT INTO adb.t (id, k) VALUES ";
        for (int i = 0; i < 50; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(next) + ", " + std::to_string(next) + ")";
            ++next;
        }
        sql += ";";
        REQUIRE(exec(sql)->is_success());
        REQUIRE(exec("DELETE FROM adb.t WHERE id = " + std::to_string(doomed) + ";")->is_success());
        ++doomed;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    // Poll, don't sleep-then-check: MEASURED index_repopulations() lands 41-69 ms after
    // table_checkpoints() becomes non-zero, so a bare sleep_for(500ms) here failed 3/3 runs
    // on `0 > 0`. 30 s deadline matches test_index_stale_marker_crash.cpp.
    const auto wait_started = std::chrono::steady_clock::now();
    {
        const auto deadline = wait_started + std::chrono::seconds(30);
        while ((services::disk::table_checkpoints() == 0 || services::index::index_repopulations() == 0) &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
    INFO("waited " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                           wait_started)
                          .count()
                   << " ms for the automatic round: table_checkpoints=" << services::disk::table_checkpoints()
                   << " index_repopulations=" << services::index::index_repopulations());

    INFO("NOT VACUOUS: without an automatic checkpoint round this case tests nothing");
    REQUIRE(services::disk::table_checkpoints() > 0);

    INFO("the automatic round must have rebuilt what it renumbered");
    CHECK(services::index::index_repopulations() > 0);

    INFO("and the lookup must still go THROUGH the index");
    {
        auto plan = exec("EXPLAIN " + auto_indexed_query());
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the indexed query:\n" << text);
        REQUIRE(text.find("Index Scan") != std::string::npos);
    }
    {
        // Loud-refusal contract: while a compacting auto-round's index rebuild is still in
        // flight the indexed read is REFUSED (stale_index), not answered with the pre-compact
        // stranger id it used to return. Under this test's back-to-back auto rounds a probe can
        // land in that window, so retry the documented remedy ("retry the statement") until a
        // round has settled — then the answer must be the survivor, NEVER a stranger.
        components::cursor::cursor_t_ptr cur;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        do {
            cur = exec(auto_indexed_query());
            if (cur->is_success()) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        } while (std::chrono::steady_clock::now() < deadline);
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == kSurvivorId);
    }

    INFO("CONTROL — the unindexed leg on the same table and the same row must agree");
    {
        auto plan = exec("EXPLAIN " + auto_control_query());
        REQUIRE(plan->is_success());
        const auto text = plan_text(plan);
        INFO("plan for the unindexed control query:\n" << text);
        REQUIRE(text.find("Index Scan") == std::string::npos);
    }
    {
        auto cur = exec(auto_control_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == kSurvivorKey);
    }
}

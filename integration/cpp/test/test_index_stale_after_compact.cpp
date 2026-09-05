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

// WHAT A LOOKUP THROUGH THE INDEX MUST ANSWER ONCE A COMPACTION HAS RENUMBERED THE TABLE
// UNDER IT.
//
// data_table_t::compact rebuilds a table at row id 0 and hands every surviving row a fresh,
// gap-free physical id. An index entry stores that id -- in the agent's tree and in its
// on-disk directory alike -- so the instant a compacting round commits, every index of every
// table it touched is wrong. Both shapes of the failure are SILENT:
//   * an id that names no row group is DROPPED by collection_t::fetch, shortening the answer;
//   * an id that now belongs to a different survivor is gathered as if it were the match, so
//     the query succeeds and returns somebody else's row.
//
// compact() has ONE call site (agent_disk_t::checkpoint_inner, reached only through
// manager_disk_t::checkpoint_all) and TWO orchestrations above it. One case per orchestration,
// because they failed differently:
//
//   1. THE CHECKPOINT STATEMENT (operator_checkpoint_t, and the shutdown checkpoint in
//      ~base_otterbrix_t) always rebuilt, and the rebuild is DURABLE before the statement
//      returns -- btree_index_agent_t::publish_buckets closes commit_inserts with
//      store_.force_flush(). The first case is a guard, not a reproduction: crash straight
//      after a compacting CHECKPOINT, reopen, and the index must still find the row.
//
//   2. THE WAL AUTO-CHECKPOINT (run_auto_checkpoint), which fires commit_txn once the log
//      outgrows wal.auto_checkpoint_threshold_bytes, mirrored the statement's steps and NOT
//      its rebuild -- the second case needs no crash and no restart to show a wrong row.
//
// THE EXPLAIN ASSERTION IS LOAD-BEARING in both, on the SAME query text the row assertions
// use: without it either case goes green through a full scan if the index fails to bootstrap
// or the planner stops routing `WHERE k = ...` to it. Each is paired with an UNINDEXED control
// on the same table and row (k is indexed, id is not), so "the row is there" and "the index
// can find it" stay separate facts.
//
// kill -9 is the mechanism of test_index_rebuild_crash.cpp: COPY the live data directory while
// the engine is up -- the destructor's CHECKPOINT then mutates only the ORIGINAL -- and reopen
// the COPY under a fresh engine. No test lays out files by hand.

namespace {

    // > row_group_size (1024) by a wide margin: 3000 rows span three row groups, and
    // deleting the middle third moves the tail by a full 1000 ids, so a stale index cannot
    // accidentally still name the right row.
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
            // A crash that caught the index directory mid-rebuild leaves nothing for
            // bootstrap_index_sync to open, and the index is then skipped entirely and the
            // same SQL answered by a Seq Scan. That must fail here, not pass quietly.
            REQUIRE(text.find("Index Scan") != std::string::npos);
        }
        {
            auto cur = exec(indexed_query());
            REQUIRE(cur->is_success());
            // The rebuild's clear() drops the on-disk index directory outright; what makes
            // this survive a crash is that commit_inserts force_flush()es the refilled tree
            // before the statement returns. Take that flush away and this is a 0-row answer.
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

// THE SECOND DOOR INTO THE SAME DEFECT, and the one that was actually open.
//
// compact() has one call site (agent_disk_t::checkpoint_inner) but TWO orchestrations
// above it. The CHECKPOINT statement is one; manager_wal_replicate_t::run_auto_checkpoint
// is the other — it fires from commit_txn once the log outgrows
// wal.auto_checkpoint_threshold_bytes, and its own comment calls it the "self-orchestrated
// analogue of the CHECKPOINT statement operator". It mirrored the statement's steps (a)-(d)
// and NOT its index rebuild, so every automatic round renumbered the indexed tables and
// left their indexes holding pre-compact ids.
//
// This needs no crash and no restart to show: with a small threshold and a front-of-table
// delete per round, `WHERE k = <key of the last row>` came back with a DIFFERENT row's id —
// the compaction had moved the row the stale entry named, and collection_t::fetch gathered
// whatever now sits at that physical id. A wrong answer, reported as a success.
//
// The guard against a vacuous pass is table_checkpoints(): if no automatic round ran, the
// case proves nothing and says so instead of going green.
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

    // Churn until an AUTOMATIC round is observed. Each round deletes a row from the FRONT of
    // the table, which is what gives the next compaction a shift to hand out: deleting the
    // tail would renumber nothing and the case would be blind.
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
    // The automatic round is fire-and-forget off commit_txn, so what follows WAITS FOR THE
    // EVENT the two assertions below read, not for a clock. A bare sleep_for(500ms) stood
    // here, and it was load-bearing: MEASURED at this point, table_checkpoints() is already
    // 2 while index_repopulations() is still 0 in every run, and the repopulation lands
    // 41-69 ms later (idle and under a 24-way CPU load alike). With the wait taken out the
    // case fails 3 runs out of 3 on `CHECK(index_repopulations() > 0)` with `0 > 0`. So the
    // question was never whether to wait -- it was whether to wait a guessed 500 ms or
    // until the thing happened. The deadline below is a ceiling on an already-broken run,
    // not the expected wait: the loop leaves the instant both meters are non-zero, which on
    // this machine is the first few polls. 30 s matches the event wait in
    // test_index_stale_marker_crash.cpp.
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
        auto cur = exec(auto_indexed_query());
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        // A stale entry names a pre-compact id, and the row that moved into it comes back
        // instead — id 100000 for a query on row 3000's key.
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

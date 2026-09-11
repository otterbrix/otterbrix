#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <set>
#include <string>

// WAL replay numbers appended rows POSITIONALLY: base_spaces.cpp:311-316 feeds every committed
// PHYSICAL_INSERT chunk to direct_append_sync and DISCARDS the start row it answers with
// (manager_disk_storage.cpp:11,76 -- the value is returned, and manager_disk_bootstrap.cpp:194 even
// says it "answers with the start row, not a count"), while the journalled
// record_t::physical_row_start (record.hpp:42, decoded at wal_binary.cpp:373) is read by nobody in
// production -- a full-tree grep finds only services/wal/tests/test_wal_binary.cpp:59. So the row id
// a replayed row gets is "how many rows the table already holds at replay time", not the id it had
// when it was written.
//
// That is an identity only while every slot AHEAD of the row is replayed too. One transaction still
// IN FLIGHT when the machine dies breaks it: its rows already consumed physical slots
// (collection.cpp:240, state.row_start = total_rows_), a concurrent transaction that committed after
// them was numbered above them, and its index entries were written with those numbers -- but the
// in-flight transaction has no COMMIT marker, so filter_committed_records (wal.hpp:38-74, a marker
// must sit at a STRICTLY GREATER wal id) leaves it out of the replay. The committed rows then replay
// that many slots LOWER than the ids the disk-backed index still holds for them.
//
// WHY AN OPEN TRANSACTION AND NOT A ROLLBACK. A rolled-back INSERT leaves the same hole today, but it
// leaves it only because abort does not revert base-table appends (operator_abort_transaction.cpp:20,
// "Appends still need explicit revert below -- their physical row slots persist on disk"; the abort
// drain keeps table OIDS where the commit drain keeps RANGES, dispatcher.cpp:1064-1067 against
// :1036-1041). Make abort reclaim those slots and a rollback-shaped test goes green with positional
// replay still broken. Nothing can reclaim the slots of a transaction that was never asked to end:
// no abort runs, no commit runs, and the rows of the concurrent COMMITTED transaction sit ABOVE the
// hole, so they cannot be renumbered downwards while they are live and visible. This case therefore
// accuses replay numbering and nothing else.
//
// What it does NOT accuse: the index. The concurrent transaction committed cleanly, its index entries
// name the ids the rows really had, and the pre-crash probe below proves the index answers correctly
// while the engine is up.
//
// The fix it asks for is base_spaces.cpp:311-316 honouring r->physical_row_start. Rebuilding the
// index from the table on restart would also turn it green -- that repairs the symptom, not the
// contract, and should be recognised as such in review.
//
// kill -9 is simulated as in test_index_rebuild_crash.cpp:17-23: COPY the live data directory while
// the engine is up, so the destructor checkpoint mutates only the ORIGINAL, and reopen the COPY.

namespace {

    void copy_dir_as_crash(const std::filesystem::path& from, const std::filesystem::path& to) {
        std::filesystem::remove_all(to);
        std::filesystem::create_directories(to.parent_path());
        std::filesystem::copy(from, to, std::filesystem::copy_options::recursive);
    }

    // 3 slots ahead, then 6 committed rows: the shift is small enough that the committed rows'
    // recorded ids still land INSIDE the replayed table, so the index names a WRONG ROW rather than
    // simply missing -- a symptom no amount of "the index came up empty" can explain away.
    constexpr int64_t kInFlight = 3;
    constexpr int64_t kCommitted = 6;

    // The rows whose identity this test is about: id i carries k = 10*i.
    std::string committed_insert_sql() {
        std::string sql = "INSERT INTO b.t (id, k) VALUES ";
        for (int64_t i = 1; i <= kCommitted; ++i) {
            if (i != 1) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        return sql + ";";
    }

    // Loud ids and keys well outside the committed range: a row carrying these can only come from the
    // transaction that never ended.
    std::string in_flight_insert_sql() {
        std::string sql = "INSERT INTO b.t (id, k) VALUES ";
        for (int64_t i = 1; i <= kInFlight; ++i) {
            if (i != 1) {
                sql += ", ";
            }
            sql += "(" + std::to_string(1000 + i) + ", " + std::to_string(10000 + i) + ")";
        }
        return sql + ";";
    }

    // Seeds the table, opens ONE explicit transaction that inserts kInFlight rows, then commits
    // kCommitted rows from a DIFFERENT session while that transaction is still open, then ends the
    // open transaction with `first_txn_end` -- "" leaves it open, so the crash catches it in flight.
    void seed_and_crash(const std::filesystem::path& orig,
                        const std::filesystem::path& crash,
                        const std::string& first_txn_end) {
        auto config = test_helpers::make_test_config(orig);
        config.log.level = log_t::level::off;

        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE b;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE b.t (id bigint, k bigint);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE INDEX k_idx ON b.t (k);")->is_success());

        // Checkpoint at total_rows = 0: the .otbx holds nothing and its checkpoint floor is here, so
        // every row below is restored by the replay and by nothing else.
        REQUIRE(test_helpers::exec(dispatcher, "CHECKPOINT;")->is_success());

        auto session = otterbrix::session_id_t();
        INFO("session A: BEGIN, then INSERT " << kInFlight << " rows -- they take the first slots");
        REQUIRE(dispatcher->execute_sql(session, "BEGIN;")->is_success());
        REQUIRE(dispatcher->execute_sql(session, in_flight_insert_sql())->is_success());

        INFO("session B, WHILE A IS STILL OPEN: autocommit INSERT of the " << kCommitted << " rows under test");
        {
            auto cur = test_helpers::exec(dispatcher, committed_insert_sql());
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == static_cast<std::size_t>(kCommitted));
        }

        if (!first_txn_end.empty()) {
            INFO("session A ends with " << first_txn_end);
            REQUIRE(dispatcher->execute_sql(session, first_txn_end + ";")->is_success());
        }

        INFO("BEFORE the crash the index answers correctly -- the divergence is made by the replay");
        {
            auto cur = test_helpers::exec(dispatcher, "SELECT id, k FROM b.t WHERE k = 10;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 10);
        }

        copy_dir_as_crash(config.main_path, crash);
    } // the destructor checkpoint runs against the ORIGINAL directory only

} // namespace

// RED: the in-flight transaction's slots are not replayed, so every committed row lands kInFlight
// slots lower than the id the index holds for it, and the index answers with the wrong row.
TEST_CASE("integration::cpp::wal_replay_rowid_drift::an_in_flight_transaction_may_not_renumber_committed_rows") {
    const std::filesystem::path crash_dir = integration_fixture_path("test_wal_replay_rowid_drift/in_flight_crashed");
    seed_and_crash(integration_fixture_path("test_wal_replay_rowid_drift/in_flight_orig"), crash_dir, "");

    auto crash_config = test_create_config(crash_dir);
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* dispatcher = space.dispatcher();

        INFO("anchor: the full scan restores exactly the committed rows, and no in-flight row");
        {
            auto cur = test_helpers::exec(dispatcher, "SELECT id FROM b.t;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == static_cast<std::size_t>(kCommitted));
            std::set<int64_t> ids;
            for (std::size_t r = 0; r < cur->size(); ++r) {
                ids.insert(cur->value(0, r).value<int64_t>());
            }
            for (int64_t i = 1; i <= kCommitted; ++i) {
                CHECK(ids.count(i) == 1);
            }
            for (int64_t i = 1; i <= kInFlight; ++i) {
                CHECK(ids.count(1000 + i) == 0);
            }
        }

        INFO("the TABLE's own answer (no index serves a predicate on id): row id=1 carries k=10");
        {
            auto cur = test_helpers::exec(dispatcher, "SELECT k FROM b.t WHERE id = 1;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            CHECK(cur->value(0, 0).value<int64_t>() == 10);
        }

        // Either answer is legal: the row whose k IS 10, or a refusal. Answering some OTHER row --
        // the one kInFlight slots on, id = 1 + kInFlight -- is the defect, and it is what this
        // assertion exists to forbid.
        INFO("the INDEX's answer for the SAME row: k = 10 answers that row, or refuses");
        {
            auto cur = test_helpers::exec(dispatcher, "SELECT id, k FROM b.t WHERE k = 10;");
            if (cur->is_error()) {
                INFO("refused: " << cur->get_error().what.c_str());
            } else {
                CHECK(cur->size() == 1);
                if (cur->size() == 1) {
                    CHECK(cur->value(0, 0).value<int64_t>() == 1);
                    CHECK(cur->value(1, 0).value<int64_t>() == 10);
                }
            }
        }

        INFO("the top of the shift runs off the end of the table: that row vanishes from the index path");
        {
            auto cur =
                test_helpers::exec(dispatcher, "SELECT id FROM b.t WHERE k = " + std::to_string(10 * kCommitted) + ";");
            if (cur->is_error()) {
                INFO("refused: " << cur->get_error().what.c_str());
            } else {
                CHECK(cur->size() == 1);
                if (cur->size() == 1) {
                    CHECK(cur->value(0, 0).value<int64_t>() == kCommitted);
                }
            }
        }
    }
    std::filesystem::remove_all(crash_dir);
}

// CONTROL: the same interleaving, the same crash copy, the same reopen -- with ONE difference, session
// A COMMITs before the image is taken. Its slots are now replayed too, so the committed rows land on
// the ids the index recorded and every lookup above answers correctly. It must stay GREEN: if it goes
// red, the case above is not about the unreplayed slots but about the crash copy, the WAL replay as
// such, the interleaving, or the index's own recovery.
TEST_CASE("integration::cpp::wal_replay_rowid_drift::the_same_interleaving_committed_keeps_replayed_row_ids") {
    const std::filesystem::path crash_dir = integration_fixture_path("test_wal_replay_rowid_drift/commit_crashed");
    seed_and_crash(integration_fixture_path("test_wal_replay_rowid_drift/commit_orig"), crash_dir, "COMMIT");

    auto crash_config = test_create_config(crash_dir);
    crash_config.log.level = log_t::level::off;
    {
        test_spaces space(crash_config);
        auto* dispatcher = space.dispatcher();

        INFO("both transactions replay, so the table holds every row from both");
        {
            auto cur = test_helpers::exec(dispatcher, "SELECT id FROM b.t;");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == static_cast<std::size_t>(kInFlight + kCommitted));
        }

        INFO("table and index agree on row id=1");
        {
            auto scanned = test_helpers::exec(dispatcher, "SELECT k FROM b.t WHERE id = 1;");
            REQUIRE(scanned->is_success());
            REQUIRE(scanned->size() == 1);
            CHECK(scanned->value(0, 0).value<int64_t>() == 10);

            auto indexed = test_helpers::exec(dispatcher, "SELECT id, k FROM b.t WHERE k = 10;");
            REQUIRE(indexed->is_success());
            CHECK(indexed->size() == 1);
            if (indexed->size() == 1) {
                CHECK(indexed->value(0, 0).value<int64_t>() == 1);
                CHECK(indexed->value(1, 0).value<int64_t>() == 10);
            }
        }

        INFO("and on the last committed row, the one the shift would push off the end");
        {
            auto cur =
                test_helpers::exec(dispatcher, "SELECT id FROM b.t WHERE k = " + std::to_string(10 * kCommitted) + ";");
            REQUIRE(cur->is_success());
            CHECK(cur->size() == 1);
            if (cur->size() == 1) {
                CHECK(cur->value(0, 0).value<int64_t>() == kCommitted);
            }
        }
    }
    std::filesystem::remove_all(crash_dir);
}

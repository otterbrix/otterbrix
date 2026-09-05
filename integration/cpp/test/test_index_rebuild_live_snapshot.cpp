#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// LIVE (no-crash, no-restart) counterpart to test_index_rebuild_crash.cpp: the checkpoint
// rebuild keys entries by physical row id (manager_index.cpp::repopulate_table) precisely so
// a compact() refusal (concurrent open snapshot) does not shift keys after a mid-table tombstone.
//
// Sessions: B opens an explicit txn (the open snapshot), C commits a mid-table
// DELETE, A runs CHECKPOINT. Lookups by keys AFTER the deleted row must resolve
// to the RIGHT rows through the rebuilt index.

TEST_CASE("integration::cpp::index_rebuild_live_snapshot::open_snapshot_checkpoint_shifts_rebuilt_row_ids") {
    auto config = test_create_config(integration_fixture_path("test_index_rebuild_live_snapshot"));
    test_clear_directory(config);
    config.wal.on = true;
    config.log.level = log_t::level::off;

    constexpr int64_t kRows = 2000;

    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE b;")->is_success());
    REQUIRE(exec("CREATE TABLE b.t (id bigint, k bigint);")->is_success());
    REQUIRE(exec("CREATE INDEX k_idx ON b.t (k);")->is_success());

    // 2000 rows, k = 10*id: enough that a mid-table tombstone shifts a long tail.
    for (int64_t start = 1; start <= kRows; start += 500) {
        std::string sql = "INSERT INTO b.t (id, k) VALUES ";
        for (int64_t i = start; i < start + 500 && i <= kRows; i++) {
            if (i != start) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(10 * i) + ")";
        }
        sql += ";";
        REQUIRE(exec(sql)->is_success());
    }

    // Session B: BEGIN plus one read pins an active txn below the upcoming DELETE's
    // commit stamp, so checkpoint_inner's compact() refuses the rebuild.
    auto session_b = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(session_b, "BEGIN;")->is_success());
    {
        auto pin = d->execute_sql(session_b, "SELECT COUNT(id) AS c FROM b.t;");
        REQUIRE(pin->is_success());
        REQUIRE(pin->size() == 1);
        REQUIRE(pin->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRows));
    }

    // Session C: the committed mid-table DELETE (auto-commit).
    REQUIRE(exec("DELETE FROM b.t WHERE id = 1000;")->is_success());

    // compact() is refused (B's snapshot), but the rebuild still runs over the
    // post-DELETE stream.
    REQUIRE(exec("CHECKPOINT;")->is_success());

    {
        auto deleted = exec("SELECT id FROM b.t WHERE k = 10000;");
        REQUIRE(deleted->is_success());
        CHECK(deleted->size() == 0);
    }

    // The LAST row must resolve to itself, not to a row one off from a shift.
    {
        auto last = exec("SELECT id FROM b.t WHERE k = " + std::to_string(10 * kRows) + ";");
        REQUIRE(last->is_success());
        REQUIRE(last->size() == 1);
        CHECK(last->value(0, 0).value<int64_t>() == kRows);
    }

    // The first key after the tombstone: the first candidate to shift into its slot.
    {
        auto shifted = exec("SELECT id FROM b.t WHERE k = 10010;");
        REQUIRE(shifted->is_success());
        REQUIRE(shifted->size() == 1);
        CHECK(shifted->value(0, 0).value<int64_t>() == 1001);
    }

    REQUIRE(d->execute_sql(session_b, "ROLLBACK;")->is_success());
}

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// F2: LIVE (no-crash, no-restart) repro of the index-rebuild row_id shift.
//
// CHECKPOINT's index rebuild drains storage_fetch_next_batch under the checkpoint
// statement's snapshot — a REGULAR, visibility-filtered scan that DROPS invisible
// rows and COMPACTS the survivors' positions. repopulate_table (manager_index.cpp)
// then numbers the re-inserted entries by POSITION in that stream, while
// collection_t::fetch resolves index hits PHYSICALLY. Whenever compact() is
// refused — here: a concurrent session holds an open snapshot, so a committed
// DELETE's version stamp sits above the compact watermark — the tombstone keeps
// its physical slot, position != physical row id, and every key after the
// tombstone points one row low. No crash and no restart needed.
//
// Sessions: B opens an explicit txn (the open snapshot), C commits a mid-table
// DELETE, A runs CHECKPOINT. Lookups by keys AFTER the deleted row must resolve
// to the RIGHT rows through the rebuilt index.

TEST_CASE("integration::cpp::index_rebuild_live_snapshot::open_snapshot_checkpoint_shifts_rebuilt_row_ids") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_index_rebuild_live_snapshot");
    test_clear_directory(config);
    config.disk.on = true;
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

    // Session B: the open snapshot. BEGIN plus one read pins an active txn whose
    // begin id is BELOW the upcoming DELETE's commit stamp, so checkpoint_inner's
    // compact() refuses the rebuild and the tombstone keeps its physical slot.
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

    // Session A: CHECKPOINT. compact() is refused (B's snapshot sits below the
    // DELETE's stamps), but the index rebuild still runs — over a stream that
    // does not contain the deleted row.
    REQUIRE(exec("CHECKPOINT;")->is_success());

    // The deleted key must be gone.
    {
        auto deleted = exec("SELECT id FROM b.t WHERE k = 10000;");
        REQUIRE(deleted->is_success());
        CHECK(deleted->size() == 0);
    }

    // The LAST row must be findable through the rebuilt index — and must be the
    // right row. With the positional rebuild the entry points one row early.
    {
        auto last = exec("SELECT id FROM b.t WHERE k = " + std::to_string(10 * kRows) + ";");
        REQUIRE(last->is_success());
        REQUIRE(last->size() == 1);
        CHECK(last->value(0, 0).value<int64_t>() == kRows);
    }

    // The first key after the tombstone: the first shifted victim. Positionally
    // rebuilt, its entry lands on the tombstone's physical slot — the lookup
    // either resurrects the deleted row or returns nothing.
    {
        auto shifted = exec("SELECT id FROM b.t WHERE k = 10010;");
        REQUIRE(shifted->is_success());
        REQUIRE(shifted->size() == 1);
        CHECK(shifted->value(0, 0).value<int64_t>() == 1001);
    }

    REQUIRE(d->execute_sql(session_b, "ROLLBACK;")->is_success());
}

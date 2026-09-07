#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// The checkpoint rebuild keys entries by physical row id (manager_index.cpp::repopulate_table);
// when compact() is refused because of an open snapshot, keys after a mid-table DELETE must
// still resolve correctly, not shift as if the deleted slot had closed up.

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

    // The read after BEGIN pins the snapshot below the upcoming DELETE's commit stamp.
    auto session_b = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(session_b, "BEGIN;")->is_success());
    {
        auto pin = d->execute_sql(session_b, "SELECT COUNT(id) AS c FROM b.t;");
        REQUIRE(pin->is_success());
        REQUIRE(pin->size() == 1);
        REQUIRE(pin->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRows));
    }

    REQUIRE(exec("DELETE FROM b.t WHERE id = 1000;")->is_success());

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

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <components/table/row_version_manager.hpp>
#include <services/index/manager_index.hpp>
#include <string>

// A probe, not a regression test: how one-row DELETE latency scales with table size, and where
// the difference actually comes from.
//
// A one-row DELETE costs 88x more on a 1M-row table than on a 10k one, but NOT because the commit
// walks every row group and all 1024 version slots of every vector, which was the obvious suspect:
//
//   * version_slots_visited() == 1024 — one vector. The slot walk is already O(affected vectors).
//   * index_repopulations() == 0 — the delete does not rebuild the index. This counter exists
//     because a whole-process profile shows repopulate_table dominating, and that is the SHUTDOWN
//     checkpoint, not the delete; only a counter can tell the two apart.
//
// A profile taken over a sustained delete loop put ~100% of the index agent's samples in
// remove_many -> btree_index_disk_t::force_flush -> btree_t::flush, which fsynced EVERY leaf of
// the disk B+tree: one deleted row fsynced the whole index. That is where the growth came from.
//
// Hidden by default ([.]): it fills a million rows, which no ordinary suite run should pay for.
// Run it explicitly with the [s3probe] tag.
//
// The timings measure the whole statement, not the commit alone — there is no finer instrument —
// so they over-report the fixed per-statement floor equally in both cases, which only makes the
// RATIO more conservative.
namespace {
    void fill(otterbrix::wrapper_dispatcher_t* d, const std::string& table, int rows) {
        constexpr int kBatch = 1000;
        for (int base = 0; base < rows; base += kBatch) {
            std::string sql = "INSERT INTO s3." + table + " (id, v) VALUES ";
            for (int i = 0; i < kBatch; ++i) {
                if (i != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(base + i) + ", 1)";
            }
            sql += ";";
            auto session = otterbrix::session_id_t();
            REQUIRE(d->execute_sql(session, sql)->is_success());
        }
    }
} // namespace

TEST_CASE("integration::cpp::test_s3_commit_scaling::single_row_delete_vs_table_size", "[.][s3probe]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_s3/scaling");
    test_clear_directory(config);
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE s3;")->is_success());
    REQUIRE(exec("CREATE TABLE s3.small (id bigint, v bigint);")->is_success());
    REQUIRE(exec("CREATE TABLE s3.big (id bigint, v bigint);")->is_success());
    fill(d, "small", 10000);
    fill(d, "big", 1000000);
    // WITHOUT an index, "WHERE id = X" scans the whole table, so a statement-level timing would
    // measure the SCAN and attribute it to the commit. Index both tables so the scan is O(1) and
    // what is left to differ is the commit itself.
    REQUIRE(exec("CREATE INDEX small_id ON s3.small (id);")->is_success());
    REQUIRE(exec("CREATE INDEX big_id ON s3.big (id);")->is_success());

    auto delete_one = [&](const std::string& table, int id) {
        const auto start = std::chrono::steady_clock::now();
        auto cur = exec("DELETE FROM s3." + table + " WHERE id = " + std::to_string(id) + ";");
        const auto finish = std::chrono::steady_clock::now();
        REQUIRE(cur->is_success());
        return std::chrono::duration<double, std::micro>(finish - start).count();
    };

    // Warm both paths, then take the median of five.
    delete_one("small", 1);
    delete_one("big", 1);
    std::vector<double> small_us, big_us;
    for (int i = 0; i < 5; ++i) {
        small_us.push_back(delete_one("small", 100 + i));
        big_us.push_back(delete_one("big", 100 + i));
    }
    std::sort(small_us.begin(), small_us.end());
    std::sort(big_us.begin(), big_us.end());

    // Committing the delete of ONE row must visit at most one vector's worth of version slots.
    components::table::reset_version_slots_visited();
    services::index::reset_index_repopulations();
    const auto one_big_us = delete_one("big", 900);
    const auto slots = components::table::version_slots_visited();
    const auto repops = services::index::index_repopulations();
    INFO("ONE delete on the 1M table: " << one_big_us << " us, version slots " << slots << ", index repopulations "
                                        << repops);
    CHECK(repops == 0);
    INFO("version slots visited committing ONE deleted row in a 1M-row table: " << slots);
    CHECK(slots <= components::vector::DEFAULT_VECTOR_CAPACITY);

    // Same predicate, same table, same index — but a SELECT. If the SELECT is fast while the
    // DELETE is not, the cost is not the commit and not the index: it is that DML never uses an
    // index scan and always walks the table.
    auto select_one = [&](const std::string& table, int id) {
        const auto start = std::chrono::steady_clock::now();
        auto cur = exec("SELECT id FROM s3." + table + " WHERE id = " + std::to_string(id) + ";");
        const auto finish = std::chrono::steady_clock::now();
        REQUIRE(cur->is_success());
        return std::chrono::duration<double, std::micro>(finish - start).count();
    };
    select_one("big", 500);
    const auto sel_big = select_one("big", 501);
    const auto sel_small = select_one("small", 501);
    INFO("SELECT by the same indexed predicate: 10k rows = " << sel_small << " us, 1M rows = " << sel_big << " us");

    // CONTROL: the same DELETE on the same 1M-row table with the index DROPPED. If the scaling
    // comes from flushing the whole disk B+tree on every statement, removing the index removes
    // the scaling — even though the predicate now needs a full scan, which is strictly MORE work.
    REQUIRE(exec("DROP INDEX s3.big.big_id;")->is_success());
    REQUIRE(exec("DROP INDEX s3.small.small_id;")->is_success());
    delete_one("big", 3000);
    std::vector<double> small_noidx, big_noidx;
    for (int i = 0; i < 3; ++i) {
        small_noidx.push_back(delete_one("small", 3100 + i));
        big_noidx.push_back(delete_one("big", 3100 + i));
    }
    std::sort(small_noidx.begin(), small_noidx.end());
    std::sort(big_noidx.begin(), big_noidx.end());
    INFO("CONTROL, index dropped — one-row DELETE: 10k rows = " << small_noidx[1] << " us, 1M rows = " << big_noidx[1]
                                                                << " us, ratio = " << (big_noidx[1] / small_noidx[1]));
    CHECK(big_noidx[1] > 0.0);

    INFO("S3 start condition — one-row DELETE: 10k rows = " << small_us[2] << " us, 1M rows = " << big_us[2]
                                                            << " us, ratio = " << (big_us[2] / small_us[2]));
    CHECK(small_us[2] > 0.0);
    CHECK(big_us[2] > 0.0);
}

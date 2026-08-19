#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <string>

// Phase 2 start condition for S3: does committing the delete of ONE row cost more as the table
// grows? The plan starts S3 only if a 10k-row table and a 1M-row table differ by more than an
// order of magnitude, because that is what "commit walks every row group" would look like.
//
// Hidden by default ([.]): it fills a million rows, which no ordinary suite run should pay for.
// Run it explicitly with the [s3probe] tag.
//
// This measures the whole statement, not the commit alone — there is no finer instrument today —
// so it over-reports the fixed per-statement floor equally in both cases, which only makes the
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
    config.disk.on = true;
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

    INFO("S3 start condition — one-row DELETE: 10k rows = " << small_us[2] << " us, 1M rows = " << big_us[2]
                                                            << " us, ratio = " << (big_us[2] / small_us[2]));
    CHECK(small_us[2] > 0.0);
    CHECK(big_us[2] > 0.0);
}

#include "test_config.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <string>
#include <vector>

// The per-statement floor must not swallow the work a statement does.
//
// A single-row INSERT used to cost ~3.5 ms of which ~0.65 ms was work: the executor parked "busy and
// ready" and only a watchdog poke revived it, so the floor dominated everything. The guard has to
// survive CI runners, which are slower and shared, so it cannot assert microseconds — the first
// version of this test did, with a 2000 us bound, and failed on every CI machine while passing
// locally.
//
// Instead it compares two statements measured in the SAME process: one row, and kBatchRows rows.
// Both scale with the machine, so their RATIO does not. Modelling t(n) = floor + n*c, the ratio is
// (floor + N*c) / (floor + c): it collapses toward 1 as the floor grows and rises as the floor
// shrinks.
//
// Measured on one machine, all three at N = 10000:
//   fixed                        12.10, 12.12
//   S1 reverted in all four pumps  8.61, 8.69
//   the original defect (floor 3470 us)  ~4.6, from the model and the recorded numbers
//
// The bound is 6.0, which sits below the original defect and well above nothing else. Be honest
// about what that buys: it catches the floor COMING BACK, not a 1.5x drift — today's revert still
// scores 8.6 because the branch removed other costs that used to sit on top of the floor, so the
// defect now shows as 1.5x rather than the 6.6x originally recorded. A bound tight enough to catch
// 1.5x would not survive a shared runner, and a test that flaps is worse than one that is honest
// about its resolution.

TEST_CASE("integration::cpp::test_statement_latency::single_statement_is_not_quantized") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_statement_latency/single");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    // The default level is trace and the logger writes synchronously to stdout AND a
    // file, which would dwarf what this test measures.
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE lat;")->is_success());
    REQUIRE(exec("CREATE TABLE lat.t (id bigint, payload bigint);")->is_success());

    constexpr int kWarmup = 30;
    constexpr int kMeasured = 300;

    for (int i = 0; i < kWarmup; ++i) {
        REQUIRE(exec("INSERT INTO lat.t (id, payload) VALUES (" + std::to_string(i) + ", 0);")->is_success());
    }

    std::vector<double> single_samples;
    auto median_of = [&](int samples, int rows_per_stmt, int id_base) {
        std::vector<double> us;
        us.reserve(static_cast<size_t>(samples));
        int id = id_base;
        for (int i = 0; i < samples; ++i) {
            std::string sql = "INSERT INTO lat.t (id, payload) VALUES ";
            for (int r = 0; r < rows_per_stmt; ++r) {
                if (r != 0) {
                    sql += ", ";
                }
                sql += "(" + std::to_string(id++) + ", 0)";
            }
            sql += ";";
            const auto start = std::chrono::steady_clock::now();
            auto cursor = exec(sql);
            const auto finish = std::chrono::steady_clock::now();
            REQUIRE(cursor->is_success());
            us.push_back(std::chrono::duration<double, std::micro>(finish - start).count());
        }
        std::sort(us.begin(), us.end());
        if (rows_per_stmt == 1) {
            single_samples = us;
        }
        return us[us.size() / 2];
    };

    constexpr int kBatchRows = 10000;
    const double single_us = median_of(kMeasured, 1, 100000);
    const double batch_us = median_of(15, kBatchRows, 500000);
    const double ratio = batch_us / single_us;
    auto pct = [&](double q) {
        const auto idx = static_cast<size_t>(q * static_cast<double>(single_samples.size() - 1));
        return single_samples[idx];
    };
    // The whole distribution, not just the middle: a per-statement floor imposed by a watchdog poke
    // shows up as a hard lower bound, which the minimum sees and the median does not.
    WARN("PROBE single-row distribution us: min " << single_samples.front() << " p10 " << pct(0.10)
                                                  << " p25 " << pct(0.25) << " median " << pct(0.50)
                                                  << " p75 " << pct(0.75) << " p90 " << pct(0.90)
                                                  << " max " << single_samples.back());
    INFO("single-row " << single_us << " us, " << kBatchRows << "-row " << batch_us
                       << " us, ratio " << ratio);
    CHECK(ratio > 6.0);
}

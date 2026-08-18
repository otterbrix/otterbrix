#include "test_config.hpp"

#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <string>
#include <vector>

// A single-row INSERT costs ~3.5 ms of which ~0.65 ms is actual work. The rest is
// handoff latency: a statement crosses ~20 actor boundaries, and each manager pump
// discovers that an awaited future became ready only on its next idle tick, because
// resolving a future writes a flag and notifies nobody (pump_cv_ is notified from
// enqueue_impl alone). Every hop therefore rounds up to the idle quantum.
//
// Established by experiment, not inference: shrinking the CLIENT poll quantum moved
// p50 by 7% (3470 -> 3233 us), while shrinking the four manager pump quanta moved it
// to 611 us at 5 us and 599 us at 1 us — a plateau, meaning everything above ~600 us
// was pure quantization. Bulk load went 874 -> 307 ms on the same experiment.
//
// This test is a TIMING test on purpose. The defect is a duration, and the fix
// changes a duration: wake-ups by timeout exist both before and after, they are just
// shorter, so no counter can express it. The bound sits between the two measured
// regimes with ~3x margin on each side, and the median of many statements is used so
// a single scheduling hiccup cannot decide the outcome.
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

    std::vector<double> latencies_us;
    latencies_us.reserve(kMeasured);
    for (int i = 0; i < kMeasured; ++i) {
        const auto sql = "INSERT INTO lat.t (id, payload) VALUES (" + std::to_string(kWarmup + i) + ", 1);";
        const auto start = std::chrono::steady_clock::now();
        auto cursor = exec(sql);
        const auto finish = std::chrono::steady_clock::now();
        REQUIRE(cursor->is_success());
        latencies_us.push_back(std::chrono::duration<double, std::micro>(finish - start).count());
    }

    std::sort(latencies_us.begin(), latencies_us.end());
    const double median_us = latencies_us[latencies_us.size() / 2];

    INFO("median single-INSERT latency: " << median_us << " us");
    CHECK(median_us < 2000.0); // quantized: ~3500 us; unquantized: ~600 us
}

// Regression test for the benchmark-runner disk-reset.
//
// The runner points every disk instance at the same current_path()/"disk"
// (+ "/wal") directory. Each benchmark's load() does CREATE TABLE IF NOT EXISTS
// followed by an appending @load_csv, so before the fix a second run over a
// persisted disk re-loaded the CSV and doubled the row count (this is the k^2
// harness bug: 60k -> 120k -> 180k ...). run_single now removes the persisted
// disk/wal dirs before opening each instance.
//
// This test drives the real public runner path (run() -> run_single) with a
// tiny self-contained benchmark loaded twice. Without the reset: the second
// run re-loads onto the first run's 3 persisted rows -> 6 rows -> the
// @expected_rows=3 check fails -> the result CSV's verified column is FAIL.
// With the reset: the second run starts empty -> 3 rows -> both OK.
#include <catch2/catch_test_macros.hpp>

#include "benchmark_configuration.hpp"
#include "benchmark_runner.hpp"
#include "sql_benchmark.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <system_error>

using namespace otterbrix::benchmark;

TEST_CASE("benchmark::runner::disk_reset_prevents_accumulation") {
    namespace fs = std::filesystem;

    const auto cwd = fs::current_path();
    const auto disk_dir = cwd / "disk";
    const auto wal_dir = cwd / "wal";
    const auto bench_dir = cwd / "reset_bench_tmp";

    // Clean slate: the runner's disk/wal live under current_path(), so pre-clean
    // them (and any leftover fixture) so the FIRST load starts from an empty
    // table irrespective of prior process state. This pre-clean is the test's own
    // hygiene — the second load is where the runner's reset is exercised.
    std::error_code ec;
    fs::remove_all(disk_dir, ec);
    fs::remove_all(wal_dir, ec);
    fs::remove_all(bench_dir, ec);
    fs::create_directories(bench_dir);

    // 3-row pipe-delimited CSV (matches the @load_csv default delimiter).
    {
        std::ofstream csv(bench_dir / "data.csv");
        csv << "id|val\n"
               "1|10\n"
               "2|20\n"
               "3|30\n";
    }
    // _setup.sql: declare the CSV load and create the table.
    {
        std::ofstream setup(bench_dir / "_setup.sql");
        setup << "-- @database resetdb\n"
                 "-- @load_csv data.csv items |\n"
                 "CREATE TABLE IF NOT EXISTS items (id bigint, val bigint);\n";
    }
    // q.sql: the benchmark query. Its row count equals the single-load table
    // size, so accumulation trips the @expected_rows check.
    const auto query_path = bench_dir / "q.sql";
    {
        std::ofstream q(query_path);
        q << "-- @expected_rows 3\n"
             "SELECT id, val FROM items;\n";
    }

    benchmark_configuration_t config;
    config.nruns = 1;
    const auto csv_out = (bench_dir / "result.csv").string();
    config.output_file = csv_out;

    // Register the SAME benchmark twice: run() drives run_single once per entry.
    // The 2nd run_single opens the disk the 1st persisted — without the reset it
    // re-loads and the table doubles (3 -> 6), tripping @expected_rows.
    benchmark_runner_t runner;
    runner.load_single_benchmark(query_path);
    runner.load_single_benchmark(query_path);

    runner.run(config);

    // The verified column (last field) of the result CSV must be OK for BOTH
    // runs. Without the reset the 2nd run reports FAIL (6 rows != expected 3).
    std::ifstream result(csv_out);
    REQUIRE(result.is_open());
    std::string line;
    std::getline(result, line); // header
    size_t data_rows = 0;
    size_t fails = 0;
    while (std::getline(result, line)) {
        if (line.empty()) {
            continue;
        }
        ++data_rows;
        if (line.find(",FAIL") != std::string::npos) {
            ++fails;
        }
    }
    result.close();

    // Cleanup regardless of outcome.
    fs::remove_all(disk_dir, ec);
    fs::remove_all(wal_dir, ec);
    fs::remove_all(bench_dir, ec);

    REQUIRE(data_rows == 2);
    INFO("Both benchmark runs must verify OK; a FAIL means the disk was not reset "
         "and rows accumulated across loads (3 -> 6).");
    REQUIRE(fails == 0);
}

// Regression: run_single now clears the persisted disk/wal before each instance; without it, a
// second run over a persisted disk re-loaded its CSV and doubled the row count (k^2 harness bug:
// 60k -> 120k -> 180k ...).
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

    // This pre-clean is the test's own hygiene; the second load below is where run_single's
    // own reset is exercised.
    std::error_code ec;
    fs::remove_all(disk_dir, ec);
    fs::remove_all(wal_dir, ec);
    fs::remove_all(bench_dir, ec);
    fs::create_directories(bench_dir);

    {
        std::ofstream csv(bench_dir / "data.csv");
        csv << "id|val\n"
               "1|10\n"
               "2|20\n"
               "3|30\n";
    }
    {
        std::ofstream setup(bench_dir / "_setup.sql");
        setup << "-- @database resetdb\n"
                 "-- @load_csv data.csv items |\n"
                 "CREATE TABLE IF NOT EXISTS items (id bigint, val bigint);\n";
    }
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

    benchmark_runner_t runner;
    runner.load_single_benchmark(query_path);
    runner.load_single_benchmark(query_path);

    runner.run(config);

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

    fs::remove_all(disk_dir, ec);
    fs::remove_all(wal_dir, ec);
    fs::remove_all(bench_dir, ec);

    REQUIRE(data_rows == 2);
    INFO("Both benchmark runs must verify OK; a FAIL means the disk was not reset "
         "and rows accumulated across loads (3 -> 6).");
    REQUIRE(fails == 0);
}

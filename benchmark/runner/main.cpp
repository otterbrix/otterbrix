#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>

#include "benchmark_configuration.hpp"
#include "benchmark_runner.hpp"

namespace {

void print_usage() {
    std::cout << "Usage: benchmark_runner [OPTIONS] [pattern]\n"
              << "\n"
              << "Options:\n"
              << "  --list              List available benchmarks\n"
              << "  --list-groups       List available groups with counts\n"
              << "  --group=NAME        Filter by group (regex)\n"
              << "  --info              Show benchmark descriptions\n"
              << "  --query             Show SQL queries\n"
              << "  --out=FILE          Write CSV results to file\n"
              << "  --runs=N            Override number of runs\n"
              << "  --timeout=N         Timeout per benchmark (seconds)\n"
              << "  --benchmarks=DIR    Directory with .benchmark/.sql files\n"
              << "  --file=PATH         Run a single .benchmark or .sql file\n"
              << "  --disk              Enable disk persistence\n"
              << "  --wal               Enable WAL\n"
              << "  --config=FILE       Load benchmark config (enable/disable benchmarks)\n"
              << "  --generate-config=FILE  Generate config file from loaded benchmarks\n"
              << "  --skip-load         Skip setup/load phase (use with --disk)\n"
              << "  --load-only         Only run setup/load, then exit (use with --disk)\n"
              << "  --verbose           Verbose output\n"
              << "  --help              Show this help\n"
              << "  [pattern]           Regex filter for benchmark names\n"
              << "\n"
              << "Examples:\n"
              << "  benchmark_runner                              # Run all\n"
              << "  benchmark_runner \"tpch/q01\"                   # Run TPC-H Q1\n"
              << "  benchmark_runner --group=tpch                 # Run all TPC-H\n"
              << "  benchmark_runner --group=micro \"select.*\"     # Run micro/select*\n"
              << "  benchmark_runner --list-groups                # Show suites\n"
              << "  benchmark_runner --file=benchmarks/tpch/q01.benchmark\n"
              << "  benchmark_runner --list --group=ssb           # List SSB only\n"
              << "  benchmark_runner --runs=20 --out=res.csv      # 20 runs, CSV\n";
}

} // namespace

int main(int argc, char* argv[]) {
    otterbrix::benchmark::benchmark_configuration_t config;
    std::string benchmarks_dir = "benchmarks";

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            print_usage();
            return 0;
        } else if (arg == "--list") {
            config.list_only = true;
        } else if (arg == "--list-groups") {
            config.list_groups = true;
        } else if (arg.starts_with("--group=")) {
            config.group_pattern = arg.substr(8);
        } else if (arg.starts_with("--file=")) {
            config.single_file = arg.substr(7);
        } else if (arg == "--info") {
            config.show_info = true;
        } else if (arg == "--query") {
            config.show_query = true;
        } else if (arg == "--disk") {
            config.disk_on = true;
        } else if (arg == "--wal") {
            config.wal_on = true;
        } else if (arg == "--skip-load") {
            config.skip_load = true;
        } else if (arg == "--load-only") {
            config.load_only = true;
        } else if (arg == "--verbose" || arg == "-v") {
            config.verbose = true;
        } else if (arg.starts_with("--out=")) {
            config.output_file = arg.substr(6);
        } else if (arg.starts_with("--runs=")) {
            config.nruns = std::stoull(arg.substr(7));
        } else if (arg.starts_with("--timeout=")) {
            config.timeout_seconds = std::stoull(arg.substr(10));
        } else if (arg.starts_with("--config=")) {
            config.config_file = arg.substr(9);
        } else if (arg.starts_with("--generate-config=")) {
            config.generate_config = arg.substr(18);
        } else if (arg.starts_with("--benchmarks=")) {
            benchmarks_dir = arg.substr(13);
        } else if (!arg.starts_with("-")) {
            config.name_pattern = arg;
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage();
            return 1;
        }
    }

    otterbrix::benchmark::benchmark_runner_t runner;

    if (!config.single_file.empty()) {
        auto file_path = std::filesystem::path(config.single_file);
        if (!file_path.is_absolute()) {
            file_path = std::filesystem::current_path() / file_path;
        }
        runner.load_single_benchmark(file_path);
    } else {
        auto dir = std::filesystem::path(benchmarks_dir);
        if (!dir.is_absolute()) {
            dir = std::filesystem::path(argv[0]).parent_path() / dir;
        }
        runner.load_benchmarks_from_directory(dir);
    }

    if (!config.generate_config.empty()) {
        runner.generate_config_file(config.generate_config);
        return 0;
    }

    if (!config.config_file.empty()) {
        runner.apply_config_file(config.config_file);
    }

    runner.run(config);

    return 0;
}

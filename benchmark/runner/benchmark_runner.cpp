#include "benchmark_runner.hpp"

#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <regex>
#include <set>

#include <components/configuration/configuration.hpp>
#include <integration/cpp/base_spaces.hpp>

#include "interpreted_benchmark.hpp"
#include "sql_benchmark.hpp"

namespace otterbrix::benchmark {

namespace {

class benchmark_instance_t final : public base_otterbrix_t {
public:
    explicit benchmark_instance_t(const benchmark_configuration_t& config)
        : base_otterbrix_t(make_config(config)) {}

private:
    static configuration::config make_config(const benchmark_configuration_t& config) {
        auto cfg = configuration::config::default_config();
        cfg.log.level = log_t::level::off;
        cfg.disk.on = config.disk_on;
        cfg.wal.on = config.wal_on;
        cfg.wal.sync_to_disk = config.disk_on;
        return cfg;
    }
};

} // namespace

void benchmark_runner_t::register_benchmark(std::unique_ptr<benchmark_t> bench) {
    benchmarks_.push_back(std::move(bench));
}

void benchmark_runner_t::load_benchmarks_from_directory(const std::filesystem::path& dir) {
    if (!std::filesystem::exists(dir)) {
        std::cerr << "Benchmark directory not found: " << dir.string() << "\n";
        return;
    }

    std::vector<std::filesystem::path> paths;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
        if (entry.is_regular_file()) {
            auto ext = entry.path().extension();
            if (ext == ".benchmark" || ext == ".sql") {
                // Skip _setup.sql files — they are loaded by sql_benchmark_t, not run as benchmarks
                if (entry.path().filename() == "_setup.sql") {
                    continue;
                }
                paths.push_back(entry.path());
            }
        }
    }
    std::sort(paths.begin(), paths.end());

    for (const auto& path : paths) {
        try {
            if (path.extension() == ".sql") {
                load_sql_file(path, dir);
            } else {
                benchmarks_.push_back(std::make_unique<interpreted_benchmark_t>(path));
            }
        } catch (const std::exception& e) {
            std::cerr << "Error loading " << path.string() << ": " << e.what() << "\n";
        }
    }
}

void benchmark_runner_t::load_single_benchmark(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) {
        std::cerr << "Benchmark file not found: " << path.string() << "\n";
        return;
    }
    try {
        if (path.extension() == ".sql") {
            load_sql_file(path, path.parent_path());
        } else {
            benchmarks_.push_back(std::make_unique<interpreted_benchmark_t>(path));
        }
    } catch (const std::exception& e) {
        std::cerr << "Error loading " << path.string() << ": " << e.what() << "\n";
    }
}

void benchmark_runner_t::load_sql_file(const std::filesystem::path& path,
                                       const std::filesystem::path& base_dir) {
    auto benchmarks = sql_benchmark_t::load_from_file(path, base_dir);
    for (auto& b : benchmarks) {
        benchmarks_.push_back(std::move(b));
    }
}

void benchmark_runner_t::generate_config_file(const std::filesystem::path& path) const {
    std::ofstream out(path);
    if (!out.is_open()) {
        std::cerr << "Error: cannot open " << path.string() << " for writing\n";
        return;
    }

    out << "# Generated benchmark configuration\n";
    out << "# Comment lines with # to disable benchmarks\n";

    // Group benchmarks by group()
    std::map<std::string, std::vector<std::string>> groups;
    for (const auto& b : benchmarks_) {
        groups[b->group()].push_back(b->name());
    }

    for (const auto& [group, names] : groups) {
        out << "\n# === " << group << " === (" << names.size() << " benchmarks)\n";
        for (const auto& name : names) {
            out << name << "\n";
        }
    }

    std::cout << "Generated config: " << path.string() << " (" << benchmarks_.size() << " benchmarks)\n";
}

void benchmark_runner_t::apply_config_file(const std::filesystem::path& path) {
    std::ifstream in(path);
    if (!in.is_open()) {
        std::cerr << "Error: cannot open config file " << path.string() << "\n";
        return;
    }

    std::set<std::string> enabled;
    std::string line;
    while (std::getline(in, line)) {
        // Trim leading/trailing whitespace
        auto start = line.find_first_not_of(" \t");
        if (start == std::string::npos) continue;
        auto end = line.find_last_not_of(" \t");
        line = line.substr(start, end - start + 1);

        if (line.empty() || line[0] == '#') continue;
        enabled.insert(line);
    }

    auto total = benchmarks_.size();
    benchmarks_.erase(
        std::remove_if(benchmarks_.begin(), benchmarks_.end(),
                       [&enabled](const std::unique_ptr<benchmark_t>& b) {
                           return enabled.find(b->name()) == enabled.end();
                       }),
        benchmarks_.end());

    auto kept = benchmarks_.size();
    std::cout << "Config: " << kept << " enabled, " << (total - kept) << " disabled out of " << total << " total\n";
}

bool benchmark_runner_t::matches_filter(const benchmark_t& bench, const benchmark_configuration_t& config) {
    // Check group filter
    if (!config.group_pattern.empty()) {
        try {
            std::regex pattern(config.group_pattern);
            if (!std::regex_search(bench.group(), pattern)) return false;
        } catch (const std::regex_error&) {
            if (bench.group().find(config.group_pattern) == std::string::npos) return false;
        }
    }

    // Check name filter
    if (!config.name_pattern.empty()) {
        try {
            std::regex pattern(config.name_pattern);
            if (!std::regex_search(bench.name(), pattern)) return false;
        } catch (const std::regex_error&) {
            if (bench.name().find(config.name_pattern) == std::string::npos) return false;
        }
    }

    return true;
}

void benchmark_runner_t::run(const benchmark_configuration_t& config) {
    // List groups mode
    if (config.list_groups) {
        std::map<std::string, size_t> groups;
        for (auto& b : benchmarks_) {
            groups[b->group()]++;
        }
        for (const auto& [group, count] : groups) {
            std::cout << std::left << std::setw(30) << group << count << " benchmarks\n";
        }
        std::cout << "\nTotal: " << benchmarks_.size() << " benchmarks in " << groups.size() << " groups\n";
        return;
    }

    std::vector<benchmark_t*> filtered;
    for (auto& b : benchmarks_) {
        if (matches_filter(*b, config)) {
            filtered.push_back(b.get());
        }
    }

    if (filtered.empty()) {
        std::cout << "No benchmarks matched.\n";
        return;
    }

    if (config.list_only) {
        for (auto* b : filtered) {
            std::cout << std::left << std::setw(45) << b->name() << "[" << b->group() << "]\n";
        }
        std::cout << "\n" << filtered.size() << " benchmarks\n";
        return;
    }

    if (config.show_info) {
        for (auto* b : filtered) {
            std::cout << b->name() << "\n";
            std::cout << "  Group:       " << b->group() << "\n";
            std::cout << "  Description: " << b->description() << "\n";
            std::cout << "  Runs:        " << b->nruns() << "\n";
            std::cout << "\n";
        }
        return;
    }

    if (config.show_query) {
        for (auto* b : filtered) {
            std::cout << "-- " << b->name() << "\n";
            std::cout << b->query() << "\n\n";
        }
        return;
    }

    // Load-only mode: create one shared instance, run load() for first benchmark per group, then exit
    if (config.load_only) {
        benchmark_instance_t instance(config);
        benchmark_state_t state;
        state.dispatcher = instance.dispatcher();
        state.session = session_id_t();

        std::set<std::string> loaded_groups;
        for (auto* b : filtered) {
            if (loaded_groups.count(b->group())) {
                continue;
            }
            loaded_groups.insert(b->group());
            if (config.verbose) {
                std::cout << "Loading data for group: " << b->group() << " (via " << b->name() << ")\n";
            }
            try {
                b->load(state);
                std::cout << "Loaded group: " << b->group() << "\n";
            } catch (const std::exception& e) {
                std::cerr << "Error loading group " << b->group() << ": " << e.what() << "\n";
            }
        }
        std::cout << "Load-only complete. " << loaded_groups.size() << " groups loaded.\n";
        return;
    }

    std::ofstream csv_file;
    if (!config.output_file.empty()) {
        csv_file.open(config.output_file);
        if (csv_file.is_open()) {
            csv_file << "name,group,nruns,min_ms,max_ms,avg_ms,median_ms,verified\n";
        }
    }

    report_header(std::cout);

    for (auto* b : filtered) {
        auto result = run_single(*b, config);
        report_result(result, std::cout);

        if (csv_file.is_open()) {
            csv_file << std::fixed << std::setprecision(3) << result.name << "," << result.group << ","
                     << result.nruns << "," << result.min_ms() << "," << result.max_ms() << "," << result.avg_ms()
                     << "," << result.median_ms() << "," << (result.verified ? "OK" : "FAIL") << "\n";
        }
    }
}

benchmark_result_t benchmark_runner_t::run_single(benchmark_t& bench, const benchmark_configuration_t& config) {
    benchmark_result_t result;
    result.name = bench.name();
    result.group = bench.group();

    auto nruns = config.nruns > 0 ? config.nruns : bench.nruns();
    result.nruns = nruns;

    if (config.verbose) {
        std::cout << "  Loading data for " << bench.name() << "...\n";
    }

    try {
        benchmark_instance_t instance(config);
        benchmark_state_t state;
        state.dispatcher = instance.dispatcher();
        state.session = session_id_t();

        if (!config.skip_load) {
            bench.load(state);
        }

        // Warmup
        if (config.verbose) {
            std::cout << "  Warmup run...\n";
        }
        bench.run(state);

        // Timed runs
        for (uint64_t i = 0; i < nruns; ++i) {
            auto start = std::chrono::high_resolution_clock::now();
            bench.run(state);
            auto end = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration<double, std::milli>(end - start);
            result.timings_ms.push_back(duration.count());

            if (config.verbose) {
                std::cout << "  Run " << (i + 1) << "/" << nruns << ": " << std::fixed << std::setprecision(3)
                          << duration.count() << " ms\n";
            }
        }

        // Verify
        auto verify_err = bench.verify(state);
        if (!verify_err.empty()) {
            result.verified = false;
            result.error = verify_err;
        }

        bench.cleanup(state);

    } catch (const std::exception& e) {
        result.error = e.what();
        result.verified = false;
    }

    return result;
}

void benchmark_runner_t::report_header(std::ostream& out) {
    out << std::left << std::setw(45) << "Benchmark" << std::right << std::setw(8) << "Runs" << std::setw(12)
        << "Min (ms)" << std::setw(12) << "Max (ms)" << std::setw(12) << "Avg (ms)" << std::setw(12) << "Median"
        << std::setw(10) << "Status"
        << "\n";
    out << std::string(111, '-') << "\n";
}

void benchmark_runner_t::report_result(const benchmark_result_t& result, std::ostream& out) {
    out << std::left << std::setw(45) << result.name << std::right << std::setw(8) << result.nruns;

    if (result.timings_ms.empty()) {
        out << std::setw(12) << "-" << std::setw(12) << "-" << std::setw(12) << "-" << std::setw(12) << "-";
    } else {
        out << std::fixed << std::setprecision(3) << std::setw(12) << result.min_ms() << std::setw(12)
            << result.max_ms() << std::setw(12) << result.avg_ms() << std::setw(12) << result.median_ms();
    }

    if (!result.error.empty()) {
        out << std::setw(10) << "FAIL";
        out << "\n  Error: " << result.error;
    } else {
        out << std::setw(10) << (result.verified ? "OK" : "FAIL");
    }
    out << "\n";
}

} // namespace otterbrix::benchmark

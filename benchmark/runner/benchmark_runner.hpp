#pragma once

#include <filesystem>
#include <memory>
#include <ostream>
#include <vector>

#include "benchmark.hpp"
#include "benchmark_configuration.hpp"

namespace otterbrix::benchmark {

class benchmark_runner_t {
public:
    void register_benchmark(std::unique_ptr<benchmark_t> bench);
    void load_benchmarks_from_directory(const std::filesystem::path& dir);
    void load_single_benchmark(const std::filesystem::path& path);

    void run(const benchmark_configuration_t& config);

    void generate_config_file(const std::filesystem::path& path) const;
    void apply_config_file(const std::filesystem::path& path);

private:
    bool matches_filter(const benchmark_t& bench, const benchmark_configuration_t& config);
    benchmark_result_t run_single(benchmark_t& bench, const benchmark_configuration_t& config);
    void report_header(std::ostream& out);
    void report_result(const benchmark_result_t& result, std::ostream& out);
    void load_sql_file(const std::filesystem::path& path, const std::filesystem::path& base_dir);

    std::vector<std::unique_ptr<benchmark_t>> benchmarks_;
};

} // namespace otterbrix::benchmark

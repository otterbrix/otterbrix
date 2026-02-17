#pragma once

#include <filesystem>
#include <vector>

#include "benchmark.hpp"

namespace otterbrix::benchmark {

struct csv_load_entry_t {
    std::string path;
    std::string table;
    char delimiter = '|';
};

class interpreted_benchmark_t final : public benchmark_t {
public:
    explicit interpreted_benchmark_t(const std::filesystem::path& path);

    std::string name() const override;
    std::string group() const override;
    std::string description() const override;
    std::string query() const override;
    uint64_t nruns() const override;
    uint64_t timeout_seconds() const override;

    void load(benchmark_state_t& state) override;
    void run(benchmark_state_t& state) override;
    void cleanup(benchmark_state_t& state) override;
    std::string verify(benchmark_state_t& state) override;

private:
    void parse(const std::filesystem::path& path);
    void execute_sql_block(benchmark_state_t& state, const std::string& sql);
    void load_csv_file(benchmark_state_t& state, const csv_load_entry_t& entry);

    std::string name_;
    std::string group_;
    std::string description_;
    std::string load_sql_;
    std::string run_sql_;
    std::string cleanup_sql_;
    int64_t expected_rows_ = -1;
    uint64_t nruns_ = 5;
    uint64_t timeout_ = 30;
    std::vector<csv_load_entry_t> load_csv_entries_;
    std::filesystem::path benchmark_dir_;
};

} // namespace otterbrix::benchmark

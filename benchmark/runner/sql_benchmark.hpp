#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "benchmark.hpp"

namespace otterbrix::benchmark {

struct sql_csv_entry_t {
    std::string path;
    std::string table;
    char delimiter = '|';
};

class sql_benchmark_t final : public benchmark_t {
public:
    std::string name() const override;
    std::string group() const override;
    std::string description() const override;
    std::string query() const override;

    void load(benchmark_state_t& state) override;
    void run(benchmark_state_t& state) override;

    static std::vector<std::unique_ptr<sql_benchmark_t>>
    load_from_file(const std::filesystem::path& path, const std::filesystem::path& base_dir);

private:
    sql_benchmark_t(std::string name,
                    std::string group,
                    std::string sql,
                    std::string setup_sql,
                    std::vector<sql_csv_entry_t> csv_entries,
                    std::filesystem::path benchmark_dir,
                    std::string database);

    void execute_sql_block(benchmark_state_t& state, const std::string& sql);
    void load_csv_file(benchmark_state_t& state, const sql_csv_entry_t& entry);
    std::string qualify_sql(const std::string& sql) const;

    std::string name_;
    std::string group_;
    std::string sql_;
    std::string setup_sql_;
    std::vector<sql_csv_entry_t> csv_entries_;
    std::filesystem::path benchmark_dir_;
    std::string database_;
};

} // namespace otterbrix::benchmark

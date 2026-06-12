#pragma once

#include <cstdint>
#include <string>

namespace otterbrix::benchmark {

inline constexpr uint64_t csv_checkpoint_megabyte_bytes = uint64_t{1} << 20;

struct benchmark_configuration_t;

struct benchmark_io_options_t {
    bool disk_on = false;
    bool verbose = false;
    uint64_t csv_checkpoint_interval_bytes = 0;

    static benchmark_io_options_t from_config(const benchmark_configuration_t& config);
};

struct benchmark_configuration_t {
    std::string name_pattern;
    std::string group_pattern;
    uint64_t nruns = 0;
    uint64_t timeout_seconds = 30;
    bool list_only = false;
    bool list_groups = false;
    bool show_query = false;
    bool show_info = false;
    std::string output_file;
    std::string single_file;
    bool disk_on = false;
    bool wal_on = false;
    bool verbose = false;
    bool skip_load = false;
    bool load_only = false;
    bool no_setup = false;
    uint64_t csv_checkpoint_interval_bytes = 0;
    std::string config_file;
    std::string generate_config;
};

inline benchmark_io_options_t benchmark_io_options_t::from_config(const benchmark_configuration_t& config) {
    return {config.disk_on, config.verbose, config.csv_checkpoint_interval_bytes};
}

} // namespace otterbrix::benchmark

#pragma once

#include <cstdint>
#include <string>

namespace otterbrix::benchmark {

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
    std::string config_file;
    std::string generate_config;
};

} // namespace otterbrix::benchmark

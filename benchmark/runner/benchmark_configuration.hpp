#pragma once

#include <cstdint>
#include <string>

namespace otterbrix::benchmark {

struct benchmark_configuration_t {
    enum class disk_layout_policy : uint8_t
    {
        auto_select = 0,
        columnar_only = 1
    };

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
    disk_layout_policy layout_policy = disk_layout_policy::auto_select;
    bool wal_on = false;
    bool verbose = false;
    bool skip_load = false;
    bool load_only = false;
    std::string config_file;
    std::string generate_config;
};

} // namespace otterbrix::benchmark

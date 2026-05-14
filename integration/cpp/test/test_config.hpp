#pragma once

#include <integration/cpp/base_spaces.hpp>

#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <filesystem>
#include <string>
#include <vector>

// Build an SQL INSERT for the standard gen_data_chunk column set
// (count, count_str, count_double, count_bool, count_array, count_decimal).
// `start` matches gen_data_chunk(size, start, …) — row i carries values
// (start+i) across all columns.
inline std::string gen_data_chunk_insert_sql(const std::string& db,
                                              const std::string& rel,
                                              int size,
                                              int start = 0) {
    std::vector<std::string> rows;
    rows.reserve(static_cast<std::size_t>(size));
    for (int k = 1; k <= size; ++k) {
        const int v = k + start;
        rows.push_back(fmt::format("({}, '{}', {}, {}, [{}, {}, {}, {}, {}], {})",
                                   v, v, v + 0.1,
                                   (v % 2 != 0) ? "true" : "false",
                                   v, v + 1, v + 2, v + 3, v + 4,
                                   v));
    }
    return fmt::format("INSERT INTO {}.{} (count, count_str, count_double, count_bool, "
                       "count_array, count_decimal) VALUES {};",
                       db, rel, fmt::join(rows, ", "));
}

inline configuration::config test_create_config(const std::filesystem::path& path = std::filesystem::current_path()) {
    return configuration::config::create_config(path);
    // To change log level
    // config.log.level =log_t::level::trace;
}

inline void test_clear_directory(const configuration::config& config) {
    std::filesystem::remove_all(config.main_path);
    std::filesystem::create_directories(config.main_path);
}

class test_spaces final : public otterbrix::base_otterbrix_t {
public:
    test_spaces(const configuration::config& config)
        : otterbrix::base_otterbrix_t(config) {}
};

#pragma once

#include <components/compute/function.hpp>
#include <integration/cpp/base_spaces.hpp>

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
        : otterbrix::base_otterbrix_t(config) {
        // Isolate the process-global UDF registry between test cases: each test
        // gets a fresh builtins-only default registry so user functions from a
        // previous test don't leak into this one (which crashed test_batch_join
        // when run after test_batch_where — a stale aggregate UDF resolved to a
        // null function at plan-gen).
        components::compute::function_registry_t::reset_default();
    }
};

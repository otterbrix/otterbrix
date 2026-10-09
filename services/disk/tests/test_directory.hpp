#pragma once

#include <components/configuration/configuration.hpp>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <system_error>

// The engine factory creates the managers' directory before it spawns them; a manager only asserts
// that it exists. A test that spawns a manager directly creates it here, usable in a member initializer.
namespace test_directory {

    inline const std::filesystem::path& created(const std::filesystem::path& path) {
        if (path.empty()) {
            return path;
        }
        std::error_code ec;
        std::filesystem::create_directories(path, ec);
        if (ec) {
            std::fprintf(stderr, "test_directory: %s could not be created: %s\n", path.c_str(), ec.message().c_str());
            std::abort();
        }
        return path;
    }

    inline const configuration::config_disk& created(const configuration::config_disk& config) {
        created(config.path);
        return config;
    }

} // namespace test_directory

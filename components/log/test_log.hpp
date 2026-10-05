#pragma once

#include <components/log/log.hpp>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string_view>

// For test fixtures that build a logger in a member initializer, where no assertion macro can run:
// a directory the test run cannot write to stops the run with the reason.
inline log_t make_test_log(std::string_view name, const std::filesystem::path& directory) {
    auto log = make_log(name, directory);
    if (log.has_error()) {
        std::fprintf(stderr, "make_test_log: %s\n", log.error().what.c_str());
        std::abort();
    }
    return std::move(log.value());
}

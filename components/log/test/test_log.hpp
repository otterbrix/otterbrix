#pragma once

#include <components/log/log.hpp>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <string_view>
#include <system_error>

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

// A test that does not read its log back writes it under the system temporary directory.
inline log_t make_test_log() {
    std::error_code ec;
    const auto temporary = std::filesystem::temp_directory_path(ec);
    if (ec) {
        std::fprintf(stderr, "make_test_log: no temporary directory: %s\n", ec.message().c_str());
        std::abort();
    }
    return make_test_log("test", temporary / "otterbrix_test_logs");
}

// Everything the log files under `directory` hold, one file after another.
inline std::string log_text(const std::filesystem::path& directory) {
    std::string text;
    std::error_code ec;
    for (std::filesystem::directory_iterator it{directory, ec}, end; !ec && it != end; it.increment(ec)) {
        std::ifstream in(it->path());
        text.append(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    return text;
}

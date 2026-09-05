#pragma once

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <signal.h>
#include <unistd.h>

namespace services::index::tests {

    // The fixture root is qualified by process id ON PURPOSE. A shared literal path like
    // "/tmp/index_disk/<name>" lets two test binaries running at once -- two build directories, or
    // one ctest -j run against a second checkout -- open, truncate and unlink EACH OTHER'S segment
    // files. What that produces are real I/O failures ("segment could not be opened for reading", a
    // record header past the end of the file) attributed to the store under test, and they read as
    // flakes. Qualifying by pid is what the storage-layer fixtures already do
    // (components/table/test/*, components/table/storage/temporary_spill_file.cpp).
    // The pid qualification costs a directory per run, and nothing was reclaiming them.
    // Both ends are needed: the sweep covers the runs this process cannot clean up after
    // (an abort or a kill, routine in this directory), the exit hook covers the rest.
    inline void reclaim_dead_index_roots(const std::filesystem::path& shared,
                                         const std::filesystem::path& mine) {
        static constexpr std::string_view prefix = "index_disk_";
        std::error_code ec;
        for (std::filesystem::directory_iterator it{shared, ec}, end; !ec && it != end; it.increment(ec)) {
            const auto& entry = it->path();
            const auto name = entry.filename().string();
            if (entry == mine || name.rfind(prefix, 0) != 0) {
                continue;
            }
            const auto suffix = name.substr(prefix.size());
            char* parsed = nullptr;
            const long owner = std::strtol(suffix.c_str(), &parsed, 10);
            if (parsed == nullptr || *parsed != '\0' || owner <= 0 || ::kill(static_cast<::pid_t>(owner), 0) == 0) {
                continue;
            }
            std::error_code drop;
            std::filesystem::remove_all(entry, drop);
        }
    }

    inline const std::filesystem::path& index_fixture_root() {
        static const std::filesystem::path root = [] {
            const std::filesystem::path shared{"/tmp"};
            std::filesystem::path mine =
                shared / ("index_disk_" + std::to_string(static_cast<long>(::getpid())));
            reclaim_dead_index_roots(shared, mine);
            static const std::filesystem::path atexit_copy = mine;
            std::atexit([] {
                std::error_code ec;
                std::filesystem::remove_all(atexit_copy, ec);
            });
            return mine;
        }();
        return root;
    }

    inline std::filesystem::path index_fixture_path(std::string_view name) {
        return index_fixture_root() / name;
    }

} // namespace services::index::tests

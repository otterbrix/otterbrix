#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <unistd.h>

namespace services::index::tests {

    // The fixture root is qualified by process id ON PURPOSE.
    //
    // Every test binary in this directory used to share the literal path
    // "/tmp/index_disk/<name>". Two test binaries running at once — two build
    // directories, or one ctest -j run against a second checkout — therefore
    // opened, truncated and unlinked EACH OTHER'S segment files. The failures
    // that produced were real I/O failures ("segment could not be opened for
    // reading", a record header past the end of the file) attributed to the
    // store under test, and they were repeatedly written off as flaky.
    //
    // Qualifying by pid is what the storage-layer fixtures already do
    // (components/table/test/*, components/table/storage/temporary_spill_file.cpp).
    inline const std::filesystem::path& index_fixture_root() {
        static const std::filesystem::path root =
            std::filesystem::path{"/tmp"} / ("index_disk_" + std::to_string(static_cast<long>(::getpid())));
        return root;
    }

    inline std::filesystem::path index_fixture_path(std::string_view name) {
        return index_fixture_root() / name;
    }

} // namespace services::index::tests

#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <unistd.h>

// THE FIXTURE ROOT OF THIS DIRECTORY, QUALIFIED BY PROCESS ID ON PURPOSE.
//
// A literal "/tmp/..." data directory is shared by every test binary running at once --
// two build directories, a second checkout, one ctest -j run beside another -- and the
// first thing a case does with its directory is test_clear_directory(): remove_all()
// followed by create_directories(). One process unlinks the segment files, the WAL and
// the catalog another process has open and is midway through writing.
//
// MEASURED, NOT SUSPECTED. Two Debug binaries from two build directories on the same case
// at the same time, five consecutive iterations: EVERY iteration had exactly one process
// fail while the other reported "All tests passed". The failures read as real I/O and as
// engine defects --
//   filesystem error: in remove_all: Directory not empty [".../orig"]
//   filesystem error: in remove_all: No such file or directory [".../auto"]
//   REQUIRE( exec("CREATE INDEX k_idx ON sdb.t (k);")->is_success() ) == false
//   a pg_catalog system table did not come up, refusing to start: pg_settings
// -- none about the code under test, and all easy to write off as flakes. A watcher on the
// shared directory's inode showed it destroyed and recreated INSIDE the winner's run, so
// the process that went green had validated a database it did not write. That is the
// dangerous half. Under NDEBUG the DEV_MODE assert()s that might catch the mismatch are
// compiled out and only the quiet pass is left -- re-run with an NDEBUG binary, it passed
// while its directory changed inode underneath.
//
// Qualifying the root by pid is what the storage-layer and index fixtures already do
// (services/index/tests/index_fixture_path.hpp, components/table/test/*), and this is that
// helper for THIS directory -- deliberately per-directory, not shared, so the directories
// stay independently editable. The helpers are file-scope free functions, matching the
// neighbouring test_create_config / test_clear_directory in test_config.hpp.
inline const std::filesystem::path& integration_fixture_root() {
    static const std::filesystem::path root =
        std::filesystem::path{"/tmp"} / ("otterbrix_integration_" + std::to_string(static_cast<long>(::getpid())));
    return root;
}

// `name` is the fixture's leaf, relative to the pid-qualified root. Sub-directories are
// fine: the leaf is joined as a path, and the engine creates the intermediate directories
// it needs.
inline std::filesystem::path integration_fixture_path(std::string_view name) {
    return integration_fixture_root() / name;
}

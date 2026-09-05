#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <unistd.h>

// THE FIXTURE ROOT OF THIS DIRECTORY, QUALIFIED BY PROCESS ID ON PURPOSE.
//
// Every case in integration/cpp/test used to name its data directory with a literal
// "/tmp/..." path. Two test binaries running at once -- two build directories, a second
// checkout, or one ctest -j run beside another -- therefore pointed their engines at THE
// SAME directory, and the first thing a case does with that directory is
// test_clear_directory(): remove_all() followed by create_directories(). One process
// unlinks the segment files, the WAL and the catalog another process has open and is
// midway through writing.
//
// WHAT THAT PRODUCES IS NOT NOISE, AND IT WAS MEASURED ON THIS BRANCH BEFORE THE FIX.
// Two Debug binaries from two build directories, run on the same case at the same time,
// five consecutive iterations: EVERY iteration had exactly one of the two processes fail
// while the other reported "All tests passed". Across those runs and the same experiment
// on index_rebuild_crash the failures read as real I/O and as engine defects --
//   filesystem error: in remove_all: Directory not empty [".../orig"]
//   filesystem error: in remove_all: No such file or directory [".../auto"]
//   REQUIRE( exec("CREATE INDEX k_idx ON sdb.t (k);")->is_success() ) == false
//   a pg_catalog system table did not come up, refusing to start: pg_settings
// -- none of which is about the code under test. A watcher on the shared directory's
// inode showed it destroyed and recreated INSIDE the winner's run, so the process that
// went green had validated a database it did not write. That half is the dangerous half:
// it is green, and it is green about somebody else's data. It looks exactly the same
// under NDEBUG, where the DEV_MODE assert()s that might have caught a mismatch are
// compiled out and only the quiet pass is left; the pairing was re-run with an NDEBUG
// binary and it, too, passed while its directory changed inode underneath it.
//
// The failures above were attributed to the store under test and written off as flakes.
// Qualifying the root by pid is what the storage-layer and index fixtures already do
// (services/index/tests/index_fixture_path.hpp, components/table/test/*), and this is
// that helper for THIS directory -- deliberately per-directory, not shared, so the
// directories stay independently editable.
//
// The helpers are file-scope free functions, matching the neighbouring test_create_config
// / test_clear_directory in test_config.hpp, so a case gets the root by including this
// header and nothing else.
inline const std::filesystem::path& integration_fixture_root() {
    static const std::filesystem::path root =
        std::filesystem::path{"/tmp"} / ("otterbrix_integration_" + std::to_string(static_cast<long>(::getpid())));
    return root;
}

// `name` is what used to follow "/tmp/" in the literal, minus the "otterbrix/integration/"
// prefix the root now carries. Sub-directories are fine: the leaf is joined as a path, and
// the engine creates the intermediate directories it needs.
inline std::filesystem::path integration_fixture_path(std::string_view name) {
    return integration_fixture_root() / name;
}

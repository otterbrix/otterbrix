#pragma once

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <string_view>
#include <system_error>

#include <signal.h>
#include <unistd.h>

// Per-process, per-run root for every filesystem artefact the test suites write.
//
// Fixtures used to hardcode "/tmp/<suite>/<case>". That path is the same for
// every process on the box, so two test binaries running at once (parallel
// ctest, two agents, a CI matrix on one runner) pointed their engines at one
// directory: one process' remove_all raced the other's writes
// ("remove_all: Directory not empty") and rows written by one engine were read
// back by the other (large_checkpoint_100k saw 49000 of 100000 rows).
//
// Every fixture now builds its paths with test_temp_path(), which prefixes a
// root that is unique per process (pid) and per run (start timestamp). Only the
// root moves: the relative structure below it — shared parents, sibling
// sub-directories, .otbx file names — is exactly what each fixture passed
// before, so fixtures that deliberately share a directory between cases keep
// sharing it.
//
// Roots are collected on the way IN, not on the way out: a starting process
// deletes the roots of runs whose pid is gone. Deleting our own root at exit
// would race the engine threads that are still shutting down (they can recreate
// files mid-walk, and a checkpoint writing into a just-deleted directory would
// fail), and it would throw away the artefacts of a run that just failed.
//
// Environment overrides:
//   OTTERBRIX_TEST_TMPDIR     base directory to place run roots under
//                             (default: std::filesystem::temp_directory_path()).
//   OTTERBRIX_TEST_KEEP_TEMP  set to keep the roots of finished runs instead of
//                             sweeping them.
namespace otterbrix_test_temp_detail {

    inline constexpr std::string_view roots_dir_name = "otterbrix-tests";

    inline std::filesystem::path temp_base() {
        if (const char* override_base = std::getenv("OTTERBRIX_TEST_TMPDIR")) {
            return override_base;
        }
        std::error_code ec;
        auto base = std::filesystem::temp_directory_path(ec);
        return ec ? std::filesystem::path("/tmp") : base;
    }

    // A root is named "<pid>-<nanoseconds>". Anything else under the roots
    // directory was not written by us and is left alone.
    inline bool parse_root_pid(const std::string& name, long& pid_out) {
        const auto dash = name.find('-');
        if (dash == std::string::npos || dash == 0 || dash + 1 == name.size()) {
            return false;
        }
        for (std::size_t i = 0; i < name.size(); ++i) {
            if (i != dash && (name[i] < '0' || name[i] > '9')) {
                return false;
            }
        }
        pid_out = std::strtol(name.substr(0, dash).c_str(), nullptr, 10);
        return pid_out > 0;
    }

    inline bool process_alive(long pid) {
        if (::kill(static_cast<pid_t>(pid), 0) == 0) {
            return true;
        }
        // EPERM means the pid exists but belongs to someone else — still alive.
        return errno == EPERM;
    }

    // Delete the roots of runs that have already exited. A root whose pid is
    // still running is never touched, so a concurrent test process — including
    // one that has not created its root yet — cannot be swept out from under.
    inline void sweep_finished_roots(const std::filesystem::path& roots_dir) {
        if (std::getenv("OTTERBRIX_TEST_KEEP_TEMP") != nullptr) {
            return;
        }
        std::error_code ec;
        std::filesystem::directory_iterator it(roots_dir, ec);
        if (ec) {
            return;
        }
        for (const auto& entry : it) {
            long pid = 0;
            if (!parse_root_pid(entry.path().filename().string(), pid) || process_alive(pid)) {
                continue;
            }
            std::error_code remove_ec;
            std::filesystem::remove_all(entry.path(), remove_ec);
        }
    }

    inline std::filesystem::path make_run_root() {
        const auto roots_dir = temp_base() / roots_dir_name;
        std::error_code ec;
        std::filesystem::create_directories(roots_dir, ec);
        sweep_finished_roots(roots_dir);

        // pid separates processes alive at the same time; the start timestamp
        // separates runs that reuse a pid, so a leftover root from an earlier
        // run can never be mistaken for this run's.
        const auto pid = static_cast<long long>(::getpid());
        const auto started =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
                .count();
        auto root = roots_dir / (std::to_string(pid) + "-" + std::to_string(started));
        std::filesystem::create_directories(root, ec);
        return root;
    }

} // namespace otterbrix_test_temp_detail

// Root directory for this process' run. Created on first use.
inline const std::filesystem::path& test_temp_root() {
    static const std::filesystem::path root = otterbrix_test_temp_detail::make_run_root();
    return root;
}

// Absolute path for a test artefact. `relative` is the path a fixture used to
// spell after "/tmp/", e.g. test_temp_path("test_sql_features/is_null").
inline std::filesystem::path test_temp_path(std::string_view relative) {
    return test_temp_root() / std::filesystem::path(relative);
}

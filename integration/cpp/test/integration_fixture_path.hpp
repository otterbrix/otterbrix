#pragma once

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <string_view>
#include <signal.h>
#include <unistd.h>

// Fixture root is qualified by pid: a literal shared "/tmp/..." directory gets
// remove_all()+create_directories() by the first thing each test case does, so two test
// binaries running at once corrupt each other's fixtures. Measured: two Debug binaries on
// the same case at the same time, 5/5 iterations had one process fail on the other's
// destroyed/recreated directory, reading as unrelated I/O and engine errors. Same convention
// as services/index/tests/index_fixture_path.hpp and components/table/test/*, kept
// per-directory rather than shared.
namespace integration_fixture_detail {

    // Reclaimed from both a sweep here and an atexit hook below: the sweep covers runs that
    // crashed/aborted/were killed, the hook covers the common exit without waiting on a
    // later run. Without this, dead pid-qualified roots accumulated unbounded (872 dirs /
    // 926 GiB observed before this was added).
    inline void reclaim_dead_roots(const std::filesystem::path& shared, const std::filesystem::path& mine) {
        std::error_code ec;
        for (std::filesystem::directory_iterator it{shared, ec}, end; !ec && it != end; it.increment(ec)) {
            const auto& entry = it->path();
            if (entry == mine || entry.filename().string().rfind("otterbrix_integration_", 0) != 0) {
                continue;
            }
            const auto suffix = entry.filename().string().substr(std::strlen("otterbrix_integration_"));
            char* parsed = nullptr;
            const long owner = std::strtol(suffix.c_str(), &parsed, 10);
            // Not a whole number, or a live owner: not ours to reclaim.
            if (parsed == nullptr || *parsed != '\0' || owner <= 0 || ::kill(static_cast<::pid_t>(owner), 0) == 0) {
                continue;
            }
            std::error_code drop;
            std::filesystem::remove_all(entry, drop);
        }
    }

} // namespace integration_fixture_detail

inline const std::filesystem::path& integration_fixture_root() {
    static const std::filesystem::path root = [] {
        const std::filesystem::path shared{"/tmp"};
        std::filesystem::path mine =
            shared / ("otterbrix_integration_" + std::to_string(static_cast<long>(::getpid())));
        integration_fixture_detail::reclaim_dead_roots(shared, mine);
        static const std::filesystem::path atexit_copy = mine;
        std::atexit([] {
            std::error_code ec;
            std::filesystem::remove_all(atexit_copy, ec);
        });
        return mine;
    }();
    return root;
}

inline std::filesystem::path integration_fixture_path(std::string_view name) {
    return integration_fixture_root() / name;
}

inline const std::filesystem::path& integration_fixture_shared_root() {
    static const std::filesystem::path shared = integration_fixture_root().parent_path();
    return shared;
}

namespace integration_fixture_detail {

    // Component-wise, not string().starts_with(): that would match
    // ".../otterbrix_integration_123" against root ".../otterbrix_integration_12".
    [[nodiscard]] inline bool path_is_within(const std::filesystem::path& path,
                                             const std::filesystem::path& prefix) {
        auto p = path.begin();
        const auto p_end = path.end();
        for (auto q = prefix.begin(), q_end = prefix.end(); q != q_end; ++q, ++p) {
            if (p == p_end || *p != *q) {
                return false;
            }
        }
        return true;
    }

} // namespace integration_fixture_detail

// Safe: under integration_fixture_root() (pid-qualified), or entirely outside
// integration_fixture_shared_root() (unreachable by another process's remove_all()).
// Refused: anything else under the shared root, including a second, different pid
// convention -- that would split the fixture root in two, cleanable by no single rule.
[[nodiscard]] inline bool integration_fixture_path_is_qualified(const std::filesystem::path& path) {
    const std::filesystem::path normal = path.lexically_normal();
    if (!integration_fixture_detail::path_is_within(normal, integration_fixture_shared_root())) {
        return true;
    }
    return integration_fixture_detail::path_is_within(normal, integration_fixture_root());
}

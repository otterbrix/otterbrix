#pragma once

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <signal.h>
#include <string>
#include <string_view>
#include <unistd.h>

// Fixture root is pid-qualified: a shared /tmp directory gets wiped and recreated by each test
// case's setup, so concurrent binaries corrupt each other's fixtures (measured: two Debug binaries
// racing the same case, 5/5 iterations failed on the other's destroyed directory).
namespace integration_fixture_detail {

    // A sweep here covers crashed/killed runs; the atexit hook below covers the common exit
    // without waiting on a later run. Without both, dead roots accumulated unbounded (872 dirs /
    // 926 GiB observed).
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
    [[nodiscard]] inline bool path_is_within(const std::filesystem::path& path, const std::filesystem::path& prefix) {
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

// Safe under integration_fixture_root() or entirely outside integration_fixture_shared_root();
// anything else under the shared root is refused, since a second pid convention would split the
// fixture root in two.
[[nodiscard]] inline bool integration_fixture_path_is_qualified(const std::filesystem::path& path) {
    const std::filesystem::path normal = path.lexically_normal();
    if (!integration_fixture_detail::path_is_within(normal, integration_fixture_shared_root())) {
        return true;
    }
    return integration_fixture_detail::path_is_within(normal, integration_fixture_root());
}

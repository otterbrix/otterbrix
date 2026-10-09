#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include <components/log/log.hpp>
#include <core/assert/assert.hpp>

#include <components/log/test/test_log.hpp>
#include <csignal>
#include <filesystem>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

TEST_CASE("core::assert::test_ok") {
    REQUIRE_NOTHROW([&]() { assertion_log_msg(nullptr, true, "ok"); }());
}

TEST_CASE("core::assert::test_string_view") {
    std::string_view message = "Testing";
    REQUIRE_NOTHROW([&]() { assertion_log_msg(nullptr, true, message); }());
}
namespace {
    struct log_pair_t {
        log_t other;
        log_t own;
    };

    log_pair_t make_log_pair(const std::filesystem::path& root) {
        return log_pair_t{make_test_log("other", root / "other"), make_test_log("own", root / "own")};
    }
} // namespace

TEST_CASE("core::assert::a_failed_assertion_reports_through_the_given_log") {
    const auto root = std::filesystem::temp_directory_path() /
                      ("otterbrix_assert_log_" + std::to_string(static_cast<long>(::getpid())));
    std::filesystem::remove_all(root);

    if constexpr (core::detail::enable_assert) {
        // A debug build aborts: the failure is a signal death of a child process.
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            // Catch2 installs a SIGABRT handler; reset it so the abort reaches waitpid as a signal death.
            ::signal(SIGABRT, SIG_DFL);
            auto logs = make_log_pair(root);
            assertion_log_msg(&logs.own, 1 + 1 == 3, "the given log carries this");
            ::_exit(0);
        }
        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        REQUIRE(WIFSIGNALED(status));
    } else {
        auto logs = make_log_pair(root);
        REQUIRE_THROWS_WITH([&] { assertion_log_msg(&logs.own, 1 + 1 == 3, "the given log carries this"); }(),
                            "invariant (1 + 1 == 3) violation: the given log carries this");
    }
    CHECK(log_text(root / "own").find("the given log carries this") != std::string::npos);
    CHECK(log_text(root / "other").find("the given log carries this") == std::string::npos);
    std::filesystem::remove_all(root);
}

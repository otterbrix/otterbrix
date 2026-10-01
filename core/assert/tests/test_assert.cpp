#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>

#include <components/log/log.hpp>
#include <core/assert/assert.hpp>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <components/log/test_log.hpp>

TEST_CASE("core::assert::test_ok") {
    //REQUIRE_NOTHROW([&]() { assertion_failed(true); }());
    //REQUIRE_NOTHROW([&]() { assertion_failed_msg(true, "ok"); }());
    REQUIRE_NOTHROW([&]() { assertion_exception_msg(true, "ok"); }());
    REQUIRE_NOTHROW([&]() { assertion_exception(true); }());
}

TEST_CASE("core::assert::test_string_view") {
    std::string_view message = "Testing";
    //REQUIRE_NOTHROW([&]() { assertion_failed_msg(true, message); }());
    REQUIRE_NOTHROW([&]() { assertion_exception_msg(true, message); }());
    REQUIRE_NOTHROW([&]() { assertion_exception(true); }());
}
namespace {
    std::string read_all(const std::filesystem::path& directory) {
        std::string text;
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            std::ifstream in(entry.path());
            text.append(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
        }
        return text;
    }
} // namespace

TEST_CASE("core::assert::a_failed_assertion_reports_through_the_given_log") {
    const auto root = std::filesystem::temp_directory_path() /
                      ("otterbrix_assert_log_" + std::to_string(static_cast<long>(::getpid())));
    std::filesystem::remove_all(root);
    const auto own = root / "own";
    const auto other = root / "other";

    if constexpr (core::detail::enable_assert) {
        // A debug build aborts: the failure is a signal death of a child process.
        const pid_t child = ::fork();
        REQUIRE(child >= 0);
        if (child == 0) {
            // Catch2 installs a SIGABRT handler; reset it so the abort reaches waitpid as a signal death.
            ::signal(SIGABRT, SIG_DFL);
            auto other_log = make_test_log("other", other);
            auto own_log = make_test_log("own", own);
            assertion_log_msg(&own_log, 1 + 1 == 3, "the given log carries this");
            ::_exit(0);
        }
        int status = 0;
        REQUIRE(::waitpid(child, &status, 0) == child);
        REQUIRE(WIFSIGNALED(status));
    } else {
        // An NDEBUG build reports the same and throws InvariantError, naming the condition and the message.
        auto other_log = make_test_log("other", other);
        auto own_log = make_test_log("own", own);
        REQUIRE_THROWS_WITH([&] { assertion_log_msg(&own_log, 1 + 1 == 3, "the given log carries this"); }(),
                            "invariant (1 + 1 == 3) violation: the given log carries this");
    }
    CHECK(read_all(own).find("the given log carries this") != std::string::npos);
    CHECK(read_all(other).find("the given log carries this") == std::string::npos);
    std::filesystem::remove_all(root);
}

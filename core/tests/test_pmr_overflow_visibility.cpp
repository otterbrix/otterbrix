#include <catch2/catch_test_macros.hpp>

#include <core/pmr.hpp>

#include <csignal>
#include <cstddef>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__SANITIZE_ADDRESS__)
#define TEST_UNDER_ASAN 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define TEST_UNDER_ASAN 1
#endif
#endif

// The arena's backing store is picked when the library is built; under ASAN it must still hand
// out one redzoned block per allocation, or an overflow into a neighbour stays silent.
TEST_CASE("core::pmr::an_overflow_inside_the_arena_is_seen_under_asan") {
#if defined(TEST_UNDER_ASAN)
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        ::signal(SIGABRT, SIG_DFL);
        core::pmr::otterbrix_resource arena;
        auto* first = static_cast<volatile char*>(arena.allocate(16, 8));
        auto* second = static_cast<volatile char*>(arena.allocate(16, 8));
        first[16] = 'x';
        second[0] = 'y';
        ::_exit(0);
    }
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    CHECK_FALSE((WIFEXITED(status) && WEXITSTATUS(status) == 0));
#else
    SKIP("only an ASAN build can see the overflow");
#endif
}

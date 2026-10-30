#include <catch2/catch_test_macros.hpp>

#include <core/counting_resource.hpp>
#include <core/pmr.hpp>
#include <core/pmr/budget_resource.hpp>

#include <csignal>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <sys/wait.h>
#include <unistd.h>

using core::pmr::budget_resource_t;
using core::pmr::counting_resource_t;

TEST_CASE("core::pmr::budget_resource::the upstream serves what the budget allows") {
    counting_resource_t upstream(std::pmr::new_delete_resource());
    budget_resource_t budget(&upstream, 1024);

    void* first = budget.try_allocate(600);
    REQUIRE(first != nullptr);
    CHECK(budget.used() == 600);
    void* rest = budget.try_allocate(424);
    REQUIRE(rest != nullptr);
    CHECK(budget.used() == 1024);
    CHECK(upstream.allocations() == 2);

    budget.deallocate(first, 600);
    budget.deallocate(rest, 424);
    CHECK(budget.used() == 0);
    CHECK(upstream.outstanding() == 0);
}

TEST_CASE("core::pmr::budget_resource::over the limit the answer is nullptr and the upstream is not asked") {
    counting_resource_t upstream(std::pmr::new_delete_resource());
    budget_resource_t budget(&upstream, 1024);

    void* held = budget.try_allocate(1000);
    REQUIRE(held != nullptr);
    CHECK(budget.try_allocate(25) == nullptr);
    CHECK(budget.try_allocate(std::numeric_limits<std::size_t>::max()) == nullptr);
    CHECK(budget.used() == 1000);
    CHECK(upstream.allocations() == 1);

    void* fits = budget.try_allocate(24);
    REQUIRE(fits != nullptr);
    budget.deallocate(fits, 24);
    budget.deallocate(held, 1000);
}

TEST_CASE("core::pmr::budget_resource::a limit below what is held refuses until enough is given back") {
    core::pmr::otterbrix_resource upstream;
    budget_resource_t budget(&upstream, std::numeric_limits<std::uint64_t>::max());

    void* held = budget.try_allocate(4096);
    REQUIRE(held != nullptr);
    budget.set_limit(0);
    CHECK(budget.try_allocate(1) == nullptr);
    CHECK(budget.used() == 4096);

    budget.deallocate(held, 4096);
    budget.set_limit(64);
    void* again = budget.try_allocate(64);
    REQUIRE(again != nullptr);
    budget.deallocate(again, 64);
    CHECK(budget.used() == 0);
}

TEST_CASE("core::pmr::budget_resource::allocate over the limit aborts instead of throwing") {
    const pid_t child = ::fork();
    REQUIRE(child >= 0);
    if (child == 0) {
        // Catch2 installs a SIGABRT handler; reset it so the abort reaches waitpid as a signal death.
        ::signal(SIGABRT, SIG_DFL);
        budget_resource_t budget(std::pmr::new_delete_resource(), 16);
        std::pmr::memory_resource& as_resource = budget;
        [[maybe_unused]] void* never = as_resource.allocate(17);
        ::_exit(0);
    }
    int status = 0;
    REQUIRE(::waitpid(child, &status, 0) == child);
    REQUIRE(WIFSIGNALED(status));
    CHECK(WTERMSIG(status) == SIGABRT);
}

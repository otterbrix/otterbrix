#include <catch2/catch_test_macros.hpp>

#include <core/counting_resource.hpp>
#include <core/pmr.hpp>
#include <core/pmr/budget_resource.hpp>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <sys/wait.h>
#include <thread>
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

TEST_CASE("core::pmr::budget_resource::a limit below what is held") {
    core::pmr::otterbrix_resource upstream;
    budget_resource_t budget(&upstream, 8192);

    void* held = budget.try_allocate(4096);
    REQUIRE(held != nullptr);
    CHECK_FALSE(budget.set_limit(4095));
    CHECK(budget.available() == 4096);

    REQUIRE(budget.set_limit(4096));
    CHECK(budget.available() == 0);
    CHECK(budget.try_allocate(1) == nullptr);

    budget.deallocate(held, 4096);
    REQUIRE(budget.set_limit(64));
    void* again = budget.try_allocate(64);
    REQUIRE(again != nullptr);
    budget.deallocate(again, 64);
    CHECK(budget.used() == 0);
}

TEST_CASE("core::pmr::budget_resource::available across allocate and deallocate") {
    core::pmr::otterbrix_resource upstream;
    budget_resource_t budget(&upstream, 1000);
    CHECK(budget.available() == 1000);

    void* first = budget.try_allocate(300);
    REQUIRE(first != nullptr);
    CHECK(budget.available() == 700);
    void* second = budget.try_allocate(700);
    REQUIRE(second != nullptr);
    CHECK(budget.available() == 0);

    budget.deallocate(first, 300);
    CHECK(budget.available() == 300);
    budget.deallocate(second, 700);
    CHECK(budget.available() == 1000);
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

TEST_CASE("core::pmr::budget_resource::reservations against the limit") {
    core::pmr::otterbrix_resource upstream;
    budget_resource_t budget(&upstream, 1000);

    void* held = budget.try_allocate(200);
    REQUIRE(held != nullptr);
    {
        auto first = budget.try_reserve(500);
        REQUIRE(first);
        CHECK(budget.reserved() == 500);
        CHECK(budget.available() == 300);
        CHECK_FALSE(budget.try_reserve(301));
        CHECK_FALSE(budget.set_limit(699));

        INFO("an allocation is not taken out of a reservation: it counts as used while the room stays reserved");
        void* inside = budget.try_allocate(100);
        REQUIRE(inside != nullptr);
        CHECK(budget.available() == 200);
        budget.deallocate(inside, 100);

        auto second = budget.try_reserve(300);
        REQUIRE(second);
        CHECK(budget.available() == 0);
    }
    CHECK(budget.reserved() == 0);
    CHECK(budget.available() == 800);
    REQUIRE(budget.set_limit(200));
    CHECK(budget.try_reserve(0));
    CHECK_FALSE(budget.try_reserve(1));
    budget.deallocate(held, 200);
}

TEST_CASE("core::pmr::budget_resource::a reservation waiting for another one") {
    core::pmr::otterbrix_resource upstream;
    budget_resource_t budget(&upstream, 1000);

    auto holder = std::make_unique<budget_resource_t::reservation_t>(budget.try_reserve(800));
    REQUIRE(*holder);
    std::atomic<bool> reserved{false};
    std::thread waiter([&budget, &reserved] {
        for (;;) {
            const auto seen = budget.releases();
            if (auto reservation = budget.try_reserve(500)) {
                reserved.store(true);
                return;
            }
            budget.wait_for_release(seen);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(reserved.load());
    holder.reset();
    waiter.join();
    CHECK(reserved.load());
    CHECK(budget.reserved() == 0);
}

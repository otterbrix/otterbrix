#pragma once

#include <chrono>
#include <thread>

// A test waits for a reply by time, not by a count of polls: under a loaded machine (-j 2*nproc, sanitizers) a
// count runs out before the actor answers and the test fails on a reply that was still coming.
namespace test_helpers {

    inline constexpr std::chrono::seconds reply_deadline{120};

    // For actors on a scheduler the test drives itself.
    template<typename Future, typename Scheduler>
    [[nodiscard]] bool
    wait_ready(Future& future, Scheduler* scheduler, std::chrono::steady_clock::duration limit = reply_deadline) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (!future.is_ready() && std::chrono::steady_clock::now() < deadline) {
            scheduler->run(1000);
            std::this_thread::yield();
        }
        return future.is_ready();
    }

    // For actors on the engine's own threads.
    template<typename Future>
    [[nodiscard]] bool wait_ready(Future& future, std::chrono::steady_clock::duration limit = reply_deadline) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (!future.is_ready() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        return future.is_ready();
    }

    // For a condition the engine reaches on its own threads: a counter, a flag a seam raised.
    template<typename Predicate>
    [[nodiscard]] bool wait_until(Predicate&& reached, std::chrono::steady_clock::duration limit = reply_deadline) {
        const auto deadline = std::chrono::steady_clock::now() + limit;
        while (!reached() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return reached();
    }

} // namespace test_helpers

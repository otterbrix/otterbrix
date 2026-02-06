#include "scheduler_test.hpp"

namespace core::non_thread_scheduler {

    scheduler_test_t::scheduler_test_t(std::size_t num_worker_threads, std::size_t max_throughput)
        : base_t(num_worker_threads, max_throughput) {}

    clock_test& scheduler_test_t::clock() noexcept { return clock_; }

    void scheduler_test_t::start() {
    }

    void scheduler_test_t::stop() {
        while (run() > 0) {
            clock_.trigger_timeouts();
        }
        auto& queue = data().queue;
        std::unique_lock<std::mutex> guard(data().lock);
        while (!queue.empty()) {
            queue.pop_front();
        }
    }

    bool scheduler_test_t::run_once() {
        auto& queue = data().queue;
        std::unique_lock<std::mutex> guard(data().lock);

        if (queue.empty()) {
            return false;
        }

        auto job = queue.pop_front();
        guard.unlock();

        auto result = job->resume(max_throughput());
        switch (result.result) {
            case actor_zeta::scheduler::resume_result::resume:
                {
                    std::unique_lock<std::mutex> re_guard(data().lock);
                    data().queue.push_back(job.release());
                }
                break;
            case actor_zeta::scheduler::resume_result::done:
            case actor_zeta::scheduler::resume_result::awaiting:
                break;
            case actor_zeta::scheduler::resume_result::shutdown:
                break;
        }
        return true;
    }

    size_t scheduler_test_t::run(size_t max_count) {
        size_t res = 0;
        while (res < max_count && run_once()) {
            ++res;
        }
        return res;
    }

    size_t scheduler_test_t::advance_time(clock_test::duration_type time) {
        return clock_.advance_time(time);
    }

} // namespace core::non_thread_scheduler
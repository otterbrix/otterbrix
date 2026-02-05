#pragma once

#include <deque>
#include <memory>

#include <actor-zeta/scheduler/sharing_scheduler.hpp>

#include "clock_test.hpp"

namespace core::non_thread_scheduler {

    class scheduler_test_t final : public actor_zeta::scheduler::sharing_scheduler {
    public:
        using base_t = actor_zeta::scheduler::sharing_scheduler;
        using job_ptr_type = std::unique_ptr<actor_zeta::scheduler::job_ptr>;

        scheduler_test_t(std::size_t num_worker_threads, std::size_t max_throughput);

        bool run_once();
        size_t run(size_t max_count = std::numeric_limits<size_t>::max());
        size_t advance_time(clock_test::duration_type);
        clock_test& clock() noexcept;

        void start();
        void stop();

    private:
        clock_test clock_;
    };

} // namespace core::non_thread_scheduler
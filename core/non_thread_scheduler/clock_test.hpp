#pragma once

#include <chrono>
#include <functional>
#include <map>

namespace core::non_thread_scheduler {

    class clock_test final {
    public:
        using time_point = std::chrono::steady_clock::time_point;
        using duration_type = std::chrono::steady_clock::duration;
        using handler = std::function<void()>;

        struct schedule_entry final {
            handler f;
            duration_type period;
        };

        using schedule_map = std::multimap<time_point, schedule_entry>;

        clock_test();
        time_point now() const noexcept;
        void schedule_periodically(time_point, handler, duration_type);
        bool trigger_timeout();
        size_t trigger_timeouts();
        size_t advance_time(duration_type);

    private:
        time_point current_time;
        schedule_map schedule;
        bool try_trigger_once();
    };

} // namespace core::non_thread_scheduler

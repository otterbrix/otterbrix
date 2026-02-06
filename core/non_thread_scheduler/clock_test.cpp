#include "clock_test.hpp"

#include <cassert>

namespace core::non_thread_scheduler {

    clock_test::clock_test()
        : current_time(duration_type{1}) {}

    void clock_test::schedule_periodically(time_point first_run, handler f, duration_type period) {
        assert(bool(f));
        schedule.emplace(first_run, schedule_entry{std::move(f), period});
    }

    clock_test::time_point clock_test::now() const noexcept { return current_time; }

    bool clock_test::trigger_timeout() {
        for (;;) {
            if (schedule.empty()) {
                return false;
            }
            auto i = schedule.begin();
            auto t = i->first;
            if (t > current_time) {
                current_time = t;
            }
            if (try_trigger_once()) {
                return true;
            }
        }
    }

    size_t clock_test::trigger_timeouts() {
        assert(schedule.size());
        if (schedule.empty()) {
            return 0u;
        }

        size_t result = 0;
        while (trigger_timeout()) {
            ++result;
        }

        return result;
    }

    size_t clock_test::advance_time(duration_type time) {
        ///assert((x << schedule.size()));
        assert(time.count() >= 0);
        current_time += time;
        auto result = size_t{0};

        while (!schedule.empty() && schedule.begin()->first <= current_time) {
            if (try_trigger_once()) {
                ++result;
            }
        }

        return result;
    }

    bool clock_test::try_trigger_once() {
        auto i = schedule.begin();
        auto t = i->first;

        if (t > current_time) {
            return false;
        }

        auto f = std::move(i->second.f);
        auto& period = i->second.period;
        schedule.erase(i);
        f();
        if (period.count() > 0) {
            auto next = t + period;
            while (next <= current_time) {
                next += period;
            }
            schedule.emplace(next, schedule_entry{std::move(f), period});
        }
        return true;
    }

} // namespace core::non_thread_scheduler

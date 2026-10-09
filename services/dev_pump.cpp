#include "dev_pump.hpp"

#ifdef DEV_MODE
#include <atomic>

namespace services {

    namespace {
        std::atomic<std::uint64_t> g_pump_wakeups{0};
        std::atomic<std::uint64_t> g_pump_idle_waiters{0};
    } // namespace

    std::uint64_t dev_pump_wakeups() noexcept { return g_pump_wakeups.load(); }
    std::uint64_t dev_pump_idle_waiters() noexcept { return g_pump_idle_waiters.load(); }

    dev_pump_wait_t::dev_pump_wait_t(bool idle) noexcept
        : idle_(idle) {
        if (idle_) {
            g_pump_idle_waiters.fetch_add(1);
        }
    }

    dev_pump_wait_t::~dev_pump_wait_t() {
        if (idle_) {
            g_pump_idle_waiters.fetch_sub(1);
        }
        g_pump_wakeups.fetch_add(1);
    }

} // namespace services
#endif

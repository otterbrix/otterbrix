#pragma once

#ifdef DEV_MODE
#include <cstdint>

namespace services {

    // Test seam over the manager loops (dispatcher, disk, index, WAL): how often a loop came out of its wait,
    // and how many loops sit in an idle wait (one that awaits no reply) right now.
    std::uint64_t dev_pump_wakeups() noexcept;
    std::uint64_t dev_pump_idle_waiters() noexcept;

    // Lives across one wait of a loop.
    class dev_pump_wait_t final {
    public:
        explicit dev_pump_wait_t(bool idle) noexcept;
        ~dev_pump_wait_t();
        dev_pump_wait_t(const dev_pump_wait_t&) = delete;
        dev_pump_wait_t& operator=(const dev_pump_wait_t&) = delete;

    private:
        bool idle_;
    };

} // namespace services
#endif

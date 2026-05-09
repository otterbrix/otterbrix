#pragma once

#include <cstdint>

namespace components::catalog {

    enum class drop_behavior_t : std::uint8_t {
        restrict_ = 0,
        cascade_  = 1,
    };

    enum class ddl_status : std::uint8_t {
        ok               = 0,
        restrict_blocked = 1,
        cycle_detected   = 2,
    };

} // namespace components::catalog

namespace services::disk {

    // Aliases so existing disk/dispatcher/executor callers compile unchanged.
    using drop_behavior_t = components::catalog::drop_behavior_t;
    using ddl_status      = components::catalog::ddl_status;

} // namespace services::disk

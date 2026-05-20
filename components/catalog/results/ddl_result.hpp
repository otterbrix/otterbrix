#pragma once

#include <cstdint>

namespace components::catalog {

    enum class drop_behavior_t : std::uint8_t
    {
        restrict_ = 0,
        cascade_ = 1,
    };

    enum class ddl_status : std::uint8_t
    {
        ok = 0,
        restrict_blocked = 1,
        cycle_detected = 2, // pg_depend back-edge encountered during cascade DFS
    };

} // namespace components::catalog
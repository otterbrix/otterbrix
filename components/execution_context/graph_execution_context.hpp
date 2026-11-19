#pragma once

#include <core/date/time_zone.hpp>

#include <cstdint>

namespace components {

    struct graph_execution_context {
        core::date::time_zone_t timezone_offset;
    };

} // namespace components

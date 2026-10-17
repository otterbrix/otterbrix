#pragma once

#include <core/date/date_types.hpp>

#include <cstdint>

namespace components {

    struct graph_execution_context {
        core::date::timezone_offset_t timezone_offset;
        // If target DECIMAL does not specify width and scale
        uint8_t decimal_width;
        uint8_t decimal_scale;
    };

} // namespace components
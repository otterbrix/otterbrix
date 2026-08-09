#pragma once

#include <components/types/logical_value.hpp>
#include <core/date/date_types.hpp>

#include <cstdint>

namespace components {

    struct graph_execution_context {
        core::date::timezone_offset_t timezone_offset;
        // An operation may fill values with this, instead of leaving a null
        const types::logical_value_t* fill_value = nullptr;
        // If target DECIMAL does not specify width and scale
        // TODO: read these from catalog settings instead of defaulting here
        uint8_t decimal_width = 18;
        uint8_t decimal_scale = 3;
    };

} // namespace components
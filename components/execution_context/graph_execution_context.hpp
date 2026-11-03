#pragma once

#include <core/date/date_types.hpp>

#include <cstdint>

namespace components {

    struct graph_execution_context {
        core::date::timezone_offset_t timezone_offset;
    };

} // namespace components
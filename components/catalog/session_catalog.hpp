#pragma once

#include <core/date/date_types.hpp>

#include <cstdint>
#include <string>

namespace components::catalog {

    // The settings the engine caches, and the defaults a value
    struct session_catalog_t {
        std::string timezone_name{"UTC"};
        core::date::timezone_offset_t timezone_offset{};
        // What a DECIMAL with no width and scale of its own gets.
        uint8_t decimal_width{18};
        uint8_t decimal_scale{3};
        bool autocommit{true};
    };

} // namespace components::catalog

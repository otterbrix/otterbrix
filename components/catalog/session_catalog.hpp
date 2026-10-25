#pragma once

#include <core/date/date_types.hpp>

#include <cstdint>
#include <string>

namespace components::catalog {

    // The values of the settings; default-constructed, the built-in ones.
    struct session_catalog_t {
        std::string timezone_name{"UTC"};
        core::date::timezone_offset_t timezone_offset{};
        bool autocommit{true};
    };

} // namespace components::catalog

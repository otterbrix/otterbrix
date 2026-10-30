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
        // The current place as the session keeps it (see place.hpp): 'database.schema' or
        // 'connection.database.schema', or one part as ALTER SYSTEM SET wrote it, read against main.
        std::string search_path{"main.public"};
    };

} // namespace components::catalog

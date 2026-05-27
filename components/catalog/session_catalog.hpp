#pragma once

#include <core/date/date_types.hpp>
#include <core/date/timezones.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <string_view>

namespace components::catalog {

    struct session_catalog_t {
        std::string timezone_name{"UTC"};
        core::date::timezone_offset_t timezone_offset{};

        core::error_t set_timezone(std::pmr::memory_resource* resource, std::string_view name) {
            auto offset = core::date::timezone_to_offset(name);
            if (!offset) {
                return core::error_t(core::error_code_t::other_error,
                                     std::pmr::string{"unrecognized timezone: '" + std::string(name) + "'", resource});
            }
            timezone_name = std::string(name);
            timezone_offset = *offset;
            return core::error_t::no_error();
        }
    };

} // namespace components::catalog

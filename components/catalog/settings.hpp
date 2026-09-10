#pragma once

#include <components/catalog/session_catalog.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>
#include <string_view>

namespace components::catalog {

    enum class setting_id : uint8_t
    {
        timezone,
        decimal_width,
        decimal_scale,
        autocommit
    };

    struct setting_def_t {
        // What SET names it, matched case-insensitively.
        std::string_view sql_name;
        // What the pg_settings row is keyed by.
        std::string_view catalog_name;
        setting_id id;
    };

    [[nodiscard]] std::span<const setting_def_t> all_settings() noexcept;

    [[nodiscard]] const setting_def_t* find_setting_by_sql_name(std::string_view sql_name) noexcept;
    [[nodiscard]] const setting_def_t& find_setting_by_id(setting_id id) noexcept;

    [[nodiscard]] core::result_wrapper_t<std::pmr::string>
    canonical_setting_value(setting_id id, std::string_view raw, std::pmr::memory_resource* resource);

    // The only way to change a setting on a cache
    [[nodiscard]] core::error_t
    set_setting(session_catalog_t& cache, setting_id id, std::string_view raw, std::pmr::memory_resource* resource);

    [[nodiscard]] core::error_t
    validate_decimal_defaults(uint8_t width, uint8_t scale, std::pmr::memory_resource* resource);

    [[nodiscard]] uint8_t unsigned_setting_value(std::string_view canonical) noexcept;
    [[nodiscard]] bool boolean_setting_value(std::string_view canonical) noexcept;

    [[nodiscard]] std::pmr::string
    current_setting_value(const session_catalog_t& cache, setting_id id, std::pmr::memory_resource* resource);

} // namespace components::catalog

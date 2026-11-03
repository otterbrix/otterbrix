#pragma once

#include <components/catalog/session_catalog.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <span>
#include <string>
#include <string_view>

namespace components::catalog {

    enum class setting_id : uint8_t
    {
        timezone,
        autocommit,
        search_path
    };

    // Where a value goes: SET to its session, SET LOCAL to its session until the transaction ends, ALTER SYSTEM to
    // pg_settings as the default of every session that has no value of its own.
    enum class setting_scope_t : uint8_t
    {
        session,
        local,
        system
    };

    // A value for a setting, or its default (RESET, SET ... TO DEFAULT). A copy would put the value on the default
    // resource, so there is none.
    struct setting_assignment_t {
        setting_assignment_t(setting_id id, std::string_view value, std::pmr::memory_resource* resource);
        static setting_assignment_t to_default(setting_id id, std::pmr::memory_resource* resource);
        setting_assignment_t(const setting_assignment_t&) = delete;
        setting_assignment_t& operator=(const setting_assignment_t&) = delete;
        setting_assignment_t(setting_assignment_t&&) noexcept = default;
        setting_assignment_t& operator=(setting_assignment_t&&) = default;

        setting_id id;
        bool is_default;
        std::pmr::string value;
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

    [[nodiscard]] bool boolean_setting_value(std::string_view canonical) noexcept;

    [[nodiscard]] std::pmr::string
    current_setting_value(const session_catalog_t& cache, setting_id id, std::pmr::memory_resource* resource);

} // namespace components::catalog

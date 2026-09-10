#include "settings.hpp"

#include <components/types/types.hpp>
#include <core/date/timezones.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <string>

namespace components::catalog {

    namespace {

        constexpr std::array<setting_def_t, 4> known_settings{
            setting_def_t{"timezone", "TimeZone", setting_id::timezone},
            setting_def_t{"decimal_width", "decimal_width", setting_id::decimal_width},
            setting_def_t{"decimal_scale", "decimal_scale", setting_id::decimal_scale},
            setting_def_t{"autocommit", "autocommit", setting_id::autocommit}};

        std::string to_lower(std::string_view text) {
            std::string lowered(text);
            std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char character) {
                return static_cast<char>(std::tolower(character));
            });
            return lowered;
        }

        core::result_wrapper_t<std::pmr::string> canonical_unsigned(std::string_view sql_name,
                                                                    std::string_view raw,
                                                                    uint8_t low,
                                                                    uint8_t high,
                                                                    std::pmr::memory_resource* resource) {
            unsigned parsed = 0;
            const auto* first = raw.data();
            const auto* last = raw.data() + raw.size();
            const auto converted = std::from_chars(first, last, parsed);
            if (converted.ec != std::errc{} || converted.ptr != last || parsed < low || parsed > high) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{std::string(sql_name) + " takes a whole number between " +
                                                          std::to_string(static_cast<unsigned>(low)) + " and " +
                                                          std::to_string(static_cast<unsigned>(high)) + ", not '" +
                                                          std::string(raw) + "'",
                                                      resource});
            }
            return std::pmr::string{std::to_string(parsed), resource};
        }

        core::result_wrapper_t<std::pmr::string> canonical_boolean(std::string_view sql_name,
                                                                    std::string_view raw,
                                                                    std::pmr::memory_resource* resource) {
            const auto lowered = to_lower(raw);
            if (lowered == "on" || lowered == "true" || lowered == "1") {
                return std::pmr::string{"on", resource};
            }
            if (lowered == "off" || lowered == "false" || lowered == "0") {
                return std::pmr::string{"off", resource};
            }
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{std::string(sql_name) + " takes on/off, true/false or 1/0, not '" +
                                                      std::string(raw) + "'",
                                                  resource});
        }

    } // namespace

    std::span<const setting_def_t> all_settings() noexcept { return known_settings; }

    const setting_def_t* find_setting_by_sql_name(std::string_view sql_name) noexcept {
        const auto lowered = to_lower(sql_name);
        for (const auto& setting : known_settings) {
            if (setting.sql_name == lowered) {
                return &setting;
            }
        }
        return nullptr;
    }

    const setting_def_t& find_setting_by_id(setting_id id) noexcept {
        for (const auto& setting : known_settings) {
            if (setting.id == id) {
                return setting;
            }
        }
        assert(false && "a setting_id with no row in known_settings");
        std::abort();
    }

    std::pmr::string
    current_setting_value(const session_catalog_t& cache, setting_id id, std::pmr::memory_resource* resource) {
        switch (id) {
            case setting_id::timezone:
                return std::pmr::string{cache.timezone_name.data(), cache.timezone_name.size(), resource};
            case setting_id::decimal_width:
                return std::pmr::string{std::to_string(static_cast<unsigned>(cache.decimal_width)), resource};
            case setting_id::decimal_scale:
                return std::pmr::string{std::to_string(static_cast<unsigned>(cache.decimal_scale)), resource};
            case setting_id::autocommit:
                return std::pmr::string{cache.autocommit ? "on" : "off", resource};
        }
        assert(false && "a setting_id with no value in the cache");
        std::abort();
    }

    uint8_t unsigned_setting_value(std::string_view canonical) noexcept {
        unsigned parsed = 0;
        // Canonical by contract: canonical_setting_value produced it, so it is digits in range.
        std::from_chars(canonical.data(), canonical.data() + canonical.size(), parsed);
        return static_cast<uint8_t>(parsed);
    }

    bool boolean_setting_value(std::string_view canonical) noexcept { return canonical == "on"; }

    core::result_wrapper_t<std::pmr::string>
    canonical_setting_value(setting_id id, std::string_view raw, std::pmr::memory_resource* resource) {
        switch (id) {
            case setting_id::timezone: {
                // Lowercase: timezone_to_offset only matches lowercase names.
                const auto lowered = to_lower(raw);
                if (!core::date::timezone_to_offset(lowered)) {
                    return core::error_t(core::error_code_t::invalid_parameter,
                                         std::pmr::string{"unrecognized timezone: '" + std::string(raw) + "'",
                                                          resource});
                }
                return std::pmr::string{lowered.data(), lowered.size(), resource};
            }
            case setting_id::decimal_width:
                return canonical_unsigned("decimal_width", raw, 1, types::DECIMAL_MAX_WIDTH, resource);
            case setting_id::decimal_scale:
                return canonical_unsigned("decimal_scale", raw, 0, types::DECIMAL_MAX_WIDTH, resource);
            case setting_id::autocommit:
                return canonical_boolean("autocommit", raw, resource);
        }
        return core::error_t(core::error_code_t::invalid_parameter,
                             std::pmr::string{"unknown setting", resource});
    }

    core::error_t
    validate_decimal_defaults(uint8_t width, uint8_t scale, std::pmr::memory_resource* resource) {
        if (!types::is_valid_decimal_spec(width, scale)) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"decimal_width " + std::to_string(static_cast<unsigned>(width)) +
                                                      " with decimal_scale " +
                                                      std::to_string(static_cast<unsigned>(scale)) +
                                                      " is not a DECIMAL this engine can express: scale must not "
                                                      "exceed width",
                                                  resource});
        }
        return core::error_t::no_error();
    }

    core::error_t
    set_setting(session_catalog_t& cache, setting_id id, std::string_view raw, std::pmr::memory_resource* resource) {
        auto canonical = canonical_setting_value(id, raw, resource);
        if (canonical.has_error()) {
            return canonical.error();
        }
        const std::string_view value{canonical.value().data(), canonical.value().size()};
        switch (id) {
            case setting_id::timezone: {
                auto offset = core::date::timezone_to_offset(value);
                assert(offset && "canonical_setting_value passed a timezone the recognizer then refused");
                cache.timezone_name = std::string(value);
                cache.timezone_offset = *offset;
                return core::error_t::no_error();
            }
            case setting_id::decimal_width:
                cache.decimal_width = unsigned_setting_value(value);
                return core::error_t::no_error();
            case setting_id::decimal_scale:
                cache.decimal_scale = unsigned_setting_value(value);
                return core::error_t::no_error();
            case setting_id::autocommit:
                cache.autocommit = boolean_setting_value(value);
                return core::error_t::no_error();
        }
        return core::error_t(core::error_code_t::invalid_parameter,
                             std::pmr::string{"unknown setting", resource});
    }

} // namespace components::catalog

#include "settings.hpp"

#include "place.hpp"

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

        constexpr std::array<setting_def_t, 9> known_settings{
            setting_def_t{"timezone",
                          "TimeZone",
                          setting_id::timezone,
                          "Sets the time zone for displaying and interpreting time stamps."},
            setting_def_t{"autocommit",
                          "autocommit",
                          setting_id::autocommit,
                          "Sets whether a statement outside a transaction block commits by itself."},
            setting_def_t{"search_path",
                          "search_path",
                          setting_id::search_path,
                          "Sets the schema search order for names that are not schema-qualified."},
            setting_def_t{"default_transaction_read_only",
                          "default_transaction_read_only",
                          setting_id::default_transaction_read_only,
                          "Sets the default read-only status of new transactions."},
            setting_def_t{"transaction_read_only",
                          "transaction_read_only",
                          setting_id::transaction_read_only,
                          "Sets the current transaction's read-only status."},
            setting_def_t{"default_transaction_isolation",
                          "default_transaction_isolation",
                          setting_id::default_transaction_isolation,
                          "Sets the transaction isolation level of each new transaction."},
            setting_def_t{"transaction_isolation",
                          "transaction_isolation",
                          setting_id::transaction_isolation,
                          "Sets the current transaction's isolation level."},
            setting_def_t{"default_transaction_deferrable",
                          "default_transaction_deferrable",
                          setting_id::default_transaction_deferrable,
                          "Sets the default deferrable status of new transactions."},
            setting_def_t{"transaction_deferrable",
                          "transaction_deferrable",
                          setting_id::transaction_deferrable,
                          "Whether to defer a read-only serializable transaction until it can be executed with no "
                          "possible serialization failures."}};

        constexpr std::string_view repeatable_read = "repeatable read";

        std::string to_lower(std::string_view text) {
            std::string lowered(text);
            std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char character) {
                return static_cast<char>(std::tolower(character));
            });
            return lowered;
        }

        core::result_wrapper_t<std::pmr::string>
        canonical_boolean(std::string_view sql_name, std::string_view raw, std::pmr::memory_resource* resource) {
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

    setting_assignment_t::setting_assignment_t(setting_id id,
                                               std::string_view value,
                                               std::pmr::memory_resource* resource)
        : id(id)
        , is_default(false)
        , value(value, resource) {}

    setting_assignment_t setting_assignment_t::to_default(setting_id id, std::pmr::memory_resource* resource) {
        setting_assignment_t assignment{id, std::string_view{}, resource};
        assignment.is_default = true;
        return assignment;
    }

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
            case setting_id::autocommit:
                return std::pmr::string{cache.autocommit ? "on" : "off", resource};
            case setting_id::search_path:
                return std::pmr::string{cache.search_path.data(), cache.search_path.size(), resource};
            case setting_id::default_transaction_read_only:
                return std::pmr::string{cache.default_transaction_read_only ? "on" : "off", resource};
            case setting_id::transaction_read_only:
                return std::pmr::string{cache.transaction_read_only ? "on" : "off", resource};
            case setting_id::default_transaction_isolation:
            case setting_id::transaction_isolation:
                return std::pmr::string{repeatable_read, resource};
            case setting_id::default_transaction_deferrable:
            case setting_id::transaction_deferrable:
                return std::pmr::string{"off", resource};
        }
        assert(false && "a setting_id with no value in the cache");
        std::abort();
    }

    bool boolean_setting_value(std::string_view canonical) noexcept { return canonical == "on"; }

    core::result_wrapper_t<std::pmr::string>
    canonical_setting_value(setting_id id, std::string_view raw, std::pmr::memory_resource* resource) {
        switch (id) {
            case setting_id::timezone: {
                // Lowercase: timezone_to_offset only matches lowercase names.
                const auto lowered = to_lower(raw);
                if (!core::date::timezone_to_offset(lowered)) {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"unrecognized timezone: '" + std::string(raw) + "'", resource});
                }
                return std::pmr::string{lowered.data(), lowered.size(), resource};
            }
            case setting_id::autocommit:
                return canonical_boolean("autocommit", raw, resource);
            case setting_id::search_path:
                return canonical_search_path(raw, resource);
            case setting_id::default_transaction_read_only:
                return canonical_boolean("default_transaction_read_only", raw, resource);
            case setting_id::transaction_read_only:
                return canonical_boolean("transaction_read_only", raw, resource);
            case setting_id::default_transaction_isolation:
            case setting_id::transaction_isolation:
                return isolation_level_of(raw, resource);
            case setting_id::default_transaction_deferrable:
                return canonical_boolean("default_transaction_deferrable", raw, resource);
            case setting_id::transaction_deferrable:
                return canonical_boolean("transaction_deferrable", raw, resource);
        }
        return core::error_t(core::error_code_t::internal_error, std::pmr::string{"unknown setting", resource});
    }

    core::result_wrapper_t<std::pmr::string> isolation_level_of(std::string_view written,
                                                                std::pmr::memory_resource* resource) {
        const auto level = to_lower(written);
        if (level == "serializable") {
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"transaction isolation level serializable is not supported: "
                                                  "transactions run at repeatable read",
                                                  resource});
        }
        if (level != "read uncommitted" && level != "read committed" && level != repeatable_read) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"invalid transaction isolation level: '" + std::string{written} + "'", resource});
        }
        return std::pmr::string{repeatable_read, resource};
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
            case setting_id::autocommit:
                cache.autocommit = boolean_setting_value(value);
                return core::error_t::no_error();
            case setting_id::search_path:
                cache.search_path = std::string(value);
                return core::error_t::no_error();
            case setting_id::default_transaction_read_only:
                cache.default_transaction_read_only = boolean_setting_value(value);
                cache.transaction_read_only = cache.default_transaction_read_only;
                return core::error_t::no_error();
            case setting_id::transaction_read_only:
                cache.transaction_read_only = boolean_setting_value(value);
                return core::error_t::no_error();
            case setting_id::default_transaction_isolation:
            case setting_id::transaction_isolation:
                // Every level a transaction may ask for runs as the one the engine has.
                return core::error_t::no_error();
            case setting_id::default_transaction_deferrable:
            case setting_id::transaction_deferrable:
                // Deferring only waits out a read-only serializable transaction's snapshot, and none runs serializable.
                return core::error_t::no_error();
        }
        return core::error_t(core::error_code_t::internal_error, std::pmr::string{"unknown setting", resource});
    }

} // namespace components::catalog

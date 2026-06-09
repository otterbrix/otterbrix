#include "alter_column_validators.hpp"

#include "system_table_schemas.hpp"

namespace components::catalog::alter_column_validators {

    core::error_t validate_column_not_duplicate(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<std::string>& visible_column_names,
                                                const std::string& new_column_name) {
        for (const auto& existing : visible_column_names) {
            if (existing == new_column_name) {
                std::pmr::string msg{resource};
                msg.append("column \"");
                msg.append(new_column_name);
                msg.append("\" already exists");
                return core::error_t{core::error_code_t::already_exists, std::move(msg)};
            }
        }
        return core::error_t::no_error();
    }

    core::error_t validate_default_value_type(std::pmr::memory_resource* resource,
                                              const components::types::complex_logical_type& column_type,
                                              const std::optional<components::types::logical_value_t>& default_value) {
        if (!default_value.has_value()) {
            return core::error_t::no_error();
        }
        // NULL default is compatible with any nullable column; the NOT-NULL check
        // is a separate constraint validation owned by the operator layer.
        if (default_value->is_null()) {
            return core::error_t::no_error();
        }
        if (default_value->type() != column_type) {
            std::pmr::string msg{resource};
            msg.append("default value type mismatch");
            return core::error_t{core::error_code_t::invalid_parameter, std::move(msg)};
        }
        return core::error_t::no_error();
    }

    core::error_t
    validate_default_value_evaluatable(std::pmr::memory_resource* /*resource*/,
                                       const std::optional<components::types::logical_value_t>& default_value) {
        // A materialised logical_value_t is evaluatable by construction; nothing to check yet.
        (void) default_value;
        return core::error_t::no_error();
    }

    core::error_t
    validate_cascade_dependencies(std::pmr::memory_resource* /*resource*/,
                                  const std::pmr::vector<std::pair<int, components::catalog::oid_t>>& dependents) {
        // TODO: stub; real handler table dispatches on pg_depend.classid (see .hpp).
        (void) dependents;
        return core::error_t::no_error();
    }

    core::error_t encode_default_spec_ec(std::pmr::memory_resource* /*resource*/,
                                         const std::optional<components::types::logical_value_t>& default_value,
                                         std::pmr::string& out_spec) {
        out_spec.clear();
        if (!default_value.has_value()) {
            return core::error_t::no_error();
        }
        // encode_default_spec returns "" for complex types; we forward that as success (see .hpp).
        const std::string encoded = components::catalog::encode_default_spec(*default_value);
        out_spec.assign(encoded.begin(), encoded.end());
        return core::error_t::no_error();
    }

} // namespace components::catalog::alter_column_validators

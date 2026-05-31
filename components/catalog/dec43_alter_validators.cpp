// Block C §3.5 dec 43 V1 — ALTER 3-phase atomic validation helpers (Phase 1).
// Implementation notes: see dec43_alter_validators.hpp for the design contract.

#include "dec43_alter_validators.hpp"

#include "system_table_schemas.hpp"

namespace components::catalog::dec43 {

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

    core::error_t
    validate_default_value_type(std::pmr::memory_resource* resource,
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
        // TODO(dec43-phase2): integrate with the expression evaluator so that
        // computed defaults (now(), nextval(...), CASE-expressions) are folded
        // here and rejected when they reference unresolved symbols or fail to
        // evaluate. For Phase 1 every materialised logical_value_t is, by
        // construction, already evaluatable — no work to do.
        (void) default_value;
        return core::error_t::no_error();
    }

    core::error_t
    validate_cascade_dependencies(std::pmr::memory_resource* /*resource*/,
                                  const std::pmr::vector<std::pair<int, components::catalog::oid_t>>& dependents) {
        // TODO(dec43-phase2): replace this stub with the real handler table:
        //   pg_depend.classid → cascade handler (FK / view / check_constraint /
        //   index / computed_column). On an unhandled classid return
        //   error_code_t::other_error so the ALTER fails atomically.
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
        // Delegate to the existing encoder (system_table_schemas.cpp). It is a
        // pure function over logical_value_t and returns an empty string for
        // complex types — we forward that semantics rather than upgrading it
        // to an error until a richer encoder exists.
        const std::string encoded = components::catalog::encode_default_spec(*default_value);
        out_spec.assign(encoded.begin(), encoded.end());
        return core::error_t::no_error();
    }

} // namespace components::catalog::dec43

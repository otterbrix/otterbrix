#pragma once

// ALTER 3-phase atomic validation helpers.
//
// These are stand-alone pure validation functions invoked by ALTER operators
// BEFORE any pg_catalog write. They never mutate state, never call actors, and
// never touch the mailbox — they take their inputs by const-reference and
// return core::error_t. On success they return error_t::no_error(); on failure
// they populate a typed error_code_t plus a human-readable message.

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace components::catalog::alter_column_validators {

    // Reject duplicate column names; returns already_exists if new_column_name is in the visible set.
    core::error_t validate_column_not_duplicate(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<std::string>& visible_column_names,
                                                const std::string& new_column_name);

    // Reject a DEFAULT whose declared type does not match the column type.
    // No-op if `default_value` is std::nullopt (no DEFAULT clause supplied).
    core::error_t validate_default_value_type(std::pmr::memory_resource* resource,
                                              const components::types::complex_logical_type& column_type,
                                              const std::optional<components::types::logical_value_t>& default_value);

    // Reject a DEFAULT whose value cannot be evaluated at planning time.
    // TODO: accepts any present logical_value_t until the expression evaluator is wired in.
    core::error_t
    validate_default_value_evaluatable(std::pmr::memory_resource* resource,
                                       const std::optional<components::types::logical_value_t>& default_value);

    // Reject ALTERs whose dependent set contains an entry we cannot CASCADE.
    // `dependents` are (object_kind, object_oid) pairs from pg_depend; object_kind
    // matches the pg_depend.classid encoding used elsewhere in the catalog.
    // TODO: stub returning no_error; wire handler table (FK/view/check/index/computed).
    core::error_t
    validate_cascade_dependencies(std::pmr::memory_resource* resource,
                                  const std::pmr::vector<std::pair<int, components::catalog::oid_t>>& dependents);

    // Error-returning wrapper over encode_default_spec (system_table_schemas.hpp).
    // Writes the encoded form into `out_spec`. A complex value encodes to an empty
    // string; that is forwarded as success, so callers must treat "no encoded
    // default" identically to "no default supplied".
    core::error_t encode_default_spec_ec(std::pmr::memory_resource* resource,
                                         const std::optional<components::types::logical_value_t>& default_value,
                                         std::pmr::string& out_spec);

} // namespace components::catalog::alter_column_validators

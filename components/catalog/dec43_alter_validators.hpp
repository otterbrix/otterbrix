#pragma once

// Block C §3.5 dec 43 V1 — ALTER 3-phase atomic validation helpers (Phase 1).
//
// These are stand-alone pure validation functions invoked by ALTER operators
// BEFORE any pg_catalog write. They never mutate state, never call actors, and
// never touch the mailbox — they take their inputs by const-reference and
// return core::error_t. On success they return error_t::no_error(); on failure
// they populate a typed error_code_t plus a human-readable message.
//
// Phase 2 integration (separate task) will rewire ALTER operators to consult
// these helpers and short-circuit before any catalog_write_t is emitted.

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace components::catalog::dec43 {

    // Reject duplicate column names. Performs a linear scan of the visible
    // column-name set on the target relation; if `new_column_name` already
    // appears, returns error_code_t::already_exists.
    core::error_t validate_column_not_duplicate(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<std::string>& visible_column_names,
                                                const std::string& new_column_name);

    // Reject a DEFAULT whose declared type does not match the column type.
    // No-op if `default_value` is std::nullopt (no DEFAULT clause supplied).
    core::error_t
    validate_default_value_type(std::pmr::memory_resource* resource,
                                const components::types::complex_logical_type& column_type,
                                const std::optional<components::types::logical_value_t>& default_value);

    // Reject a DEFAULT whose value cannot be evaluated at planning time.
    // TODO(dec43-phase2): currently accepts any present logical_value_t; once
    // the expression evaluator is wired in, fold constant-folding here so we
    // can reject e.g. malformed expression trees or unresolved references.
    core::error_t
    validate_default_value_evaluatable(std::pmr::memory_resource* resource,
                                       const std::optional<components::types::logical_value_t>& default_value);

    // Reject ALTERs whose dependent set contains an entry we cannot CASCADE.
    // `dependents` is the list of (object_kind, object_oid) pairs harvested
    // from pg_depend for the target column/relation. object_kind matches the
    // pg_depend.classid encoding used elsewhere in the catalog.
    //
    // TODO(dec43-phase2): wire full handler table (FK / view / check_constraint
    // / index / computed_column). For now this returns no_error and is a
    // placeholder so callers can be written against the final signature.
    core::error_t
    validate_cascade_dependencies(std::pmr::memory_resource* resource,
                                  const std::pmr::vector<std::pair<int, components::catalog::oid_t>>& dependents);

    // Strict, error-returning replacement for the silent-fail encode_default_spec
    // (declared in system_table_schemas.hpp). On success writes the encoded form
    // into `out_spec` and returns no_error. On a present-but-complex value the
    // legacy encoder produces an empty string; we forward that result and signal
    // success — the caller is expected to treat "no encoded default" the same as
    // "no default supplied". A future revision may upgrade complex-type defaults
    // to a real error code once a richer encoder lands.
    core::error_t encode_default_spec_ec(std::pmr::memory_resource* resource,
                                         const std::optional<components::types::logical_value_t>& default_value,
                                         std::pmr::string& out_spec);

} // namespace components::catalog::dec43

#pragma once

#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <string>
#include <vector>

namespace components::catalog::alter_column_validators {

    // Reject duplicate column names; returns already_exists if new_column_name is in the visible set.
    core::error_t validate_column_not_duplicate(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<std::string>& visible_column_names,
                                                const std::string& new_column_name);

    // Rejects a DEFAULT whose type doesn't match the column type; a no-op if default_value is nullopt.
    core::error_t validate_default_value_type(std::pmr::memory_resource* resource,
                                              const components::types::complex_logical_type& column_type,
                                              const std::optional<components::types::logical_value_t>& default_value);

    // DROP COLUMN dependents are not validated here: telling a blocking FK (pg_depend.deptype='n') from an owned
    // index/constraint ('i') needs two more catalog reads, so operator_alter_column_drop_t does that check instead.

    // out_spec is empty ONLY when no default was supplied; an unencodable default fails the statement instead.
    core::error_t encode_default_spec_ec(std::pmr::memory_resource* resource,
                                         const std::optional<components::types::logical_value_t>& default_value,
                                         std::pmr::string& out_spec);

} // namespace components::catalog::alter_column_validators

#pragma once

#include "catalog_view.hpp"

#include <components/context/execution_context.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

#include <actor-zeta/detail/future.hpp>

#include <vector>

namespace services::dispatcher {

    // Returns true if ct.type_name() maps to a known built-in logical type.
    bool resolve_builtin(components::types::complex_logical_type& ct);

    void apply_resolved(components::types::complex_logical_type& ct,
                        const resolved_type_t* rt);

    // Resolves a single UNKNOWN type by querying catalog_view.
    actor_zeta::unique_future<void>
    resolve_one_type(components::types::complex_logical_type& ct,
                     catalog_view_t& view,
                     components::execution_context_t ctx);

    // Resolves UNKNOWN types in all columns (including STRUCT fields and ARRAY element types).
    actor_zeta::unique_future<void>
    resolve_column_definitions(std::vector<components::table::column_definition_t>& cols,
                                catalog_view_t& view,
                                components::execution_context_t ctx);

} // namespace services::dispatcher
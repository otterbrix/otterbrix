#pragma once

#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

#include <vector>

// Real type lives in services::catalog_resolve; impl::plan_resolve_index_t
// below is an alias (see plan_resolve_index.hpp).
namespace services::catalog_resolve {
    struct plan_resolve_index_t;
} // namespace services::catalog_resolve

namespace services::dispatcher {

    namespace impl {
        using ::services::catalog_resolve::plan_resolve_index_t;
    }

    // Returns true if ct.type_name() maps to a known built-in logical type.
    bool resolve_builtin(components::types::complex_logical_type& ct);

    // Resolves a single UNKNOWN type from the plan-tree idx (passed
    // explicitly). Pure sync — the resolve_type operators must have stamped
    // the relevant metadata before this is called.
    void resolve_one_type(components::types::complex_logical_type& ct, const impl::plan_resolve_index_t* idx);

    // Resolves UNKNOWN types in all columns (including STRUCT fields and
    // ARRAY element types) using the plan-tree idx.
    void resolve_column_definitions(std::vector<components::table::column_definition_t>& cols,
                                    const impl::plan_resolve_index_t* idx);

} // namespace services::dispatcher
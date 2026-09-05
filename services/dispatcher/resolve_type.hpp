#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

#include <vector>

namespace services::dispatcher {

    using components::logical_plan::catalog_resolves_t;

    // Returns true if ct.type_name() maps to a known built-in logical type.
    bool resolve_builtin(components::types::complex_logical_type& ct);

    // Resolves a single UNKNOWN type from the plan's resolved type entries. Pure
    // sync — the resolve-type operator must have stamped them before this is called.
    // Returns the oid of the resolved type — a UDT's rides on its catalog
    // metadata, a built-in's comes from the static table — or INVALID_OID if
    // the type did not resolve. Callers that only need ct discard it.
    components::catalog::oid_t resolve_one_type(components::types::complex_logical_type& ct,
                                                const catalog_resolves_t* resolves);

    // Resolves UNKNOWN types in all columns (including STRUCT fields and
    // ARRAY element types).
    void resolve_column_definitions(std::vector<components::table::column_definition_t>& cols,
                                    const catalog_resolves_t* resolves);

    void resolve_expression_types(const components::logical_plan::node_ptr& node, const catalog_resolves_t* resolves);

} // namespace services::dispatcher
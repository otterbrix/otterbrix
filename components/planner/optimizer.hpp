#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner {

    // Early optimization pass. Runs BEFORE the schema validator / enrich.
    // Safe rules only — those that don't need resolved column indices or
    // table OIDs.
    //   - constant_folding (on parameter expressions)
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters);

    // Late optimization pass. Runs AFTER validate_schema +
    // stamp_oids_from_resolves, so node->table_oid() is populated and
    // sibling catalog_resolve_table_t nodes carry resolved_metadata().
    // Schema-aware rules go here.
    //   - column_pruning (annotates node_aggregate_t with projected_cols)
    //
    // Schema info is read from the plan tree itself (sibling resolves);
    // the optimizer is self-contained and needs no external catalog handle.
    logical_plan::node_ptr post_validate_optimize(std::pmr::memory_resource* resource, logical_plan::node_ptr node);

} // namespace components::planner
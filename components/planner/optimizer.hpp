#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner {

    // Single optimization pass. Runs AFTER the planner rewrite, i.e. after
    // resolve → validate → enrich → planner.create_plan, so node->table_oid()
    // is populated, sibling node_catalog_resolve_t (resolve_kind::table) nodes
    // carry resolved_metadata(), and the schema stamps key.side()/key.path() set by
    // validate_schema are present.
    // Rules (in order):
    //   - constant_folding (on parameter expressions)
    //   - pushdown_filter (skipped when enable_pushdown is false)
    //   - hash_join selection (needs the validate_schema stamps)
    // On DDL trees (sequence_t of primitive writes) it is a harmless no-op:
    // the planner leaves the match_t/join_t/aggregate_t these rules target
    // intact (DML wrappers sit on top; DDL has no such nodes).
    // enable_pushdown defaults to true; callers that want a non-pushed-down
    // plan (the Python relation API's optimize=False) pass false to gate only
    // the pushdown_filter rule, leaving const-folding and hash-join intact.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    bool enable_pushdown = true);

} // namespace components::planner

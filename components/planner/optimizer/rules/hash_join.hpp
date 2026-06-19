#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Stamps a hash-algo annotation on every node_join_t whose ON condition is a
    // single eq(left.key, right.key) — and whose join type is inner/left/right/full —
    // by calling node_join_t::set_equi_columns(left_col, right_col), which records the
    // matched column indices and flips node_join_t::algo() to hash. The planner
    // (create_plan_join) reads the annotation and lowers it to operator_hash_join_t
    // (O(L+R)); any join the rule leaves as nested keeps the nested-loop
    // operator_join_t. The equi-detection lives here, not in the planner, so the
    // planner stays a pure 1:1 lowering.
    //
    // Must run AFTER validate_schema, which stamps key.side()/key.path() — the rule
    // reads those to identify the equi columns.
    //
    // Returns the root; the join nodes are annotated in place.
    logical_plan::node_ptr rewrite_hash_joins(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer

#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Rewrites every node_join_t whose ON condition is a single
    // eq(left.key, right.key) — and whose join type is inner/left/right/full —
    // into a node_hash_join_t carrying the matched (left_col, right_col) column
    // indices. The planner then lowers hash_join_t straight to
    // operator_hash_join_t (O(L+R)); any join the rule leaves as join_t stays a
    // nested-loop operator_join_t. The equi-detection used to live in the planner
    // (create_plan_join); it now lives here so the planner is a pure 1:1 lowering.
    //
    // Must run AFTER validate_schema, which stamps key.side()/key.path() — the rule
    // reads those to identify the equi columns.
    //
    // Returns the (possibly new) root; nested joins are replaced in place.
    logical_plan::node_ptr rewrite_hash_joins(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer

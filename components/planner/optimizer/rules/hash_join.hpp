#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Stamps algo() == hash on every inner/left/right/full node_join_t whose ON condition is a single eq of
    // two operands that each read one side only (any expression over its columns, no volatile call, at
    // least one column), one per side, and puts the left side's operand first. create_plan_join lowers it
    // to operator_hash_join_t, which evaluates the two operands as the probe and build keys; any join the
    // rule leaves as nested keeps the nested-loop operator_join_t.
    //
    // Must run AFTER validate_schema, which stamps key.side()/key.path() and each call's volatility.
    //
    // Returns the root; the join nodes are annotated in place.
    logical_plan::node_ptr rewrite_hash_joins(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer

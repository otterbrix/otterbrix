#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Pushes a match_t (filter) sitting under an aggregate_t down through
    // any of the following patterns, when the rewrite is provably safe:
    //   - aggregate over sort        -> filter lands under the sort
    //   - aggregate over identity-projection select (no rename, no cost regression)
    //   - aggregate over pure-projection group (no aggregation expressions)
    //   - aggregate over group with aggregations -- splits conjuncts:
    //       conjuncts referencing only group keys are pushed, the rest stay above
    //   - aggregate over join        -- splits conjuncts per side:
    //       left-only conjuncts wrap the left input, right-only wrap the right input,
    //       cross-side conjuncts remain as a residual filter on the join output
    //
    // Returns the (possibly new) root of the rewritten subtree. The caller
    // must reassign because the rule may collapse the outer aggregate into
    // its child.
    logical_plan::node_ptr pushdown_filter(std::pmr::memory_resource* resource,
                                           logical_plan::node_ptr node);

} // namespace components::planner::optimizer

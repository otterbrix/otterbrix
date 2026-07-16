#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Clear a provably-redundant DISTINCT that sits on top of a GROUP BY.
    //
    // A GROUP BY already emits rows DISTINCT on its key columns. When the outer
    // DISTINCT is over a projection that is a SUPERSET of those keys, the grouped
    // output is already distinct on the projection, so the extra operator_distinct
    // ("Unique") pass is pure overhead. Post-order walk; at every node_aggregate_t
    // with is_distinct() and a group_t child, clear the flag iff:
    //
    //   plain DISTINCT      : the group keys are a SUBSET of the projected columns
    //                         (group_keys ⊆ projection). The GROUP BY leading key
    //                         columns occupy the contiguous output ordinals {0..G-1};
    //                         DISTINCT is cleared when every one of those ordinals is
    //                         referenced by a plain get_field projection column.
    //   DISTINCT ON (cols)  : the group keys are a SUBSET of the ON columns
    //                         (group_keys ⊆ cols), same direction.
    //
    // The direction is deliberate and NOT symmetric: `SELECT DISTINCT a GROUP BY a,b`
    // is NOT redundant (two groups (a,b1),(a,b2) both emit a) — there {a,b} ⊄ {a}, so
    // DISTINCT stays. A DISTINCT with no GROUP BY (and a scalar aggregate with no
    // explicit keys, G==0) is never touched. Any group shape that is not a clean
    // all-group_field grouping, or a projection column that cannot be resolved to a
    // single base-column ordinal, is treated conservatively (DISTINCT kept).
    //
    // Semantics-preserving: only the is_distinct flag (and, when cleared, the now-dead
    // DISTINCT ON key list) change; the plan is otherwise identical. Reads the
    // group-output column ordinals stamped by validate_schema, so it must run on the
    // post-validate tree. Returns the root, mutated in place.
    logical_plan::node_ptr drop_redundant_distinct(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer

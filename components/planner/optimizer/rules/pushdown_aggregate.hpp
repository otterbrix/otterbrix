#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Annotate an aggregate sub-plan as "pushable to the single owning agent".
    // Post-order walk; at every node_aggregate_t that targets ONE owned base
    // table (table_oid() valid, and no join_t / nested aggregate_t / data_t /
    // cte_scan_t / union_t / intersect_t / recursive_cte_t child), find the
    // node_group_t child and stamp node_group_t::set_pushdown(true) UNLESS a
    // skip condition holds:
    //   (a) shape mismatch (any of the above children) => not one owned table;
    //   (b) an aggregate that is not fragment-mergeable — DISTINCT, or a kind
    //       outside {SUM, COUNT, MIN, MAX, AVG} (only these have kernel merges);
    //   (c) HAVING present — a coordinator kernel-merge reduce would never
    //       evaluate HAVING, so pushing would be wrong. Hard correctness gate.
    // (The relkind computed/matview gate is NOT decidable here — physgen keeps
    // the final gate.) The rule is unconditional-by-shape; the optimize() bool
    // (can_push_to_agent) is a CAPABILITY precondition — an owning agent must be
    // reachable — not a rollout flag. When it is false (disk-less/in-memory mode)
    // there is no agent to push to, so optimize() never calls the rule and
    // nothing is stamped.
    //
    // ANNOTATION only — logical semantics are unchanged; physgen lowering
    // (create_plan_aggregate) consumes the flag. Signature mirrors
    // rewrite_hash_joins. Returns the root; group nodes annotated in place.
    logical_plan::node_ptr pushdown_aggregate(std::pmr::memory_resource* resource, logical_plan::node_ptr root);

} // namespace components::planner::optimizer

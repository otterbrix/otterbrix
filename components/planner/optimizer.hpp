#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/host_hooks.hpp>

#include <set>
#include <span>

namespace components::planner {

    // Single optimization pass. Runs AFTER the planner rewrite, i.e. after
    // resolve → validate → enrich → planner.create_plan, so node->table_oid()
    // is populated, the plan's `resolves` table entries carry their resolved
    // metadata, and the schema stamps key.side()/key.path() set by validate_schema
    // are present.
    // Rules (in order):
    //   - constant_folding (on parameter expressions)
    //   - pushdown_filter
    //   - hash_join selection (needs the validate_schema stamps)
    //   - pushdown_aggregate — annotates pushable single-owned-table aggregates.
    //     Runs whenever `can_push_to_agent` is true. This is NOT a rollout flag: it
    //     is a hard CAPABILITY precondition — false only when the executor was given
    //     no disk-manager address, so there is no owning agent to push to and pushable
    //     aggregates must stay coordinator-side. Defaults false so a bare
    //     3-arg caller (unit/planner tests without a disk manager) does not stamp.
    // On DDL trees (sequence_t of primitive writes) it is a harmless no-op:
    // the planner leaves the match_t/join_t/aggregate_t these rules target
    // intact (DML wrappers sit on top; DDL has no such nodes).
    // `deferred_parameters`: unknown type/value parameters at optimization step
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    const logical_plan::catalog_resolves_t* resolves = nullptr,
                                    bool can_push_to_agent = false,
                                    std::span<const optimizer_rule_t> host_rules = {},
                                    const std::pmr::set<core::parameter_id_t>* deferred_parameters = nullptr);

} // namespace components::planner

#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace components::planner {

    // Host-injected optimizer pass: a final rewrite the embedding host runs on the
    // fully-optimized logical tree (e.g. federation-specific reshaping). Injected
    // through the CONSTRUCTOR chain (base_spaces -> dispatcher -> executor) and
    // passed as a scalar argument here — the optimizer lives in components/planner,
    // which must not depend on services/context_storage, so it cannot ride the
    // context. Plain fn-ptr (no std::function). NEVER null — defaults to no_op_pass
    // (a Null Object returning the tree unchanged).
    using optimizer_pass_t = logical_plan::node_ptr (*)(std::pmr::memory_resource*, logical_plan::node_ptr);

    inline logical_plan::node_ptr no_op_pass(std::pmr::memory_resource*, logical_plan::node_ptr node) { return node; }

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
    //     is a hard CAPABILITY precondition — false only in disk-less (in-memory)
    //     mode, where there is no owning agent to push to, so pushable aggregates
    //     architecturally must stay coordinator-side. Defaults false so a bare
    //     3-arg caller (unit/planner tests without a disk manager) does not stamp.
    // On DDL trees (sequence_t of primitive writes) it is a harmless no-op:
    // the planner leaves the match_t/join_t/aggregate_t these rules target
    // intact (DML wrappers sit on top; DDL has no such nodes).
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    const logical_plan::catalog_resolves_t* resolves = nullptr,
                                    bool can_push_to_agent = false,
                                    optimizer_pass_t host_pass = &no_op_pass);

} // namespace components::planner

#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner::optimizer {

    // Stamps an exec_strategy annotation (in_memory vs spill) on every
    // node_sort_t / node_group_t / node_join_t in the tree. The decision is a
    // global stamp driven by a single `spill_enabled` primitive:
    //   - spill_enabled == false  → every node stays in_memory (the default)
    //   - spill_enabled == true   → every grace-eligible node is stamped spill
    //
    // The annotation is read by create_plan_sort / create_plan_group /
    // create_plan_join, which lower a spill node to the grace / external-sort
    // operator and an in_memory node to the regular operator. The annotation
    // does NOT change the logical semantics.
    //
    // Why a plain bool and not the whole context_storage_t / config_disk?
    // The planner library is a low-level component that does not link spdlog,
    // while context_storage.hpp and configuration.hpp transitively include
    // log/log.hpp. Passing the already-resolved primitive across this layer
    // boundary keeps the dependency graph acyclic. The VALUE still originates
    // from context_storage_t::disk_config (R10: config flows through
    // context_storage, not a thread_local) — the executor resolves it before
    // calling optimize().
    //
    // Returns the root; nodes are annotated in place.
    logical_plan::node_ptr spill_strategy(logical_plan::node_ptr root, bool spill_enabled);

} // namespace components::planner::optimizer

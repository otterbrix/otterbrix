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
    //   - pushdown_filter
    //   - hash_join selection (needs the validate_schema stamps)
    //   - spill_strategy (stamps exec_strategy annotations from spill_enabled)
    // On DDL trees (sequence_t of primitive writes) it is a harmless no-op:
    // the planner leaves the match_t/join_t/aggregate_t these rules target
    // intact (DML wrappers sit on top; DDL has no such nodes).
    //
    // `spill_enabled` drives the spill_strategy rule: when true every
    // sort/group/join node is stamped with the spill exec_strategy; when false
    // (the default) they stay in_memory. The value originates from
    // context_storage_t::disk_config (R10: config flows through context_storage,
    // not a thread_local) — the executor resolves it before calling optimize().
    // The planner is a low-level component that does not link spdlog, so the
    // whole context_storage_t / config_disk cannot cross this boundary; the
    // already-resolved primitive can. Defaults to false so the 50+ existing
    // call sites keep compiling unchanged.
    logical_plan::node_ptr optimize(std::pmr::memory_resource* resource,
                                    logical_plan::node_ptr node,
                                    logical_plan::parameter_node_t* parameters,
                                    bool spill_enabled = false);

} // namespace components::planner

#pragma once

// Dispatcher-side catalog enrichment pass.
// Called after validate_schema (catalog_view cache is warm) and before
// planner_t::create_plan.  Fills logical plan node fields (outgoing_fks,
// not_null_cols, etc.) from the catalog_view snapshot + disk actor calls.
// The planner then does pure structural rewrite reading those fields — no
// external context parameter needed.

#include "catalog_view.hpp"

#include <actor-zeta.hpp>
#include <components/context/execution_context.hpp>
#include <components/logical_plan/node.hpp>
#include <memory_resource>

namespace services::dispatcher {

    // Walks the plan tree and fills catalog metadata fields into DML nodes
    // (node_insert_t, node_update_t, node_delete_t).  DDL nodes are left
    // untouched — they go through execute_ddl_inline unchanged.
    //
    // Precondition: validate_schema has already co_awaited get_table() for every
    // table referenced in the plan, so try_get_table() hits the cache synchronously.
    actor_zeta::unique_future<void>
    enrich_plan(components::logical_plan::node_ptr root,
                catalog_view_t& view,
                actor_zeta::address_t disk_address,
                components::execution_context_t ctx,
                std::pmr::memory_resource* resource);

} // namespace services::dispatcher

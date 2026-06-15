#pragma once

#include <components/catalog/oid_batch.hpp>
#include <components/logical_plan/node.hpp>

namespace components::planner {

    class planner_t {
    public:
        // DML path — no OIDs needed.
        auto create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node) -> logical_plan::node_ptr;

        // DDL path — oid_batch holds pre-allocated OIDs for building pg_class /
        // pg_attribute rows inside the planner without async disk access.
        auto create_plan(std::pmr::memory_resource* resource,
                         logical_plan::node_ptr node,
                         catalog::oid_batch_t oid_batch) -> logical_plan::node_ptr;
    };

    // OID demand for a DDL node: the exact number of OIDs the DDL create_plan
    // path (walk_ddl → rewrite_create_*) consumes from the oid_batch. The single
    // source of truth for the per-kind counts, so callers (the executor) need
    // not duplicate the formulas. Returns 0 for DROP/ALTER (no pre-allocation),
    // for CREATE MATERIALIZED VIEW with no inferred columns (planner bails), and
    // for DML / non-DDL nodes. `node` must be the effective DDL node (the result
    // of catalog_resolve::effective_root_node after resolve-wrap).
    std::size_t compute_oid_demand(const logical_plan::node_t* node);

} // namespace components::planner

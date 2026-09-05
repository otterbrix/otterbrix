#pragma once

#include <components/catalog/oid_batch.hpp>
#include <components/logical_plan/node.hpp>

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <vector>

namespace components::planner {

    class planner_t {
    public:
        // DML path — no OIDs needed.
        auto create_plan(std::pmr::memory_resource* resource, logical_plan::node_ptr node) -> logical_plan::node_ptr;

        // DDL path — `oids` is what the caller's OID allocation round delivered and `need`
        // is compute_oid_demand's answer FOR THIS NODE. The two arrive together, and the
        // batch is built (and checked against the demand) HERE, in catalog::oid_batch_t::make:
        // a caller cannot hand in a batch nobody compared with the demand, because it cannot
        // hand in a batch at all. That is deliberate — the previous shape took a ready
        // oid_batch_t, and the one caller filled it straight from a round that reported its
        // failures as an empty vector, so a failed allocation reached the rewrite as a batch
        // the planner then read past the end of.
        //
        // Refuses (no fallback, rule 6) when the round came up short, and when the rewrite
        // consumed more OIDs than `need` — in that case the half-stamped tree is dropped
        // unbuilt rather than executed with an INVALID_OID identity in it.
        // `need == 0` with an empty `oids` is a normal success: see oid_batch_t::make.
        [[nodiscard]] auto create_plan(std::pmr::memory_resource* resource,
                                       logical_plan::node_ptr node,
                                       std::vector<catalog::oid_t> oids,
                                       std::size_t need) -> core::result_wrapper_t<logical_plan::node_ptr>;
    };

    // OID demand for a DDL node: the exact number of OIDs the DDL create_plan
    // path (walk_ddl → rewrite_create_*) consumes from the oid_batch. The single
    // source of truth for the per-kind counts, so callers (the executor) need
    // not duplicate the formulas. Returns 0 for DROP/ALTER (no pre-allocation),
    // for CREATE MATERIALIZED VIEW with no inferred columns (planner bails), and
    // for DML / non-DDL nodes.
    //
    // `node` must be THE SAME node create_plan is then handed: the count is per-kind and
    // walk_ddl starts at the root it is given, so a demand computed on one node and a
    // rewrite walked from another would disagree by construction — and a demand of 0
    // computed on a wrapper whose child does allocate is exactly the empty batch this
    // whole path now refuses. (The comment that used to stand here named
    // catalog_resolve::effective_root_node; no such function exists in the tree.)
    std::size_t compute_oid_demand(const logical_plan::node_t* node);

} // namespace components::planner

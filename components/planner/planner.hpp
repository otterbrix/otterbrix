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

        // DDL path — oids/need arrive together so the batch is built and checked against the demand
        // HERE (oid_batch_t::make), rather than letting a caller hand in an already-built batch that
        // skipped the check. Refuses, no fallback, on a short allocation or if the rewrite
        // consumed more than `need`; need==0 with empty oids is a normal success.
        [[nodiscard]] auto create_plan(std::pmr::memory_resource* resource,
                                       logical_plan::node_ptr node,
                                       std::vector<catalog::oid_t> oids,
                                       std::size_t need) -> core::result_wrapper_t<logical_plan::node_ptr>;
    };

    // The single source of truth for how many OIDs each DDL kind consumes (0 for DROP/ALTER, DML, or
    // CREATE MATERIALIZED VIEW with no inferred columns). `node` must be the SAME node create_plan is
    // then handed — walk_ddl starts from the given root, so a demand computed elsewhere would
    // disagree by construction.
    std::size_t compute_oid_demand(const logical_plan::node_t* node);

} // namespace components::planner

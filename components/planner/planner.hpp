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

} // namespace components::planner

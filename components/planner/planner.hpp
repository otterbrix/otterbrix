#pragma once

#include <components/logical_plan/node.hpp>

namespace components::planner {

    class planner_t {
    public:
        // Logical plan rewrite (Phase 1.5).
        // Walks the plan tree and inserts constraint nodes driven by catalog
        // metadata that the dispatcher's enrich pass has already written into
        // the node fields.  No external catalog context needed.
        auto create_plan(std::pmr::memory_resource*  resource,
                         logical_plan::node_ptr      node)
            -> logical_plan::node_ptr;
    };

} // namespace components::planner

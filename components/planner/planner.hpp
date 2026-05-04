#pragma once

#include <components/logical_plan/node.hpp>

// V4 catalog facade. Planner currently doesn't read it — the parameter is wired so
// future passes (e.g. type-aware folding, schema-aware rewrites) can pull schemas
// without re-routing the plumbing. Forward-declared to keep planner free of any
// dispatcher-layer includes; only services that already depend on dispatcher need
// to know the concrete type.
namespace services::dispatcher {
    class catalog_view_t;
}

namespace components::planner {

    class planner_t {
    public:
        auto create_plan(std::pmr::memory_resource* resource,
                         logical_plan::node_ptr node,
                         const services::dispatcher::catalog_view_t* catalog = nullptr)
            -> logical_plan::node_ptr;
    };

} // namespace components::planner

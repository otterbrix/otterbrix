#pragma once

#include <components/casts/cast_registry.hpp>
#include <components/compute/function.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>

#include <memory_resource>

namespace services::dispatcher::validation {

    struct validation_context_t {
        std::pmr::memory_resource* resource;
        // Null when the plan needed no catalog lookups (hand-built plans, most unit tests).
        const components::logical_plan::catalog_resolves_t* resolves;
        const components::casts::cast_registry_t& cast_registry;
        const components::compute::function_registry_t& function_registry;
        const components::graph_execution_context& execution_context;
    };

} // namespace services::dispatcher::validation

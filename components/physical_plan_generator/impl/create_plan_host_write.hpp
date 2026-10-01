#pragma once

#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    // An INSERT / UPDATE / DELETE whose target is a host relation: the host's operator does the write. An INSERT
    // feeds it its source, cast to the declared columns.
    plan_result_t create_plan_host_write(const context_storage_t& context,
                                         const components::compute::function_registry_t& function_registry,
                                         const components::logical_plan::node_ptr& node,
                                         const components::logical_plan::storage_parameters* params);

} // namespace services::planner::impl

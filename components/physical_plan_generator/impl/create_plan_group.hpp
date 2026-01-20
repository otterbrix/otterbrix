#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <services/memory_storage/context_storage.hpp>

namespace services::collection::planner::impl {

    components::collection::operators::operator_ptr create_plan_group(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node);

}

namespace services::table::planner::impl {

    components::base::operators::operator_ptr
    create_plan_group(const context_storage_t& context,
                      const components::compute::function_registry_t& function_registry,
                      const components::logical_plan::node_ptr& node);

}

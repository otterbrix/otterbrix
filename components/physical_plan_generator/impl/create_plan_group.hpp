#pragma once

#include <components/physical_plan_generator/create_plan.hpp>

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // The GROUP BY of `group` as the operator that runs it: otterbrix's own, and the disk's when it takes the
    // GROUP BY of a block over one of its tables.
    components::operators::operator_ptr make_hash_group(std::pmr::memory_resource* resource,
                                                        log_t log,
                                                        const components::logical_plan::node_group_t& group);

    components::operators::operator_ptr
    create_plan_group(const context_storage_t& context,
                      const components::compute::function_registry_t& function_registry,
                      const components::logical_plan::node_ptr& node,
                      const components::logical_plan::storage_parameters* params = nullptr);
} // namespace services::planner::impl

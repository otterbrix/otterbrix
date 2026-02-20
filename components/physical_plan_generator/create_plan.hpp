#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner {

    components::operators::operator_ptr create_plan(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit,
                                                          const components::logical_plan::storage_parameters* params = nullptr);

} // namespace services::planner
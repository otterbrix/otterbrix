#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

#include <vector>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_vector_search(const context_storage_t& context,
                              const components::logical_plan::node_ptr& node,
                              const components::logical_plan::storage_parameters* params,
                              const std::vector<size_t>& projected_cols = {});

} // namespace services::planner::impl

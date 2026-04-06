#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>
#include <vector>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_match(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node,
                                                          components::logical_plan::limit_t limit,
                                                          std::vector<size_t> projected_cols = {});

    components::operators::operator_ptr create_plan_having(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           components::logical_plan::limit_t limit);

} // namespace services::planner::impl

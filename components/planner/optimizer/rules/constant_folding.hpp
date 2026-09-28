#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

#include <set>

namespace components::planner::optimizer {

    // `deferred_parameters` name parameters whose value is not known yet
    void fold_constants(std::pmr::memory_resource* resource,
                        const logical_plan::node_ptr& node,
                        logical_plan::parameter_node_t* parameters,
                        const std::pmr::set<core::parameter_id_t>* deferred_parameters = nullptr);

} // namespace components::planner::optimizer

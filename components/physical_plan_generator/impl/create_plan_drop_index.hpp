#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::collection::planner::impl {

    components::collection::operators::operator_ptr
    create_plan_drop_index(const context_storage_t& context, const components::logical_plan::node_ptr& node);

}

namespace services::table::planner::impl {

    components::base::operators::operator_ptr create_plan_drop_index(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node);

}

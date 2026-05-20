#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Builds the physical operator_dynamic_cascade_delete_t from a
    // node_dynamic_cascade_delete_t. Wired into create_plan.cpp's switch on
    // node_type. Task #49 will add the corresponding planner-rewrite that
    // actually emits this node from DROP statements.
    components::operators::operator_ptr
    create_plan_dynamic_cascade_delete(const context_storage_t& context,
                                       const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl

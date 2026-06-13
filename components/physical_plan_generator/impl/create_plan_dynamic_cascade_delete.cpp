#include "create_plan_dynamic_cascade_delete.hpp"

#include <components/logical_plan/node_dynamic_cascade_delete.hpp>
#include <components/physical_plan/operators/operator_dynamic_cascade_delete.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_dynamic_cascade_delete(const context_storage_t& context,
                                       const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_dynamic_cascade_delete_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_dynamic_cascade_delete_t(context.resource,
                                                                                                 context.log.clone(),
                                                                                                 n->seed_classid(),
                                                                                                 n->seed_objid(),
                                                                                                 n->behavior()));
    }

} // namespace services::planner::impl

#include "create_plan_primitive_delete.hpp"

#include <components/logical_plan/node_primitive_delete.hpp>
#include <components/physical_plan/operators/operator_primitive_delete.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_primitive_delete(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_primitive_delete_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_primitive_delete_t(context.resource,
                                                                                           context.log.clone(),
                                                                                           n->catalog_table_oid(),
                                                                                           n->oid_col_idx(),
                                                                                           n->target_oid()));
    }

} // namespace services::planner::impl
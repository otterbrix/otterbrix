#include "create_plan_alter_column_add.hpp"

#include <components/logical_plan/node_alter_column_add.hpp>
#include <components/physical_plan/operators/operator_alter_column_add.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_alter_column_add(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_alter_column_add_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_alter_column_add_t(context.resource,
                                                                                           context.log.clone(),
                                                                                           n->table_oid(),
                                                                                           n->column()));
    }

} // namespace services::planner::impl

#include "create_plan_alter_column_drop.hpp"

#include <components/logical_plan/node_alter_column_drop.hpp>
#include <components/physical_plan/operators/operator_alter_column_drop.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_alter_column_drop(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_alter_column_drop_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_alter_column_drop_t(context.resource,
                                                                                            context.log.clone(),
                                                                                            n->table_oid(),
                                                                                            n->namespace_oid(),
                                                                                            n->column_name(),
                                                                                            n->attoid(),
                                                                                            n->behavior()));
    }

} // namespace services::planner::impl

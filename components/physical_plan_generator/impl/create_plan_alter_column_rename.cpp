#include "create_plan_alter_column_rename.hpp"

#include <components/logical_plan/node_alter_column_rename.hpp>
#include <components/physical_plan/operators/operator_alter_column_rename.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_alter_column_rename(const context_storage_t& context, const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_alter_column_rename_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_alter_column_rename_t(context.resource,
                                                                                              context.log.clone(),
                                                                                              n->table_oid(),
                                                                                              n->attoid(),
                                                                                              n->old_name(),
                                                                                              n->new_name()));
    }

} // namespace services::planner::impl

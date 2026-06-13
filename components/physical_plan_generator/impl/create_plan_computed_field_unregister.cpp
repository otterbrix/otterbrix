#include "create_plan_computed_field_unregister.hpp"

#include <components/logical_plan/node_computed_field_unregister.hpp>
#include <components/physical_plan/operators/operator_computed_field_unregister.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_computed_field_unregister(const context_storage_t& context,
                                          const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_computed_field_unregister_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_computed_field_unregister_t(context.resource,
                                                                                                    context.log.clone(),
                                                                                                    n->table_oid(),
                                                                                                    n->attoid(),
                                                                                                    n->column_name()));
    }

} // namespace services::planner::impl

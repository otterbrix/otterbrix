#include "create_plan_computed_field_register.hpp"

#include <components/logical_plan/node_alter_column.hpp>
#include <components/physical_plan/operators/operator_computed_field_register.hpp>

#include <vector>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_computed_field_register(const context_storage_t& context,
                                        const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_alter_column_t*>(node.get());
        // node holds registered_cols as a pmr::vector (rule 14); the operator
        // ctor takes std::vector by value — copy the range across.
        const auto& cols = n->registered_cols();
        std::vector<components::table::column_definition_t> columns(cols.begin(), cols.end());
        return boost::intrusive_ptr(new components::operators::operator_computed_field_register_t(context.resource,
                                                                                                  context.log.clone(),
                                                                                                  n->table_oid(),
                                                                                                  std::move(columns)));
    }

} // namespace services::planner::impl

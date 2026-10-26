#include "create_plan_alter_column_add.hpp"

#include <components/logical_plan/node_alter_column.hpp>
#include <components/physical_plan/operators/operator_alter_column_add.hpp>

namespace services::planner::impl {

    plan_result_t create_plan_alter_column_add(const context_storage_t& context,
                                               const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_alter_column_t*>(node.get());
        components::operators::operator_ptr add = boost::intrusive_ptr(
            new components::operators::operator_alter_column_add_t(context.resource,
                                                                   context.log.clone(),
                                                                   n->table_oid(),
                                                                   n->column(),
                                                                   !n->remote_table().is_remote()));
        if (!n->remote_table().is_remote()) {
            return add;
        }
        const auto& remote = n->remote_table();
        std::pmr::vector<components::planner::remote_column_t> columns(context.resource);
        const auto table = remote_table_of(remote, columns);
        VALUE_OR_RETURN(auto* connector, connector_for(context, table.name));
        const auto& column = n->column();
        const components::planner::remote_alter_t alter{
            components::planner::remote_alter_t::kind_t::add_column,
            components::planner::remote_column_t{column.attoid(), column.name(), column.type(), column.is_not_null()}};
        return connector_then_catalog(
            context,
            storage_operator(context.resource, remote.name, connector->make_alter_table(context, table, alter)),
            std::move(add));
    }

} // namespace services::planner::impl

#include "create_plan_alter_column_drop.hpp"

#include <components/logical_plan/node_alter_column.hpp>
#include <components/physical_plan/operators/operator_alter_column_drop.hpp>

#include <algorithm>

namespace services::planner::impl {

    plan_result_t create_plan_alter_column_drop(const context_storage_t& context,
                                                const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_alter_column_t*>(node.get());
        components::operators::operator_ptr drop = boost::intrusive_ptr(
            new components::operators::operator_alter_column_drop_t(context.resource,
                                                                    context.log.clone(),
                                                                    n->table_oid(),
                                                                    n->column_name(),
                                                                    n->target(),
                                                                    n->relkind(),
                                                                    n->attoid(),
                                                                    n->behavior(),
                                                                    !n->remote_table().is_remote()));
        if (!n->remote_table().is_remote()) {
            return drop;
        }
        const auto& remote = n->remote_table();
        std::pmr::vector<components::planner::remote_column_t> columns(context.resource);
        const auto table = remote_table_of(remote, columns);
        VALUE_OR_RETURN(auto* connector, connector_for(context, table.name));
        const auto dropped = std::find_if(columns.begin(), columns.end(), [n](const auto& column) {
            return column.name == n->column_name().t;
        });
        // The executor refused a column the table does not have before planning.
        if (dropped == columns.end()) {
            return plan_refusal(context.resource, "DROP COLUMN of a connection's table names no column of it");
        }
        const components::planner::remote_alter_t alter{components::planner::remote_alter_t::kind_t::drop_column,
                                                        *dropped};
        // The executor checked RESTRICT before planning.
        return connector_then_catalog(
            context,
            storage_operator(context.resource, remote.name, connector->make_alter_table(context, table, alter)),
            std::move(drop));
    }

} // namespace services::planner::impl

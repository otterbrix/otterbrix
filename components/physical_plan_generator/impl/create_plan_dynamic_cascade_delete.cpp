#include "create_plan_dynamic_cascade_delete.hpp"

#include <components/logical_plan/node_dynamic_cascade_delete.hpp>
#include <components/physical_plan/operators/operator_dynamic_cascade_delete.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>

namespace services::planner::impl {

    namespace {
        // The connector's operator of one object a DROP takes of a connection's database.
        plan_result_t remote_drop_operator(const context_storage_t& context,
                                           const components::logical_plan::remote_object_t& drop) {
            using kind_t = components::logical_plan::remote_object_t::kind_t;
            if (drop.kind == kind_t::schema) {
                const components::planner::remote_schema_t schema{
                    drop.oid,
                    {drop.schema.connection, drop.schema.database, drop.schema.schema, {}}};
                VALUE_OR_RETURN(auto* connector, connector_for(context, schema.name));
                return storage_operator(context.resource,
                                        drop.schema.schema,
                                        connector->make_drop_schema(context, schema));
            }
            const auto* resolved = context.catalog_resolves->table_md(drop.table_oid);
            if (resolved == nullptr) {
                // Unreachable: the executor resolved every table of the DROP's closure before planning.
                std::fprintf(stderr, "remote_drop_operator: table oid %u is not resolved\n", drop.table_oid);
                assert(false && "remote_drop_operator: a table of the DROP's closure is not resolved");
                std::abort();
            }
            std::pmr::vector<components::planner::remote_column_t> columns(context.resource);
            const auto table = remote_table_of(*resolved, columns);
            VALUE_OR_RETURN(auto* connector, connector_for(context, table.name));
            if (drop.kind == kind_t::table) {
                return storage_operator(context.resource, resolved->name, connector->make_drop_table(context, table));
            }
            const auto dropped = std::find_if(columns.begin(), columns.end(), [&drop](const auto& column) {
                return column.attoid == drop.oid;
            });
            if (dropped == columns.end()) {
                return plan_refusal(context.resource, "DROP of a connection's column names no column of its table");
            }
            const components::planner::remote_alter_t alter{components::planner::remote_alter_t::kind_t::drop_column,
                                                            *dropped};
            return storage_operator(context.resource,
                                    resolved->name,
                                    connector->make_alter_table(context, table, alter));
        }
    } // namespace

    plan_result_t create_plan_dynamic_cascade_delete(const context_storage_t& context,
                                                     const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_dynamic_cascade_delete_t*>(node.get());
        components::operators::operator_ptr cascade =
            boost::intrusive_ptr(new components::operators::operator_dynamic_cascade_delete_t(context.resource,
                                                                                              context.log.clone(),
                                                                                              n->seed_classid(),
                                                                                              n->seed_objid(),
                                                                                              n->behavior(),
                                                                                              n->target(),
                                                                                              n->relkind()));
        if (n->remote_drops().empty()) {
            return cascade;
        }
        // The connectors' DROPs, dependents first, then the catalog's rows: steps the executor runs in order.
        auto steps =
            boost::intrusive_ptr(new components::operators::operator_sequence_t(context.resource, context.log.clone()));
        for (const auto& drop : n->remote_drops()) {
            VALUE_OR_RETURN(auto step, remote_drop_operator(context, drop));
            steps->append_step(std::move(step));
        }
        steps->append_step(std::move(cascade));
        return steps;
    }

} // namespace services::planner::impl

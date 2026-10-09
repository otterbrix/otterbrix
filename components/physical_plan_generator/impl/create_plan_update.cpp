#include "create_plan_update.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_match.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    namespace {
        // The rows come from the storage's scan, numbered in row_ids; the changed ones go to its update sink. A
        // WHERE without FROM filters above the scan; with FROM the semi-join applies it, as for a local table.
        plan_result_t create_plan_storage_update(const context_storage_t& context,
                                                 const components::compute::function_registry_t& function_registry,
                                                 const components::logical_plan::node_update_t& node_update,
                                                 const components::logical_plan::resolved_table_metadata_t& table,
                                                 const components::logical_plan::node_ptr& node_match,
                                                 const components::logical_plan::node_ptr& node_source,
                                                 components::logical_plan::limit_t limit,
                                                 std::pmr::vector<components::operators::projected_column_t> returning,
                                                 const components::logical_plan::storage_parameters* params) {
            VALUE_OR_RETURN(auto sink,
                            storage_operator(context.resource, table.name, table.storage->make_update(context)));
            VALUE_OR_RETURN(auto scan,
                            storage_operator(context.resource, table.name, table.storage->make_scan(context)));
            std::pmr::vector<components::types::complex_logical_type> columns(context.resource);
            columns.reserve(table.columns.size());
            for (const auto& column : table.columns) {
                columns.push_back(column.type);
            }
            const auto& where = node_match->expressions()[0];
            if (!node_source) {
                auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                            context.log.clone(),
                                                                                            node_update.table_oid(),
                                                                                            node_update.updates(),
                                                                                            std::move(returning)));
                plan->set_storage_sink(std::move(sink), std::move(columns));
                auto filter = boost::intrusive_ptr(
                    new components::operators::operator_match_t(context.resource, context.log.clone(), where, limit));
                filter->set_children(std::move(scan));
                plan->set_children(std::move(filter));
                return plan;
            }
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        node_update.table_oid(),
                                                                                        node_update.updates(),
                                                                                        std::move(returning),
                                                                                        where,
                                                                                        limit.limit()));
            plan->set_storage_sink(std::move(sink), std::move(columns));
            VALUE_OR_RETURN(auto source_op,
                            create_plan(context,
                                        function_registry,
                                        node_source,
                                        components::logical_plan::limit_t::unlimit(),
                                        params));
            plan->set_children(std::move(scan), std::move(source_op));
            return plan;
        }
    } // namespace

    plan_result_t create_plan_update(const context_storage_t& context,
                                     const components::compute::function_registry_t& function_registry,
                                     const components::logical_plan::node_ptr& node,
                                     const components::logical_plan::storage_parameters* params) {
        const auto* node_update = static_cast<const components::logical_plan::node_update_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_update->returning());

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_source = nullptr;
        for (auto child : node_update->children()) {
            switch (child->type()) {
                case components::logical_plan::node_type::match_t:
                    node_match = child;
                    break;
                case components::logical_plan::node_type::limit_t:
                    node_limit = child;
                    break;
                default:
                    node_source = child;
                    break;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        if (const auto* table = node->table_metadata(); table != nullptr && table->storage != nullptr) {
            return create_plan_storage_update(context,
                                              function_registry,
                                              *node_update,
                                              *table,
                                              node_match,
                                              node_source,
                                              std::move(limit),
                                              std::move(returning),
                                              params);
        }
        auto table_oid = node->table_oid();
        // The update target is always a NAMED table; a target the context cannot vouch
        // for is a table that never resolved. Validation refuses this before plan
        // generation; if that refusal is ever lost again, lowering anyway builds a sink
        // with no table behind it — an UPDATE that changes nothing and reports SUCCESS.
        if (!context.has_table_oid(table_oid)) {
            return unresolved_table_refusal(context.resource,
                                            node_update->target().database.t,
                                            node_update->target().collection.t);
        }
        if (!node_source) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        node_update->updates(),
                                                                                        std::move(returning)));
            plan->set_table_has_indexes(node->table_has_indexes());
            VALUE_OR_RETURN(auto scan, create_plan_match(context, node_match, limit));
            plan->set_children(std::move(scan));

            return plan;
        }
        // Source (UPDATE ... FROM) path: the semi-join reads ALL left rows (unlimit) and
        // operator_update stops after exactly limit.limit() MATCHED rows — capping the left
        // scan would under-update (fewer than n of the first n left rows may join a source row).
        auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                    context.log.clone(),
                                                                                    table_oid,
                                                                                    node_update->updates(),
                                                                                    std::move(returning),
                                                                                    node_match->expressions()[0],
                                                                                    limit.limit()));
        plan->set_table_has_indexes(node->table_has_indexes());
        VALUE_OR_RETURN(
            auto source_op,
            create_plan(context, function_registry, node_source, components::logical_plan::limit_t::unlimit(), params));
        plan->set_children(
            boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                      context.log.clone(),
                                                                      table_oid,
                                                                      nullptr,
                                                                      components::logical_plan::limit_t::unlimit())),
            std::move(source_op));
        return plan;
    }

} // namespace services::planner::impl

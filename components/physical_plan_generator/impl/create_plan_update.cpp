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
        const auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        // No Limit operator runs over a DML root, so the count is refused here, before any row is touched.
        if (auto invalid = limit.validate(context.resource); invalid.contains_error()) {
            return invalid;
        }
        const auto cap = limit.read_cap();
        auto table_oid = node->table_oid();
        // The update target is always a NAMED table; a target the context cannot vouch
        // for is a table that never resolved. Validation refuses this before plan
        // generation; if that refusal is ever lost again, lowering anyway builds a sink
        // with no table behind it — an UPDATE that changes nothing and reports SUCCESS.
        if (!context.has_table_oid(table_oid)) {
            return unresolved_table_refusal(context.resource, node_update->target());
        }
        VALUE_OR_RETURN(auto address, storage_address_of(context, table_oid));
        auto name = full_name_of(context, table_oid);
        if (!node_update->excluded_alias().empty()) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(address),
                                                                                        std::move(name),
                                                                                        node_update->updates(),
                                                                                        std::move(returning),
                                                                                        node_match->expressions()[0]));
            plan->set_table_has_indexes(node->table_has_indexes());
            if (!node_update->returning().empty()) {
                VALUE_OR_RETURN(auto table_types, page_types_of(context, table_oid, {}));
                plan->set_table_types(std::move(table_types));
            }
            plan->set_paired_from(true);
            return plan;
        }
        if (!node_source) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(address),
                                                                                        std::move(name),
                                                                                        node_update->updates(),
                                                                                        std::move(returning)));
            plan->set_table_has_indexes(node->table_has_indexes());
            if (!node_update->returning().empty()) {
                VALUE_OR_RETURN(auto table_types, page_types_of(context, table_oid, {}));
                plan->set_table_types(std::move(table_types));
            }
            VALUE_OR_RETURN(auto scan, create_plan_match(context, node_match, cap));
            plan->set_children(std::move(scan));

            return plan;
        }
        // Source (UPDATE ... FROM) path: the semi-join reads ALL left rows (uncapped) and
        // operator_update stops after exactly the capped count of MATCHED rows — capping the left
        // scan would under-update (fewer than n of the first n left rows may join a source row).
        auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                    context.log.clone(),
                                                                                    table_oid,
                                                                                    address,
                                                                                    name,
                                                                                    node_update->updates(),
                                                                                    std::move(returning),
                                                                                    node_match->expressions()[0],
                                                                                    cap));
        plan->set_table_has_indexes(node->table_has_indexes());
        if (!node_update->returning().empty()) {
            VALUE_OR_RETURN(auto table_types, page_types_of(context, table_oid, {}));
            plan->set_table_types(std::move(table_types));
        }
        VALUE_OR_RETURN(
            auto source_op,
            create_plan(context, function_registry, node_source, components::logical_plan::limit_t::all(), params));
        auto scan_columns = scan_columns_of(context, table_oid, {});
        VALUE_OR_RETURN(auto scan_types, page_types_of(context, table_oid, scan_columns));
        auto scan =
            boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                      context.log.clone(),
                                                                      table_oid,
                                                                      std::move(address),
                                                                      std::move(name),
                                                                      nullptr,
                                                                      components::logical_plan::row_cap_t::uncapped(),
                                                                      std::move(scan_columns)));
        scan->set_page_types(std::move(scan_types));
        plan->set_children(std::move(scan), std::move(source_op));
        return plan;
    }

} // namespace services::planner::impl

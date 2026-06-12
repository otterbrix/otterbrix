#include "create_plan_delete.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator_delete.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>

#include "create_plan_data.hpp"

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_delete(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           const components::logical_plan::storage_parameters* params) {
        const auto* node_delete = static_cast<const components::logical_plan::node_delete_t*>(node.get());
        auto returning = build_returning_columns(context.resource, node_delete->returning(), params);

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_delete->children()) {
            if (child->type() == components::logical_plan::node_type::match_t) {
                node_match = child;
            } else if (child->type() == components::logical_plan::node_type::limit_t) {
                node_limit = child;
            } else if (child->type() == components::logical_plan::node_type::data_t) {
                node_raw_data = child;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        auto table_oid = node->table_oid();
        // Target the simple (no USING) path when neither a USING-side
        // table_oid nor raw data join input is present.
        if (node_delete->table_oid_from() == components::catalog::INVALID_OID && !node_raw_data) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(returning)));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            auto expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr*>(&node_match->expressions()[0]);

            auto plan = boost::intrusive_ptr(new components::operators::operator_delete(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        std::move(returning),
                                                                                        *expr));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             table_oid,
                                                                                             nullptr,
                                                                                             limit)),
                                   create_plan_data(node_raw_data));
            } else {
                // Read the USING-side table_oid from the node
                // (enrich_logical_plan stamps it via the same plan-tree
                // resolve path as the primary table). INVALID_OID would
                // scan an empty storage and silently drop the join condition.
                const auto using_oid = node_delete->table_oid_from();
                plan->set_children(boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             table_oid,
                                                                                             nullptr,
                                                                                             limit)),
                                   boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                                             context.log.clone(),
                                                                                             using_oid,
                                                                                             nullptr,
                                                                                             limit)));
            }

            return plan;
        }
    }

} // namespace services::planner::impl

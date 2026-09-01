#include "create_plan_update.hpp"
#include "create_plan_match.hpp"
#include "create_plan_select.hpp"
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/operators/operator_update.hpp>
#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_update(const context_storage_t& context,
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
        auto table_oid = node->table_oid();
        if (!node_source) {
            auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                        context.log.clone(),
                                                                                        table_oid,
                                                                                        node_update->updates(),
                                                                                        node_update->upsert(),
                                                                                        std::move(returning)));
            plan->set_table_has_indexes(node->table_has_indexes());
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        }
        // Source (UPDATE ... FROM) path: the semi-join reads ALL left rows (unlimit) and
        // operator_update stops after exactly limit.limit() MATCHED rows — capping the left
        // scan would under-update (fewer than n of the first n left rows may join a source row).
        auto plan = boost::intrusive_ptr(new components::operators::operator_update(context.resource,
                                                                                    context.log.clone(),
                                                                                    table_oid,
                                                                                    node_update->updates(),
                                                                                    node_update->upsert(),
                                                                                    std::move(returning),
                                                                                    node_match->expressions()[0],
                                                                                    limit.limit()));
        plan->set_table_has_indexes(node->table_has_indexes());
        plan->set_children(
            boost::intrusive_ptr(new components::operators::full_scan(context.resource,
                                                                      context.log.clone(),
                                                                      table_oid,
                                                                      nullptr,
                                                                      components::logical_plan::limit_t::unlimit())),
            create_plan(context, function_registry, node_source, components::logical_plan::limit_t::unlimit(), params));
        return plan;
    }

} // namespace services::planner::impl

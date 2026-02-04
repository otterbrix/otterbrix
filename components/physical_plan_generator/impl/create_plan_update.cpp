#include "create_plan_update.hpp"
#include "create_plan_match.hpp"
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/physical_plan/table/operators/operator_update.hpp>
#include <components/physical_plan/table/operators/scan/full_scan.hpp>

#include "create_plan_data.hpp"

namespace services::table::planner::impl {

    components::base::operators::operator_ptr create_plan_update(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node) {
        const auto* node_update = static_cast<const components::logical_plan::node_update_t*>(node.get());

        components::logical_plan::node_ptr node_match = nullptr;
        components::logical_plan::node_ptr node_limit = nullptr;
        components::logical_plan::node_ptr node_raw_data = nullptr;
        for (auto child : node_update->children()) {
            if (child->type() == components::logical_plan::node_type::match_t) {
                node_match = child;
            } else if (child->type() == components::logical_plan::node_type::limit_t) {
                node_limit = child;
            } else if (child->type() == components::logical_plan::node_type::data_t) {
                node_raw_data = child;
            }
        }
        auto limit = static_cast<components::logical_plan::node_limit_t*>(node_limit.get())->limit();
        if (node_update->collection_from().empty() && !node_raw_data) {
            auto plan = boost::intrusive_ptr(
                new components::table::operators::operator_update(context.at(node->collection_full_name()),
                                                                  node_update->updates(),
                                                                  node_update->upsert()));
            plan->set_children(create_plan_match(context, node_match, limit));

            return plan;
        } else {
            auto& expr =
                reinterpret_cast<const components::expressions::compare_expression_ptr&>(node_match->expressions()[0]);

            auto plan = boost::intrusive_ptr(
                new components::table::operators::operator_update(context.at(node->collection_full_name()),
                                                                  node_update->updates(),
                                                                  node_update->upsert(),
                                                                  expr));
            if (node_raw_data) {
                plan->set_children(boost::intrusive_ptr(new components::table::operators::full_scan(
                                       context.at(node->collection_full_name()),
                                       nullptr,
                                       limit)),
                                   create_plan_data(node_raw_data));
            } else {
                plan->set_children(boost::intrusive_ptr(new components::table::operators::full_scan(
                                       context.at(node->collection_full_name()),
                                       nullptr,
                                       limit)),
                                   boost::intrusive_ptr(new components::table::operators::full_scan(
                                       context.at(node_update->collection_from()),
                                       nullptr,
                                       limit)));
            }

            return plan;
        }
    }

} // namespace services::table::planner::impl
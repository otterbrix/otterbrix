#include "create_plan_insert.hpp"

#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_insert(const context_storage_t& context,
                                                           const components::compute::function_registry_t& function_registry,
                                                           const components::logical_plan::node_ptr& node,
                                                           components::logical_plan::limit_t limit) {
        // TODO: figure out key translation
        auto plan = boost::intrusive_ptr(
            //new components::operators::operator_insert(context.at(node->collection_full_name()),
            //                                                       insert->key_translation()));
            new components::operators::operator_insert(context.at(node->collection_full_name())));
        plan->set_children(create_plan(context, function_registry, node->children().front(), std::move(limit)));

        return plan;
    }

} // namespace services::planner::impl
#include "create_plan_sequence.hpp"

#include <components/logical_plan/node_sequence.hpp>
#include <components/physical_plan/operators/operator_sequence.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_sequence(const context_storage_t& context,
                         const components::compute::function_registry_t& function_registry,
                         const components::logical_plan::node_ptr& node,
                         const components::logical_plan::storage_parameters* params) {
        std::vector<components::operators::operator_ptr> steps;
        steps.reserve(node->children().size());
        for (const auto& child : node->children()) {
            steps.push_back(create_plan(context, function_registry, child, {}, params));
        }
        return boost::intrusive_ptr(new components::operators::operator_sequence_t(
            context.resource,
            context.log.clone(),
            std::move(steps)));
    }

} // namespace services::planner::impl
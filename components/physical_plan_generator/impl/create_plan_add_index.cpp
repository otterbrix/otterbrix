#include "create_plan_add_index.hpp"
#include <components/logical_plan/node_create_index.hpp>
#include <components/physical_plan/operators/operator_add_index.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_add_index(const context_storage_t& context,
                                                              const components::logical_plan::node_ptr& node) {
        auto* node_create_index = static_cast<components::logical_plan::node_create_index_t*>(node.get());
        auto plan = boost::intrusive_ptr(
            new components::operators::operator_add_index(context.at(node->collection_full_name()), node_create_index));

        return plan;
    }

} // namespace services::planner::impl
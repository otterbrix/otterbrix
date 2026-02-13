#include "create_plan_data.hpp"
#include <components/logical_plan/node_data.hpp>
#include <components/physical_plan/operators/operator_raw_data.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_data(const components::logical_plan::node_ptr& node) {
        const auto* data = static_cast<const components::logical_plan::node_data_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_raw_data_t(data->data_chunk()));
    }

} // namespace services::planner::impl

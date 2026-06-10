#include "create_plan_set_timezone.hpp"

#include <components/logical_plan/node_set_timezone.hpp>
#include <components/physical_plan/operators/operator_set_timezone.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_set_timezone(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node) {
        auto* tz_node = static_cast<components::logical_plan::node_set_timezone_t*>(node.get());
        std::pmr::string tz_pmr{tz_node->timezone_name().c_str(), tz_node->timezone_name().size(), context.resource};
        return boost::intrusive_ptr(new components::operators::operator_set_timezone_t(context.resource,
                                                                                       context.log.clone(),
                                                                                       std::move(tz_pmr)));
    }

} // namespace services::planner::impl
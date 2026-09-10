#include "create_plan_set_setting.hpp"

#include <components/logical_plan/node_set_setting.hpp>
#include <components/physical_plan/operators/operator_set_setting.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_set_setting(const context_storage_t& context,
                                                                const components::logical_plan::node_ptr& node) {
        auto* setting_node = static_cast<components::logical_plan::node_set_setting_t*>(node.get());
        std::pmr::string value{setting_node->value().c_str(), setting_node->value().size(), context.resource};
        return boost::intrusive_ptr(new components::operators::operator_set_setting_t(context.resource,
                                                                                      context.log.clone(),
                                                                                      setting_node->setting(),
                                                                                      std::move(value)));
    }

} // namespace services::planner::impl

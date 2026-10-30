#include "create_plan_cursor.hpp"

#include <components/logical_plan/node_cursor.hpp>
#include <components/physical_plan/operators/operator_portal.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_fetch(const context_storage_t& context,
                                                          const components::logical_plan::node_ptr& node) {
        const auto* fetch = static_cast<const components::logical_plan::node_fetch_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_portal_fetch_t(context.resource,
                                                                                       context.log.clone(),
                                                                                       fetch->portal_id(),
                                                                                       fetch->name(),
                                                                                       fetch->count(),
                                                                                       fetch->is_move()));
    }

    components::operators::operator_ptr create_plan_close_cursor(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node) {
        const auto* close = static_cast<const components::logical_plan::node_close_cursor_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_portal_close_t(context.resource,
                                                                                       context.log.clone(),
                                                                                       close->portal_id(),
                                                                                       close->name()));
    }

} // namespace services::planner::impl

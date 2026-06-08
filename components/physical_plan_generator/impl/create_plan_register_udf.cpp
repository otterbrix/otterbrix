#include "create_plan_register_udf.hpp"

#include <components/logical_plan/node_register_udf.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>

#include <utility>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_register_udf(const context_storage_t& context,
                             const components::logical_plan::node_ptr& node,
                             components::operators::operator_register_udf_t::executor_uids_t executor_uids) {
        auto* n = static_cast<components::logical_plan::node_register_udf_t*>(node.get());
        const auto* fn = n->function();
        // The node retains ownership of the canonical payload; the operator gets
        // an independent deep copy so it can read name()/signatures and mirror it
        // into the default registry without consuming the node's instance.
        components::compute::function_ptr fn_copy = fn ? fn->get_copy(context.resource) : nullptr;
        return boost::intrusive_ptr(new components::operators::operator_register_udf_t(context.resource,
                                                                                       context.log.clone(),
                                                                                       std::move(fn_copy),
                                                                                       std::move(executor_uids)));
    }

} // namespace services::planner::impl

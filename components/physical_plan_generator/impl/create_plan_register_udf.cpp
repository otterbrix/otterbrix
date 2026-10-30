#include "create_plan_register_udf.hpp"

#include <components/logical_plan/node_register_udf.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>

#include <utility>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_register_udf(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_register_udf_t*>(node.get());
        const auto* fn = n->function();
        components::compute::function_ptr fn_copy =
            fn ? fn->get_copy(context.resource) : components::compute::no_function();
        return boost::intrusive_ptr(new components::operators::operator_register_udf_t(context.resource,
                                                                                       context.log.clone(),
                                                                                       std::move(fn_copy),
                                                                                       n->uid(),
                                                                                       n->schema_oid()));
    }

} // namespace services::planner::impl

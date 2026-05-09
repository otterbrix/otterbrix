#include "create_plan_register_udf.hpp"

#include <components/logical_plan/node_register_udf.hpp>
#include <components/physical_plan/operators/operator_register_udf.hpp>

#include <utility>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_register_udf(const context_storage_t& context,
                              const components::logical_plan::node_ptr& node,
                              std::size_t executor_count,
                              components::operators::operator_register_udf_t::executor_register_fn_t fanout) {
        auto* n = static_cast<components::logical_plan::node_register_udf_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_register_udf_t(
            context.resource,
            context.log.clone(),
            n->function(),
            executor_count,
            std::move(fanout)));
    }

} // namespace services::planner::impl

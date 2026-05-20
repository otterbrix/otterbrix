#include "create_plan_unregister_udf.hpp"

#include <components/logical_plan/node_unregister_udf.hpp>
#include <components/physical_plan/operators/operator_unregister_udf.hpp>

#include <utility>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_unregister_udf(const context_storage_t& context,
                                                                   const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_unregister_udf_t*>(node.get());

        // Re-pack inputs into a fresh pmr-vector backed by the planner's
        // resource so the operator owns its lifetime.
        std::pmr::vector<components::types::complex_logical_type> inputs(context.resource);
        inputs.reserve(n->inputs().size());
        for (const auto& t : n->inputs()) {
            inputs.push_back(t);
        }

        return boost::intrusive_ptr(new components::operators::operator_unregister_udf_t(context.resource,
                                                                                         context.log.clone(),
                                                                                         n->function_name(),
                                                                                         std::move(inputs)));
    }

} // namespace services::planner::impl

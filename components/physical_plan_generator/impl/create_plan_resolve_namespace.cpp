#include "create_plan_resolve_namespace.hpp"

#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/physical_plan/operators/operator_resolve_namespace.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_namespace(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_namespace_t*>(node.get());
        // Pass the back-pointer so the operator stamps the resolved
        // namespace_oid onto the logical node after a successful
        // pg_namespace scan. plan_resolve_index_t reads it in Pass 2.
        return boost::intrusive_ptr(new components::operators::operator_resolve_namespace_t(context.resource,
                                                                                            context.log.clone(),
                                                                                            n->dbname(),
                                                                                            n));
    }

} // namespace services::planner::impl

#include "create_plan_resolve_function.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_catalog_resolve_function.hpp>
#include <components/physical_plan/operators/operator_resolve_function.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_function(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_function_t*>(node.get());
        // namespace_oid is not yet carried by the logical node (dbname is
        // captured instead and resolved upstream). Pass INVALID_OID as a
        // placeholder so the operator falls back to its empty-match path
        // until namespace resolution is wired through.
        return boost::intrusive_ptr(
            new components::operators::operator_resolve_function_t(context.resource,
                                                                   context.log.clone(),
                                                                   components::catalog::INVALID_OID,
                                                                   n->function_name()));
    }

} // namespace services::planner::impl

#include "create_plan_resolve_type.hpp"

#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/physical_plan/operators/operator_resolve_type.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_type(const context_storage_t& context,
                                                                 const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_type_t*>(node.get());
        // Pass the back-pointer so the operator resolves namespace_oid
        // from dbname ("public" / "pg_catalog" via well-known constants;
        // arbitrary names via pg_namespace scan) and stamps
        // resolved_metadata onto the logical node. plan_resolve_index_t
        // gathers it via type_md_by_qname for downstream consumers.
        return boost::intrusive_ptr(new components::operators::operator_resolve_type_t(context.resource,
                                                                                       context.log.clone(),
                                                                                       n->dbname(),
                                                                                       n->type_name(),
                                                                                       n));
    }

} // namespace services::planner::impl
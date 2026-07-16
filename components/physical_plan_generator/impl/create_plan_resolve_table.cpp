#include "create_plan_resolve_table.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/physical_plan/operators/operator_resolve_table.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_table(const context_storage_t& context,
                                                                  const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_t*>(node.get());
        // Pass the back-pointer so the operator stamps namespace_oid +
        // table_oid onto the logical node after a successful pg_class scan.
        // plan_resolve_index_t reads them in Pass 2.
        //
        // dbname is passed as a STRING: at plan-generation time the node's
        // namespace_oid() is still INVALID_OID (the sibling resolve_namespace
        // operator stamps its own node only at execution time), so the
        // operator translates dbname -> namespace_oid itself at execution
        // time. Dropping the dbname here was the #557 cross-database leak.
        return boost::intrusive_ptr(new components::operators::operator_resolve_table_t(context.resource,
                                                                                        context.log.clone(),
                                                                                        n->namespace_oid(),
                                                                                        n->dbname(),
                                                                                        n->relname(),
                                                                                        n));
    }

} // namespace services::planner::impl

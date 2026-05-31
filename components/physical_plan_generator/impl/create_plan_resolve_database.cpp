#include "create_plan_resolve_database.hpp"

#include <components/logical_plan/node_catalog_resolve_database.hpp>
#include <components/physical_plan/operators/operator_resolve_database.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_resolve_database(const context_storage_t& context,
                                                                     const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_database_t*>(node.get());
        // Back-pointer form: operator stamps database_oid onto the logical
        // node after a successful pg_database scan; enrich_logical_plan reads
        // it via stamp_oids_from_resolves() in the enrich pass.
        return boost::intrusive_ptr(new components::operators::operator_resolve_database_t(context.resource,
                                                                                           context.log.clone(),
                                                                                           n->dbname(),
                                                                                           n));
    }

} // namespace services::planner::impl

#include "create_plan_resolve_table.hpp"

#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/physical_plan/operators/operator_resolve_table.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_resolve_table(const context_storage_t& context,
                              const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_catalog_resolve_table_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_resolve_table_t(
            context.resource,
            context.log.clone(),
            n->namespace_oid(),
            n->relname()));
    }

} // namespace services::planner::impl

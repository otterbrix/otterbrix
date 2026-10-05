#include "create_plan_register_cast.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/logical_plan/node_register_cast.hpp>
#include <components/physical_plan/operators/operator_register_cast.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_register_cast(const context_storage_t& context,
                                                                  const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_register_cast_t*>(node.get());
        const auto source_oid = components::catalog::builtin_type_to_oid(n->source().type());
        const auto target_oid = components::catalog::builtin_type_to_oid(n->target().type());
        return boost::intrusive_ptr(new components::operators::operator_register_cast_t(context.resource,
                                                                                        context.log.clone(),
                                                                                        source_oid,
                                                                                        target_oid));
    }

    components::operators::operator_ptr create_plan_unregister_cast(const context_storage_t& context,
                                                                    const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_unregister_cast_t*>(node.get());
        const auto source_oid = components::catalog::builtin_type_to_oid(n->source().type());
        const auto target_oid = components::catalog::builtin_type_to_oid(n->target().type());
        return boost::intrusive_ptr(new components::operators::operator_unregister_cast_t(context.resource,
                                                                                          context.log.clone(),
                                                                                          source_oid,
                                                                                          target_oid));
    }

} // namespace services::planner::impl
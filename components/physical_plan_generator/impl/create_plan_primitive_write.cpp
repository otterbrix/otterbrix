#include "create_plan_primitive_write.hpp"

#include <components/logical_plan/node_primitive_write.hpp>
#include <components/physical_plan/operators/operator_primitive_write.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_primitive_write(const context_storage_t& context,
                                                                    const components::logical_plan::node_ptr& node) {
        auto* n = static_cast<components::logical_plan::node_primitive_write_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_primitive_write_t(context.resource,
                                                                                          context.log.clone(),
                                                                                          n->catalog_table_oid(),
                                                                                          std::move(n->row())));
    }

} // namespace services::planner::impl
#include "create_plan_begin_transaction.hpp"

#include <components/physical_plan/operators/operator_begin_transaction.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_begin_transaction(const context_storage_t& context,
                                  const components::logical_plan::node_ptr& /*node*/) {
        return boost::intrusive_ptr(
            new components::operators::operator_begin_transaction_t(context.resource, context.log.clone()));
    }

} // namespace services::planner::impl

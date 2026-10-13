#include "create_plan_savepoint.hpp"

#include <components/logical_plan/node_transaction.hpp>
#include <components/physical_plan/operators/operator_release_savepoint.hpp>
#include <components/physical_plan/operators/operator_rollback_to_savepoint.hpp>
#include <components/physical_plan/operators/operator_savepoint.hpp>

namespace services::planner::impl {

    namespace {
        std::string_view savepoint_name(const components::logical_plan::node_ptr& node) {
            return static_cast<const components::logical_plan::node_transaction_t*>(node.get())->savepoint_name();
        }
    } // namespace

    components::operators::operator_ptr create_plan_savepoint(const context_storage_t& context,
                                                              const components::logical_plan::node_ptr& node) {
        return boost::intrusive_ptr(new components::operators::operator_savepoint_t(context.resource,
                                                                                    context.log.clone(),
                                                                                    savepoint_name(node)));
    }

    components::operators::operator_ptr create_plan_release_savepoint(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node) {
        return boost::intrusive_ptr(new components::operators::operator_release_savepoint_t(context.resource,
                                                                                            context.log.clone(),
                                                                                            savepoint_name(node)));
    }

    components::operators::operator_ptr
    create_plan_rollback_to_savepoint(const context_storage_t& context,
                                      const components::logical_plan::node_ptr& node) {
        return boost::intrusive_ptr(new components::operators::operator_rollback_to_savepoint_t(context.resource,
                                                                                                context.log.clone(),
                                                                                                savepoint_name(node)));
    }

} // namespace services::planner::impl

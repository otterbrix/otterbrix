#include "create_plan_prepared_statement.hpp"

#include <components/logical_plan/node_deallocate.hpp>
#include <components/logical_plan/node_prepare.hpp>
#include <components/physical_plan/operators/operator_prepared_statement.hpp>

namespace services::planner::impl {

    using components::logical_plan::prepared_change_t;

    components::operators::operator_ptr create_plan_prepare(const context_storage_t& context,
                                                            const components::logical_plan::node_ptr& node) {
        const auto* prepare = static_cast<const components::logical_plan::node_prepare_t*>(node.get());
        return boost::intrusive_ptr(new components::operators::operator_prepared_statement_t(
            context.resource,
            context.log.clone(),
            prepared_change_t{prepared_change_t::kind_t::store, prepare->name(), prepare->statement()},
            prepare->exists()));
    }

    components::operators::operator_ptr create_plan_deallocate(const context_storage_t& context,
                                                               const components::logical_plan::node_ptr& node) {
        const auto* deallocate = static_cast<const components::logical_plan::node_deallocate_t*>(node.get());
        const auto kind = deallocate->all() ? prepared_change_t::kind_t::drop_all : prepared_change_t::kind_t::drop;
        return boost::intrusive_ptr(
            new components::operators::operator_prepared_statement_t(context.resource,
                                                                     context.log.clone(),
                                                                     prepared_change_t{kind, deallocate->name(), {}},
                                                                     deallocate->exists()));
    }

} // namespace services::planner::impl

#include "create_plan_insert_on_conflict.hpp"

#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator_insert_on_conflict.hpp>
#include <components/physical_plan_generator/create_plan.hpp>

namespace services::planner::impl {

    components::operators::operator_ptr
    create_plan_insert_on_conflict(const context_storage_t& context,
                                   const components::compute::function_registry_t& function_registry,
                                   const components::logical_plan::node_ptr& node,
                                   const components::logical_plan::storage_parameters* params) {
        namespace ops = components::operators;
        using components::logical_plan::node_type;

        const components::logical_plan::node_t* insert_node = node->children().front().get();
        while (insert_node->type() != node_type::insert_t) {
            insert_node = insert_node->children().front().get();
        }
        const auto* insert = static_cast<const components::logical_plan::node_insert_t*>(insert_node);

        auto insert_part = create_plan(context,
                                       function_registry,
                                       node->children().front(),
                                       components::logical_plan::limit_t::unlimit(),
                                       params);
        if (!insert_part) {
            return nullptr;
        }
        ops::operator_unique_constraint_t* check = nullptr;
        for (ops::operator_t* op = insert_part.get(); op != nullptr; op = op->left().get()) {
            if (op->type() == ops::operator_type::unique_constraint) {
                check = static_cast<ops::operator_unique_constraint_t*>(op);
                break;
            }
        }

        auto removal =
            boost::intrusive_ptr(new ops::operator_delete(context.resource,
                                                          context.log.clone(),
                                                          insert->table_oid(),
                                                          std::pmr::vector<ops::projected_column_t>{context.resource}));
        removal->set_table_has_indexes(insert->table_has_indexes());

        auto plan = boost::intrusive_ptr(new ops::operator_insert_on_conflict_t(context.resource,
                                                                                context.log.clone(),
                                                                                insert->table_oid(),
                                                                                !insert->returning().empty()));
        plan->set_parts(std::move(insert_part), check, std::move(removal));

        if (node->children().size() > 1) {
            auto update_part = create_plan(context,
                                           function_registry,
                                           node->children()[1],
                                           components::logical_plan::limit_t::unlimit(),
                                           params);
            if (!update_part) {
                return nullptr;
            }
            ops::operator_update* update = nullptr;
            for (ops::operator_t* op = update_part.get(); op != nullptr; op = op->left().get()) {
                if (op->type() == ops::operator_type::update) {
                    update = static_cast<ops::operator_update*>(op);
                    break;
                }
            }
            plan->set_update(std::move(update_part), update);
        }
        return plan;
    }

} // namespace services::planner::impl

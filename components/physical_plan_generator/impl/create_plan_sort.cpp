#include "create_plan_sort.hpp"

#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/physical_plan/operators/operator_external_sort.hpp>
#include <components/physical_plan/operators/operator_sort.hpp>
#include <components/physical_plan/operators/sort/sort.hpp>

namespace {

    // Configure either standalone sort operator (operator_sort_t / operator_external_sort_t).
    // They share the public add/add_computed/set_limit API but no longer share a base,
    // so this templates over the concrete type. Returns nullptr on
    // an unresolved sort-key path (executor surfaces it; rule 9: no throw on build path).
    template<class Op>
    components::operators::operator_ptr configure_sort_op(boost::intrusive_ptr<Op> sort,
                                                          const components::logical_plan::node_ptr& node,
                                                          std::pmr::memory_resource* plan_resource,
                                                          components::logical_plan::limit_t limit) {
        for (const auto& expr : node->expressions()) {
            if (expr->group() == components::expressions::expression_group::sort) {
                const auto* sort_expr = static_cast<components::expressions::sort_expression_t*>(expr.get());
                const auto& path = sort_expr->key().path();
                if (path.empty()) {
                    return nullptr;
                }
                sort->add(path, components::sort::order(sort_expr->order()));
            } else if (expr->group() == components::expressions::expression_group::scalar) {
                const auto* scalar_expr =
                    static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                components::operators::computed_sort_key_t ck(plan_resource);
                ck.op = scalar_expr->type();
                ck.operands = scalar_expr->params();
                bool is_desc = !scalar_expr->key().path().empty() && scalar_expr->key().path()[0] == size_t(1);
                ck.order_ = is_desc ? components::sort::order::descending : components::sort::order::ascending;
                sort->add_computed(std::move(ck));
            }
        }
        sort->set_limit(limit);
        return sort;
    }

} // namespace

namespace services::planner::impl {

    components::operators::operator_ptr create_plan_sort(const context_storage_t& context,
                                                         const components::logical_plan::node_ptr& node,
                                                         components::logical_plan::limit_t limit) {
        using exec_strategy = components::logical_plan::node_sort_t::exec_strategy;

        auto table_oid = node->table_oid();
        bool known = context.has_table_oid(table_oid);
        auto plan_resource = known ? context.resource : node->resource();

        // R3 / R6: lower purely on the optimizer's spill annotation. The
        // operator itself never checks memory at runtime (no fallback). Default
        // in_memory keeps the pre-spill behaviour when the optimizer did not stamp.
        // (See node_sort.hpp::exec_strategy for the annotation contract.)
        // Guarded cast: default to in_memory unless the node really is a sort
        // node, instead of an unchecked static_cast deref. The type tag is checked
        // via node->type() (rule 14: no dynamic_cast) before the static_cast.
        exec_strategy strategy = exec_strategy::in_memory;
        if (node->type() == components::logical_plan::node_type::sort_t) {
            strategy = static_cast<const components::logical_plan::node_sort_t*>(node.get())->strategy();
        }
        if (strategy == exec_strategy::spill) {
            auto op = known ? boost::intrusive_ptr(new components::operators::operator_external_sort_t(
                                  context.resource, context.log.clone()))
                            : boost::intrusive_ptr(new components::operators::operator_external_sort_t(
                                  node->resource(), log_t{}));
            return configure_sort_op(std::move(op), node, plan_resource, limit);
        }
        auto op = known ? boost::intrusive_ptr(
                              new components::operators::operator_sort_t(context.resource, context.log.clone()))
                        : boost::intrusive_ptr(
                              new components::operators::operator_sort_t(node->resource(), log_t{}));
        return configure_sort_op(std::move(op), node, plan_resource, limit);
    }

} // namespace services::planner::impl

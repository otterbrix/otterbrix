#include "create_plan_group.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/clone_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_group.hpp>

#include <components/physical_plan/operators/operator_hash_group.hpp>

namespace services::planner::impl {

    namespace {

        using components::expressions::expression_group;
        using components::expressions::scalar_type;

        // Registers every REDUCTION an output expression contains. The expression itself is handed
        // to the group UNCHANGED: an aggregate is a node the graph builds like any other, so
        // SUM(x) * 2 needs no rewriting — the aggregate node folds the group's rows and the
        // multiply reads its single value. What the OPERATOR still has to know is merely that a
        // reduction exists, because that is what makes an empty input emit its one row.
        struct reduction_registrar {
            boost::intrusive_ptr<components::operators::operator_hash_group_t>& group;

            void visit(const components::expressions::param_storage& param) const {
                if (!components::expressions::is_expr(param)) {
                    return;
                }
                const auto& child = components::expressions::as_expr(param);
                if (child) {
                    visit_in(child);
                }
            }

            void visit_in(const components::expressions::expression_ptr& expr) const {
                switch (expr->group()) {
                    case expression_group::aggregate: {
                        const auto* aggregate =
                            static_cast<const components::expressions::aggregate_expression_t*>(expr.get());
                        group->add_value(aggregate->key().as_pmr_string(), aggregate->result_type());
                        // An aggregate's ARGUMENTS are row-cardinality by validation, so they can
                        // hold no further reduction — sum(sum(x)) is rejected before here.
                        break;
                    }
                    case expression_group::scalar:
                        for (const auto& param :
                             static_cast<const components::expressions::scalar_expression_t*>(expr.get())->params()) {
                            visit(param);
                        }
                        break;
                    case expression_group::function:
                        for (const auto& argument :
                             static_cast<const components::expressions::function_expression_t*>(expr.get())->args()) {
                            visit(argument);
                        }
                        break;
                    case expression_group::cast:
                        visit(static_cast<const components::expressions::cast_expression_t*>(expr.get())->child());
                        break;
                    case expression_group::compare: {
                        const auto* comparison =
                            static_cast<const components::expressions::compare_expression_t*>(expr.get());
                        visit(comparison->left());
                        visit(comparison->right());
                        for (const auto& nested : comparison->children()) {
                            visit_in(nested);
                        }
                        break;
                    }
                    default:
                        break;
                }
            }
        };

    } // namespace

    components::operators::operator_ptr create_plan_group(const context_storage_t& context,
                                                          const components::compute::function_registry_t&,
                                                          const components::logical_plan::node_ptr& node,
                                                          const components::logical_plan::storage_parameters*) {
        boost::intrusive_ptr<components::operators::operator_hash_group_t> group;
        auto table_oid = node->table_oid();
        bool known = context.has_table_oid(table_oid);

        // create_plan_group is only ever dispatched with a group_t node (create_plan.cpp's
        // case group_t and the aggregate's group child), so the static_cast is safe.
        const auto* group_node = static_cast<const components::logical_plan::node_group_t*>(node.get());

        if (known) {
            group = new components::operators::operator_hash_group_t(context.resource, context.log.clone());
        } else {
            group = new components::operators::operator_hash_group_t(node->resource(), log_t{});
        }

        // Build group operator from node expressions
        auto plan_resource = known ? context.resource : node->resource();

        // The schema the group reduces over, resolved by validation against the same incoming
        // schema its expressions were resolved against.
        group->set_input_types(group_node->input_types());

        // Aggregates the validator marked internal (HAVING helpers) are reduced but never
        // emitted: they sit at the tail of the expression list and get no output entry.
        const size_t select_end = node->expressions().size() - group_node->internal_aggregate_count;

        // Pass 1 — the grouping KEYS, and only those. A key comes from GROUP BY (a group_field) and
        // decides what a group IS; it is not an output column. Everything else in the list is a
        // projected column the graph evaluates, so nothing else may become a key here.
        for (const auto& expr : node->expressions()) {
            if (expr->group() != expression_group::scalar) {
                continue;
            }
            const auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
            if (scalar_expr->type() != scalar_type::group_field) {
                continue;
            }
            const auto& path = scalar_expr->key().path();
            components::operators::group_key_t key(plan_resource);
            key.name = std::pmr::string(scalar_expr->key().storage().back(), plan_resource);
            key.type = components::operators::group_key_t::kind::column;
            key.full_path = path;
            group->add_key(std::move(key));
        }

        // Pass 2 — the output list, in target-list order.
        for (size_t i = 0; i < node->expressions().size(); i++) {
            const auto& expr = node->expressions()[i];
            if (expr->group() == expression_group::scalar &&
                static_cast<const components::expressions::scalar_expression_t*>(expr.get())->type() ==
                    scalar_type::group_field) {
                continue; // the key list, not a projected column
            }
            if (i >= select_end) {
                continue; // a HAVING helper the projection above strips
            }
            // Everything else is evaluated by the group's graph over the group's own input rows:
            // aggregates included, since a reduction is a node like any other. The registrar only
            // records that the reductions are there; it rewrites nothing.
            reduction_registrar{group}.visit_in(expr);
            group->add_output(expr);
        }

        // A HAVING helper is reduced but never emitted, so it contributes no output — but it IS a
        // reduction, and the group has to perform it for operator_having to read.
        for (size_t i = select_end; i < node->expressions().size(); i++) {
            reduction_registrar{group}.visit_in(node->expressions()[i]);
        }

        return group;
    }

} // namespace services::planner::impl

#pragma once

#include "node.hpp"
#include "node_check_constraint.hpp"
#include "node_create_constraint.hpp"
#include "node_delete.hpp"
#include "node_function.hpp"
#include "node_insert.hpp"
#include "node_insert_on_conflict.hpp"
#include "node_update.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>

#include <memory_resource>
#include <type_traits>
#include <vector>

namespace components::expressions {

    // Every function call in `expr`, an aggregate's own call included, the outer call first.
    template<typename Visit>
    void for_each_call(const expression_ptr& expr, Visit& visit) {
        if (!expr) {
            return;
        }
        const auto walk_param = [&visit](const param_storage& param) {
            if (is_expr(param)) {
                for_each_call(as_expr(param), visit);
            }
        };
        switch (expr->group()) {
            case expression_group::compare: {
                auto* e = static_cast<compare_expression_t*>(expr.get());
                walk_param(e->left());
                walk_param(e->right());
                for (const auto& child : e->children()) {
                    for_each_call(child, visit);
                }
                break;
            }
            case expression_group::scalar: {
                auto* e = static_cast<scalar_expression_t*>(expr.get());
                for (const auto& param : e->params()) {
                    walk_param(param);
                }
                break;
            }
            case expression_group::aggregate: {
                auto* e = static_cast<aggregate_expression_t*>(expr.get());
                for (const auto& param : e->params()) {
                    walk_param(param);
                }
                for_each_call(e->child(), visit);
                break;
            }
            case expression_group::function: {
                auto* e = static_cast<function_expression_t*>(expr.get());
                visit(*e);
                for (const auto& arg : e->args()) {
                    walk_param(arg);
                }
                break;
            }
            case expression_group::cast: {
                walk_param(static_cast<cast_expression_t*>(expr.get())->child());
                break;
            }
            case expression_group::sort: {
                walk_param(static_cast<sort_expression_t*>(expr.get())->operand());
                break;
            }
            case expression_group::invalid:
                break;
        }
    }

} // namespace components::expressions

namespace components::logical_plan {

    // `node` as its own kind, as const as it is.
    template<typename Kind, typename Node>
    auto as_kind(Node* node) {
        return static_cast<std::conditional_t<std::is_const_v<Node>, const Kind*, Kind*>>(node);
    }

    // Every function call of the plan, `visit`ed with its function_expression_t, or with the node_function_t of a
    // table function, then the node that carries it and whether it is in a CHECK: the nodes' expressions, an UPDATE's
    // SET list, RETURNING, an ON CONFLICT's WHERE; in a CHECK: a CHECK constraint's expression and the CHECK
    // predicates a DML carries. A DEFAULT is a constant by then. A const plan is walked as it is: the calls stay the
    // callers' to change, the nodes are not.
    template<typename Node, typename Visit>
        requires std::is_same_v<std::remove_const_t<Node>, node_t>
    void for_each_plan_call(Node* root, Visit& visit) {
        if (root == nullptr) {
            return;
        }
        std::pmr::vector<Node*> pending{{root}, root->resource()};
        while (!pending.empty()) {
            auto* node = pending.back();
            pending.pop_back();
            if (node == nullptr) {
                continue;
            }
            auto plain = [&visit, node](auto& call) { visit(call, *node, false); };
            auto in_check = [&visit, node](auto& call) { visit(call, *node, true); };
            const auto each = [&plain](const std::pmr::vector<expressions::expression_ptr>& expressions) {
                for (const auto& expression : expressions) {
                    expressions::for_each_call(expression, plain);
                }
            };
            const auto checks =
                [&in_check](const std::vector<std::pair<std::string, expressions::expression_ptr>>& checks) {
                    for (const auto& check : checks) {
                        expressions::for_each_call(check.second, in_check);
                    }
                };
            each(node->expressions());
            switch (node->type()) {
                case node_type::function_t:
                    plain(*as_kind<node_function_t>(node));
                    break;
                case node_type::update_t: {
                    const auto* update = as_kind<node_update_t>(node);
                    each(update->updates());
                    each(update->returning());
                    checks(update->check_predicates());
                    break;
                }
                case node_type::insert_t: {
                    const auto* insert = as_kind<node_insert_t>(node);
                    each(insert->returning());
                    checks(insert->check_predicates());
                    break;
                }
                case node_type::delete_t:
                    each(as_kind<node_delete_t>(node)->returning());
                    break;
                case node_type::check_constraint_t:
                    checks(as_kind<node_check_constraint_t>(node)->check_predicates());
                    break;
                case node_type::insert_on_conflict_t:
                    expressions::for_each_call(as_kind<node_insert_on_conflict_t>(node)->on_conflict().target_where,
                                               plain);
                    break;
                case node_type::create_constraint_t:
                    expressions::for_each_call(as_kind<node_create_constraint_t>(node)->check_expression(), in_check);
                    break;
                default:
                    break;
            }
            for (const auto& child : node->children()) {
                pending.push_back(child.get());
            }
        }
    }

} // namespace components::logical_plan

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    void transformer::join_dfs(std::pmr::memory_resource* resource,
                               JoinExpr* join,
                               logical_plan::node_join_ptr& node_join,
                               name_collection_t& names,
                               logical_plan::parameter_node_t* params) {
        if (nodeTag(join->larg) == T_JoinExpr) {
            name_collection_t sub_query_names;
            join_dfs(resource, pg_ptr_cast<JoinExpr>(join->larg), node_join, sub_query_names, params);
            auto prev = node_join;
            node_join =
                logical_plan::make_node_join(resource, {database_name_t(), collection_name_t()}, jointype_to_ql(join));
            node_join->append_child(prev);
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                sub_query_names.right_name = rangevar_to_collection(table_r);
                sub_query_names.right_alias = construct_alias(table_r->alias);
                node_join->append_child(logical_plan::make_node_aggregate(resource, sub_query_names.right_name));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, sub_query_names, params));
            }
            names.right_name = sub_query_names.right_name;
            names.right_alias = sub_query_names.right_alias;
        } else if (nodeTag(join->larg) == T_RangeVar) {
            // bamboo end
            auto table_l = pg_ptr_cast<RangeVar>(join->larg);
            assert(!node_join);
            names.left_name = rangevar_to_collection(table_l);
            names.left_alias = construct_alias(table_l->alias);
            node_join = logical_plan::make_node_join(resource, {}, jointype_to_ql(join));
            node_join->append_child(logical_plan::make_node_aggregate(resource, names.left_name));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                names.right_name = rangevar_to_collection(table_r);
                names.right_alias = construct_alias(table_r->alias);
                node_join->append_child(logical_plan::make_node_aggregate(resource, names.right_name));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, names, params));
            }
        } else if (nodeTag(join->larg) == T_RangeFunction) {
            assert(!node_join);
            node_join =
                logical_plan::make_node_join(resource, {database_name_t(), collection_name_t()}, jointype_to_ql(join));
            node_join->append_child(transform_function(*pg_ptr_cast<RangeFunction>(join->larg), names, params));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                names.right_name = rangevar_to_collection(table_r);
                names.right_alias = construct_alias(table_r->alias);
                node_join->append_child(logical_plan::make_node_aggregate(resource, names.right_name));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, names, params));
            }
        } else {
            throw parser_exception_t{"incorrect type for join join->larg node",
                                     node_tag_to_string(nodeTag(join->larg))};
        }
        // on
        if (join->quals) {
            // should always be A_Expr
            if (nodeTag(join->quals) == T_A_Expr) {
                node_join->append_expression(transform_a_expr(pg_ptr_cast<A_Expr>(join->quals), names, params));
            } else if (nodeTag(join->quals) == T_A_Indirection) {
                node_join->append_expression(
                    transform_a_indirection(pg_ptr_cast<A_Indirection>(join->quals), names, params));
            } else if (nodeTag(join->quals) == T_FuncCall) {
                node_join->append_expression(transform_a_expr_func(pg_ptr_cast<FuncCall>(join->quals), names, params));
            } else {
                throw parser_exception_t{"incorrect type for join join->quals node",
                                         node_tag_to_string(nodeTag(join->larg))};
            }
        } else {
            node_join->append_expression(make_compare_expression(resource, compare_type::all_true));
        }
    }

    logical_plan::node_ptr transformer::transform_select(SelectStmt& node, logical_plan::parameter_node_t* params) {
        logical_plan::node_aggregate_ptr agg = nullptr;
        logical_plan::node_join_ptr join = nullptr;
        name_collection_t names;

        if (node.fromClause && !node.fromClause->lst.empty()) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                // from table_name
                auto table = pg_ptr_cast<RangeVar>(from_first);
                names.left_name = rangevar_to_collection(table);
                names.left_alias = construct_alias(table->alias);
                agg = logical_plan::make_node_aggregate(resource_, names.left_name);
            } else if (nodeTag(from_first) == T_JoinExpr) {
                // from table_1 join table_2 on cond
                agg = logical_plan::make_node_aggregate(resource_, {});
                join_dfs(resource_, pg_ptr_cast<JoinExpr>(from_first), join, names, params);
                agg->append_child(join);
            } else if (nodeTag(from_first) == T_RangeFunction) {
                agg = logical_plan::make_node_aggregate(resource_, {});
                auto range_func = *pg_ptr_cast<RangeFunction>(from_first);
                names.left_alias = construct_alias(range_func.alias);
                agg->append_child(transform_function(range_func, names, params));
            }
        } else {
            agg = logical_plan::make_node_aggregate(resource_, {});
        }

        auto group = logical_plan::make_node_group(resource_, agg->collection_full_name());
        // fields
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                switch (nodeTag(res->val)) {
                    case T_FuncCall: {
                        // group
                        auto func = pg_ptr_cast<FuncCall>(res->val);

                        auto funcname = std::string{strVal(linitial(func->funcname))};
                        std::pmr::vector<param_storage> args;
                        args.reserve(func->args->lst.size());
                        // Note: AGGREGATE(*) invoke parameterless aggregate (also agg_star is set to true)
                        for (const auto& arg : func->args->lst) {
                            auto arg_value = pg_ptr_cast<Node>(arg.data);
                            if (nodeTag(arg_value) == T_ColumnRef) {
                                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg_value), names);
                                key.deduce_side(names);
                                args.emplace_back(std::move(key.field));
                            } else {
                                args.emplace_back(add_param_value(arg_value, params));
                            }
                        }

                        std::string expr_name;
                        if (res->name) {
                            expr_name = res->name;
                        } else {
                            expr_name = funcname;
                        }

                        auto expr = make_aggregate_expression(resource_,
                                                              funcname,
                                                              expressions::key_t{resource_, std::move(expr_name)});
                        for (const auto& arg : args) {
                            expr->append_param(arg);
                        }
                        if (func->agg_distinct) {
                            expr->set_distinct(true);
                        }
                        group->append_expression(expr);

                        break;
                    }
                    case T_ColumnRef: {
                        // field
                        auto table = pg_ptr_cast<ColumnRef>(res->val)->fields->lst;

                        if (nodeTag(table.front().data) == T_A_Star) {
                            // ???
                            break;
                        }
                        if (res->name) {
                            group->append_expression(make_scalar_expression(
                                resource_,
                                scalar_type::get_field,
                                expressions::key_t{resource_, res->name},
                                columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(res->val), names).field));
                        } else {
                            group->append_expression(make_scalar_expression(
                                resource_,
                                scalar_type::get_field,
                                columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(res->val), names).field));
                        }
                        break;
                    }
                    case T_ParamRef: // fall-through
                    case T_TypeCast: // fall-through
                    case T_A_Const: {
                        // constant
                        auto expr = make_scalar_expression(
                            resource_,
                            scalar_type::get_field,
                            expressions::key_t{resource_, res->name ? res->name : get_str_value(res->val)});
                        expr->append_param(add_param_value(res->val, params));
                        group->append_expression(expr);
                        break;
                    }
                    case T_A_Indirection: {
                        std::pmr::vector<std::pmr::string> path;
                        A_Indirection* indirection = pg_ptr_cast<A_Indirection>(res->val);
                        while (indirection) {
                            auto& lst = indirection->indirection->lst;
                            // reverse order to be consistent with indirections stacking
                            for (auto it = lst.rbegin(); it != lst.crend(); ++it) {
                                auto data = it->data;
                                if (nodeTag(data) == T_A_Star) {
                                    path.emplace_back("*");
                                } else if (nodeTag(data) == T_A_Indices) {
                                    auto indices = pg_ptr_cast<A_Indices>(data);
                                    path.emplace_back(indices_to_str(resource_, indices));
                                } else {
                                    path.emplace_back(pmrStrVal(data, resource_));
                                }
                            }
                            if (nodeTag(indirection->arg) == T_A_Indirection) {
                                indirection = pg_ptr_cast<A_Indirection>(indirection->arg);
                            } else if (nodeTag(indirection->arg) == T_FuncCall) {
                                // function here is an aggregate_expr and field selection is a scalar_expr
                                // TODO: proper expression chaining support
                                throw parser_exception_t(
                                    "Otterbrix does not support field selection from function results for now",
                                    {});
                            } else {
                                path.emplace_back(
                                    pmrStrVal(pg_ptr_cast<ColumnRef>(indirection->arg)->fields->lst.back().data,
                                              resource_));
                                break;
                            }
                        }
                        std::reverse(path.begin(), path.end());

                        group->append_expression(make_scalar_expression(resource_,
                                                                        scalar_type::get_field,
                                                                        expressions::key_t{std::move(path)}));
                        break;
                    }
                    case T_CaseExpr: {
                        auto* case_expr = pg_ptr_cast<CaseExpr>(res->val);
                        std::string expr_name;
                        if (res->name) {
                            expr_name = res->name;
                        } else {
                            expr_name = "case";
                        }
                        auto expr = make_scalar_expression(resource_,
                                                           scalar_type::case_when,
                                                           expressions::key_t{resource_, std::move(expr_name)});

                        for (auto& when_item : case_expr->args->lst) {
                            auto* case_when = pg_ptr_cast<CaseWhen>(when_item.data);
                            // condition
                            if (case_expr->arg) {
                                // Simple CASE: CASE col WHEN val THEN ...
                                // Generate equality: col = val
                                auto col_key =
                                    columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(case_expr->arg), names);
                                col_key.deduce_side(names);
                                auto param_id = add_param_value(pg_ptr_cast<Node>(case_when->expr), params);
                                auto cond = make_compare_expression(params->parameters().resource(),
                                                                    compare_type::eq,
                                                                    col_key.field,
                                                                    param_id);
                                expr->append_param(expression_ptr(cond));
                            } else {
                                // Searched CASE: CASE WHEN condition THEN ...
                                expression_ptr cond;
                                if (nodeTag(case_when->expr) == T_A_Expr) {
                                    cond = transform_a_expr(pg_ptr_cast<A_Expr>(case_when->expr), names, params);
                                } else if (nodeTag(case_when->expr) == T_NullTest) {
                                    cond = transform_null_test(pg_ptr_cast<NullTest>(case_when->expr), names, params);
                                } else {
                                    cond = transform_a_expr(pg_ptr_cast<A_Expr>(case_when->expr), names, params);
                                }
                                expr->append_param(cond);
                            }
                            // result
                            auto* result_node = pg_ptr_cast<Node>(case_when->result);
                            if (nodeTag(result_node) == T_ColumnRef) {
                                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(result_node), names);
                                key.deduce_side(names);
                                expr->append_param(std::move(key.field));
                            } else {
                                expr->append_param(add_param_value(result_node, params));
                            }
                        }
                        // ELSE (defresult)
                        if (case_expr->defresult) {
                            auto* def_node = pg_ptr_cast<Node>(case_expr->defresult);
                            if (nodeTag(def_node) == T_ColumnRef) {
                                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(def_node), names);
                                key.deduce_side(names);
                                expr->append_param(std::move(key.field));
                            } else {
                                expr->append_param(add_param_value(def_node, params));
                            }
                        } else {
                            // No ELSE → NULL
                            expr->append_param(params->add_parameter(
                                types::logical_value_t(resource_,
                                                       types::complex_logical_type{types::logical_type::NA})));
                        }
                        group->append_expression(expr);
                        break;
                    }
                    case T_CoalesceExpr: {
                        auto* coalesce = pg_ptr_cast<CoalesceExpr>(res->val);
                        std::string expr_name;
                        if (res->name) {
                            expr_name = res->name;
                        } else {
                            expr_name = "coalesce";
                        }
                        auto expr = make_scalar_expression(resource_,
                                                           scalar_type::coalesce,
                                                           expressions::key_t{resource_, std::move(expr_name)});
                        for (auto& arg_item : coalesce->args->lst) {
                            auto arg_node = pg_ptr_cast<Node>(arg_item.data);
                            if (nodeTag(arg_node) == T_ColumnRef) {
                                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg_node), names);
                                key.deduce_side(names);
                                expr->append_param(std::move(key.field));
                            } else {
                                expr->append_param(add_param_value(arg_node, params));
                            }
                        }
                        group->append_expression(expr);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unknown node type in field clause: " +
                                                 node_tag_to_string(nodeTag(res->val)));
                }
            }
        }

        // where
        if (node.whereClause) {
            expression_ptr expr;
            if (nodeTag(node.whereClause) == T_FuncCall) {
                expr = transform_a_expr_func(pg_ptr_cast<FuncCall>(node.whereClause), names, params);
            } else if (nodeTag(node.whereClause) == T_NullTest) {
                expr = transform_null_test(pg_ptr_cast<NullTest>(node.whereClause), names, params);
            } else {
                expr = transform_a_expr(pg_ptr_cast<A_Expr>(node.whereClause), names, params);
            }
            if (expr) {
                agg->append_child(logical_plan::make_node_match(resource_, agg->collection_full_name(), expr));
            }
        }

        if (node.groupClause && !node.groupClause->lst.empty()) {
            // TODO: check GROUP BY & SELECT field correctness: every non-agg & non-const field MUST BE in GROUP BY!
            // Note: right now execution implicitly assumes that every SELECT field is in GROUP BY
            for (auto field : node.groupClause->lst) {
                if (nodeTag(field.data) != T_ColumnRef) {
                    throw std::runtime_error("Unknown node type in group by clause: " +
                                             node_tag_to_string(nodeTag(field.data)));
                }

                group->append_expression(make_scalar_expression(
                    resource_,
                    scalar_type::group_field,
                    columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(field.data), names).field));
            }
        }

        if (!group->expressions().empty()) {
            agg->append_child(group);
        }

        // having
        if (node.havingClause) {
            // Helper: find the SELECT alias for a FuncCall used in HAVING
            auto find_having_alias = [&](FuncCall* func) -> std::string {
                auto funcname = std::string{strVal(linitial(func->funcname))};
                for (auto target : node.targetList->lst) {
                    auto res = pg_ptr_cast<ResTarget>(target.data);
                    if (nodeTag(res->val) == T_FuncCall) {
                        auto target_func = pg_ptr_cast<FuncCall>(res->val);
                        auto target_funcname = std::string{strVal(linitial(target_func->funcname))};
                        if (target_funcname == funcname) {
                            return res->name ? std::string(res->name) : funcname;
                        }
                    }
                }
                return funcname;
            };

            // Transform a HAVING comparison that may contain aggregate functions
            auto transform_having_comparison = [&](A_Expr* expr) -> expression_ptr {
                if (expr->kind != AEXPR_OP) {
                    return transform_a_expr(expr, names, params);
                }
                bool left_is_func = nodeTag(expr->lexpr) == T_FuncCall;
                bool right_is_func = nodeTag(expr->rexpr) == T_FuncCall;
                if (!left_is_func && !right_is_func) {
                    return transform_a_expr(expr, names, params);
                }
                auto op_name = std::string(strVal(expr->name->lst.front().data));
                auto cmp = get_compare_type(op_name);
                if (left_is_func) {
                    auto alias = find_having_alias(pg_ptr_cast<FuncCall>(expr->lexpr));
                    auto key = expressions::key_t{resource_, std::move(alias)};
                    auto param_id = add_param_value(expr->rexpr, params);
                    return make_compare_expression(params->parameters().resource(), cmp, key, param_id);
                } else {
                    auto alias = find_having_alias(pg_ptr_cast<FuncCall>(expr->rexpr));
                    auto key = expressions::key_t{resource_, std::move(alias)};
                    auto param_id = add_param_value(expr->lexpr, params);
                    // Flip comparison direction for value op aggregate
                    if (cmp == compare_type::gt)
                        cmp = compare_type::lt;
                    else if (cmp == compare_type::lt)
                        cmp = compare_type::gt;
                    else if (cmp == compare_type::gte)
                        cmp = compare_type::lte;
                    else if (cmp == compare_type::lte)
                        cmp = compare_type::gte;
                    return make_compare_expression(params->parameters().resource(), cmp, key, param_id);
                }
            };

            expression_ptr having_expr;
            if (nodeTag(node.havingClause) == T_A_Expr) {
                having_expr = transform_having_comparison(pg_ptr_cast<A_Expr>(node.havingClause));
            } else if (nodeTag(node.havingClause) == T_BoolExpr) {
                auto* bool_expr = pg_ptr_cast<BoolExpr>(node.havingClause);
                auto cmp_type = bool_expr->boolop == AND_EXPR ? compare_type::union_and : compare_type::union_or;
                auto union_expr = make_compare_union_expression(params->parameters().resource(), cmp_type);
                for (auto& item : bool_expr->args->lst) {
                    expression_ptr child;
                    if (nodeTag(item.data) == T_A_Expr) {
                        child = transform_having_comparison(pg_ptr_cast<A_Expr>(item.data));
                    } else if (nodeTag(item.data) == T_NullTest) {
                        child = transform_null_test(pg_ptr_cast<NullTest>(item.data), names, params);
                    }
                    if (child)
                        union_expr->append_child(child);
                }
                having_expr = union_expr;
            } else if (nodeTag(node.havingClause) == T_NullTest) {
                having_expr = transform_null_test(pg_ptr_cast<NullTest>(node.havingClause), names, params);
            }
            if (having_expr) {
                agg->append_child(logical_plan::make_node_having(resource_, agg->collection_full_name(), having_expr));
            }
        }

        // distinct
        if (node.distinctClause && !node.distinctClause->lst.empty()) {
            agg->set_distinct(true);
        }

        // order by
        if (node.sortClause && !node.sortClause->lst.empty()) {
            std::vector<expression_ptr> expressions;
            expressions.reserve(node.sortClause->lst.size());
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                column_ref_t field(resource_);
                if (nodeTag(sortby->node) == T_ColumnRef) {
                    field = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(sortby->node), names);
                } else {
                    assert(nodeTag(sortby->node) == T_A_Indirection);
                    field = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(sortby->node), names);
                }
                expressions.emplace_back(
                    make_sort_expression(field.field,
                                         sortby->sortby_dir == SORTBY_DESC ? sort_order::desc : sort_order::asc));
            }
            agg->append_child(logical_plan::make_node_sort(resource_, agg->collection_full_name(), expressions));
        }

        // limit
        if (node.limitCount) {
            if (nodeTag(node.limitCount) != T_A_Const) {
                throw std::runtime_error("Unknown node type in limit clause: " +
                                         node_tag_to_string(nodeTag(node.limitCount)));
            }

            auto* value = &(pg_ptr_cast<A_Const>(node.limitCount)->val);
            logical_plan::limit_t limit;
            switch (nodeTag(value)) {
                case T_Null: {
                    limit = logical_plan::limit_t::unlimit();
                    break;
                }
                case T_Integer:
                    limit = logical_plan::limit_t(intVal(value));
                    break;
                default:
                    throw std::runtime_error("Forbidden expression in limit clause: allowed only LIMIT <integer>/ALL");
            }

            agg->append_child(logical_plan::make_node_limit(resource_, agg->collection_full_name(), limit));
        }

        return agg;
    }
} // namespace components::sql::transform

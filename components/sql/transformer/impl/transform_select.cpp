#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
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
                               logical_plan::parameter_node_t* params) {
        if (nodeTag(join->larg) == T_JoinExpr) {
            join_dfs(resource, pg_ptr_cast<JoinExpr>(join->larg), node_join, params);
            auto prev = node_join;
            node_join =
                logical_plan::make_node_join(resource, {database_name_t(), collection_name_t()}, jointype_to_ql(join));
            node_join->append_child(prev);
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                node_join->append_child(logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_r)));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, params));
            }
        } else if (nodeTag(join->larg) == T_RangeVar) {
            // bamboo end
            auto table_l = pg_ptr_cast<RangeVar>(join->larg);
            assert(!node_join);
            node_join = logical_plan::make_node_join(resource, {}, jointype_to_ql(join));
            node_join->append_child(logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_l)));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                node_join->append_child(logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_r)));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, params));
            }
        } else if (nodeTag(join->larg) == T_RangeFunction) {
            assert(!node_join);
            node_join =
                logical_plan::make_node_join(resource, {database_name_t(), collection_name_t()}, jointype_to_ql(join));
            node_join->append_child(transform_function(*pg_ptr_cast<RangeFunction>(join->larg), params));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                node_join->append_child(logical_plan::make_node_aggregate(resource, rangevar_to_collection(table_r)));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, params));
            }
        } else {
            throw parser_exception_t{"incorrect type for join join->larg node",
                                     node_tag_to_string(nodeTag(join->larg))};
        }
        // on
        if (join->quals) {
            // should always be A_Expr
            assert(nodeTag(join->quals) == T_A_Expr);
            auto a_expr = pg_ptr_cast<A_Expr>(join->quals);
            node_join->append_expression(transform_a_expr(params, a_expr));
        } else {
            node_join->append_expression(make_compare_expression(resource, compare_type::all_true));
        }
    }

    logical_plan::node_ptr transformer::transform_select(SelectStmt& node, logical_plan::parameter_node_t* params) {
        logical_plan::node_aggregate_ptr agg = nullptr;
        logical_plan::node_join_ptr join = nullptr;
        // temp solution for custom function
        logical_plan::node_ptr overlying_func = nullptr;

        if (node.fromClause) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                // from table_name
                auto table = pg_ptr_cast<RangeVar>(from_first);
                agg = logical_plan::make_node_aggregate(resource_, rangevar_to_collection(table));
            } else if (nodeTag(from_first) == T_JoinExpr) {
                // from table_1 join table_2 on cond
                agg = logical_plan::make_node_aggregate(resource_, {});
                join_dfs(resource_, pg_ptr_cast<JoinExpr>(from_first), join, params);
                agg->append_child(join);
            }
        } else {
            throw parser_exception_t{"otterbrix currently does not support SELECT without FROM", ""};
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
                        auto arg = std::string{
                            strVal(pg_ptr_cast<ColumnRef>(func->args->lst.front().data)->fields->lst.front().data)};
                        auto funcname = std::string{strVal(func->funcname->lst.front().data)};

                        std::string expr_name;
                        if (res->name) {
                            expr_name = res->name;
                        } else {
                            expr_name.reserve(funcname.size() + arg.size() + 2);
                            expr_name.append(funcname).append("(").append(arg).append(")");
                        }

                        group->append_expression(
                            make_aggregate_expression(resource_,
                                                      get_aggregate_type(funcname),
                                                      components::expressions::key_t{std::move(expr_name)},
                                                      components::expressions::key_t{std::move(arg)}));
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
                            group->append_expression(
                                make_scalar_expression(resource_,
                                                       scalar_type::get_field,
                                                       components::expressions::key_t{res->name},
                                                       components::expressions::key_t{strVal(table.back().data)}));
                        } else {
                            group->append_expression(
                                make_scalar_expression(resource_,
                                                       scalar_type::get_field,
                                                       components::expressions::key_t{strVal(table.back().data)}));
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
                            components::expressions::key_t{res->name ? res->name : get_str_value(res->val)});
                        expr->append_param(add_param_value(res->val, params));
                        group->append_expression(expr);
                        break;
                    }
                    default:
                        throw std::runtime_error("Unknown node type: " + node_tag_to_string(nodeTag(res->val)));
                }
            }
        }

        // where
        if (node.whereClause) {
            if (nodeTag(node.whereClause) == T_FuncCall) {
                auto func = pg_ptr_cast<FuncCall>(node.whereClause);
                overlying_func = transform_function(*func, params);
            } else {
                auto where = pg_ptr_cast<A_Expr>(node.whereClause);
                auto expr = transform_a_expr(params, where, &overlying_func);
                if (expr) {
                    agg->append_child(logical_plan::make_node_match(resource_, agg->collection_full_name(), expr));
                }
            }
        }

        // group by
        if (node.groupClause) {
            // commented: current parser does not translate this clause to any logical plan
            // todo: add groupClause correctness check
        }

        if (!group->expressions().empty()) {
            agg->append_child(group);
        }

        // order by
        if (node.sortClause) {
            std::vector<expression_ptr> expressions;
            expressions.reserve(node.sortClause->lst.size());
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                assert(nodeTag(sortby->node) == T_ColumnRef);
                auto field = strVal(pg_ptr_cast<ColumnRef>(sortby->node)->fields->lst.back().data);
                expressions.emplace_back(
                    make_sort_expression(components::expressions::key_t{field},
                                         sortby->sortby_dir == SORTBY_DESC ? sort_order::desc : sort_order::asc));
            }
            agg->append_child(logical_plan::make_node_sort(resource_, agg->collection_full_name(), expressions));
        }

        if (overlying_func) {
            overlying_func->append_child(agg);
            return overlying_func;
        }
        return agg;
    }
} // namespace components::sql::transform

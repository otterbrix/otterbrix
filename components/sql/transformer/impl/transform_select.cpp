#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
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
                logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, jointype_to_ql(join));
            node_join->append_child(prev);
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                sub_query_names.right_name = rangevar_to_qualified_name(table_r);
                sub_query_names.right_alias = construct_alias(table_r->alias);
                node_join->append_child(logical_plan::make_node_aggregate(resource,
                                                                          core::uid_t{sub_query_names.right_name.uuid},
                                                                          core::dbname_t{sub_query_names.right_name.dbname},
                                                                          core::relname_t{sub_query_names.right_name.relname}));
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
            names.left_name = rangevar_to_qualified_name(table_l);
            names.left_alias = construct_alias(table_l->alias);
            node_join = logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, jointype_to_ql(join));
            node_join->append_child(logical_plan::make_node_aggregate(resource,
                                                                      core::uid_t{names.left_name.uuid},
                                                                      core::dbname_t{names.left_name.dbname},
                                                                      core::relname_t{names.left_name.relname}));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                names.right_name = rangevar_to_qualified_name(table_r);
                names.right_alias = construct_alias(table_r->alias);
                node_join->append_child(logical_plan::make_node_aggregate(resource,
                                                                          core::uid_t{names.right_name.uuid},
                                                                          core::dbname_t{names.right_name.dbname},
                                                                          core::relname_t{names.right_name.relname}));
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, names, params));
            }
        } else if (nodeTag(join->larg) == T_RangeFunction) {
            assert(!node_join);
            node_join =
                logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, jointype_to_ql(join));
            node_join->append_child(transform_function(*pg_ptr_cast<RangeFunction>(join->larg), names, params));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                names.right_name = rangevar_to_qualified_name(table_r);
                names.right_alias = construct_alias(table_r->alias);
                node_join->append_child(logical_plan::make_node_aggregate(resource,
                                                                          core::uid_t{names.right_name.uuid},
                                                                          core::dbname_t{names.right_name.dbname},
                                                                          core::relname_t{names.right_name.relname}));
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
        // Set operations (UNION / INTERSECT / EXCEPT) are not yet wired
        // through the transformer. For a SETOP_* node, node.targetList is
        // null (the column projection lives on the larg / rarg children),
        // so the for-loop below would dereference null and SIGSEGV. Bail
        // out cleanly until proper set-operation lowering lands.
        // dynamic_schema_union sits on this path; lldb pinned the crash to
        // node.targetList->lst at line 137 here.
        if (node.op != SETOP_NONE || node.targetList == nullptr) {
            throw std::runtime_error(
                "SELECT set operations (UNION / INTERSECT / EXCEPT) are not "
                "yet supported by the SQL transformer");
        }

        logical_plan::node_aggregate_ptr agg = nullptr;
        logical_plan::node_join_ptr join = nullptr;
        name_collection_t names;

        if (node.fromClause && !node.fromClause->lst.empty()) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                // from table_name
                auto table = pg_ptr_cast<RangeVar>(from_first);
                names.left_name = rangevar_to_qualified_name(table);
                names.left_alias = construct_alias(table->alias);
                agg = logical_plan::make_node_aggregate(resource_,
                                                        core::uid_t{names.left_name.uuid},
                                                        core::dbname_t{names.left_name.dbname},
                                                        core::relname_t{names.left_name.relname});
            } else if (nodeTag(from_first) == T_JoinExpr) {
                // from table_1 join table_2 on cond
                agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                join_dfs(resource_, pg_ptr_cast<JoinExpr>(from_first), join, names, params);
                agg->append_child(join);
            } else if (nodeTag(from_first) == T_RangeFunction) {
                agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                auto range_func = *pg_ptr_cast<RangeFunction>(from_first);
                names.left_alias = construct_alias(range_func.alias);
                agg->append_child(transform_function(range_func, names, params));
            } else {
                // Unhandled FROM type (e.g. T_RangeSubselect for
                // `(SELECT ...) AS sub` derived tables). Without this guard
                // `agg` stays null and the next agg->dbname() at line ~147
                // hits the intrusive_ptr assert (px != 0) → SIGABRT.
                // dynamic_schema_subquery pins this; the test accepts a clean
                // is_success()=false (WARN branch) until derived-table
                // subqueries are wired through the planner.
                throw std::runtime_error(
                    "FROM clause type not supported by the SQL transformer: "
                    + std::string{node_tag_to_string(nodeTag(from_first))});
            }
        } else {
            agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
        }

        auto group = logical_plan::make_node_group(resource_, core::dbname_t{agg->dbname()}, core::relname_t{agg->relname()});
        // fields — collect expressions into group
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
                            } else if (nodeTag(arg_value) == T_A_Expr) {
                                auto sub = pg_ptr_cast<A_Expr>(arg_value);
                                if (sub->kind == AEXPR_OP &&
                                    is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                                    args.emplace_back(transform_a_expr_arithmetic(sub, names, params));
                                } else {
                                    args.emplace_back(add_param_value(arg_value, params));
                                }
                            } else if (nodeTag(arg_value) == T_CaseExpr) {
                                // SUM(CASE WHEN ...) and friends — pass the CASE through as a
                                // scalar expression so validate sees its real param shape and
                                // the executor can evaluate per row. The pre-existing fallback
                                // would have wrapped the whole CASE as a constant parameter,
                                // breaking aggregation.
                                auto case_expr_node = pg_ptr_cast<CaseExpr>(arg_value);
                                logical_plan::node_ptr null_group{};
                                args.emplace_back(
                                    case_expr_to_scalar(case_expr_node, nullptr, names, params, null_group));
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
                    case T_A_Expr: {
                        auto a_expr = pg_ptr_cast<A_Expr>(res->val);
                        if (a_expr->kind == AEXPR_OP) {
                            auto op_str = std::string_view(strVal(a_expr->name->lst.front().data));
                            if (is_arithmetic_operator(op_str)) {
                                logical_plan::node_ptr group_node = group;
                                transform_select_a_expr(a_expr, res->name, names, params, group_node);
                                break;
                            }
                        }
                        throw std::runtime_error("Unknown A_Expr kind in field clause");
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
                        logical_plan::node_ptr group_node = group;
                        transform_select_case_expr(pg_ptr_cast<CaseExpr>(res->val),
                                                   res->name,
                                                   names,
                                                   params,
                                                   group_node);
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

        // Record visible SELECT column count before adding group_field/internal aggs
        if (auto* group_node = dynamic_cast<logical_plan::node_group_t*>(group.get())) {
            group_node->visible_select_count = group->expressions().size();
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
                agg->append_child(logical_plan::make_node_match(resource_, core::dbname_t{agg->dbname()}, core::relname_t{agg->relname()}, expr));
            }
        }

        // having (parse before GROUP BY so the group node is created only once)
        expression_ptr having_expr;
        if (node.havingClause) {
            having_expr = transform_having_expr(node.havingClause, names, params, group);
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

        // Flush buffered internal aggregates to end of group expressions
        for (auto& internal_agg : pending_internal_aggs_) {
            group->append_expression(internal_agg);
        }
        if (auto* group_node = dynamic_cast<logical_plan::node_group_t*>(group.get())) {
            group_node->internal_aggregate_count = pending_internal_aggs_.size();
        }
        pending_internal_aggs_.clear();

        if (!group->expressions().empty()) {
            if (having_expr) {
                auto final_group = logical_plan::make_node_group(resource_,
                                                                 core::dbname_t{agg->dbname()},
                                                                 core::relname_t{agg->relname()},
                                                                 group->expressions(),
                                                                 std::move(having_expr));
                auto* src_group = dynamic_cast<logical_plan::node_group_t*>(group.get());
                final_group->internal_aggregate_count = src_group->internal_aggregate_count;
                final_group->visible_select_count = src_group->visible_select_count;
                agg->append_child(final_group);
            } else {
                agg->append_child(group);
            }
        }

        // distinct
        if (node.distinctClause && !node.distinctClause->lst.empty()) {
            agg->set_distinct(true);
        }

        // order by
        // TODO: validate that ORDER BY expressions reference named columns or SELECT-list aliases;
        //       sorting by unnamed expressions is non-standard and may produce unexpected results
        if (node.sortClause && !node.sortClause->lst.empty()) {
            std::vector<expression_ptr> expressions;
            expressions.reserve(node.sortClause->lst.size());
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                column_ref_t field(resource_);
                if (nodeTag(sortby->node) == T_ColumnRef) {
                    field = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(sortby->node), names);
                } else if (nodeTag(sortby->node) == T_A_Indirection) {
                    field = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(sortby->node), names);
                } else if (nodeTag(sortby->node) == T_A_Expr) {
                    // Arithmetic in ORDER BY: create computed alias, add to group, sort by alias
                    auto a_expr = pg_ptr_cast<A_Expr>(sortby->node);
                    std::string sort_alias = "__sort_expr_" + std::to_string(aggregate_counter_++);
                    logical_plan::node_ptr group_node = group;
                    transform_select_a_expr(a_expr, sort_alias.c_str(), names, params, group_node);
                    field.field = expressions::key_t{resource_, sort_alias};
                } else {
                    throw std::runtime_error("Unknown node type in ORDER BY: " +
                                             node_tag_to_string(nodeTag(sortby->node)));
                }
                expressions.emplace_back(
                    make_sort_expression(field.field,
                                         sortby->sortby_dir == SORTBY_DESC ? sort_order::desc : sort_order::asc));
            }
            agg->append_child(logical_plan::make_node_sort(resource_, core::dbname_t{agg->dbname()}, core::relname_t{agg->relname()}, expressions));
        }

        // limit / offset
        int64_t offset_val = 0;
        if (node.limitOffset) {
            if (nodeTag(node.limitOffset) != T_A_Const) {
                throw std::runtime_error("Unknown node type in offset clause: " +
                                         node_tag_to_string(nodeTag(node.limitOffset)));
            }
            auto* off_value = &(pg_ptr_cast<A_Const>(node.limitOffset)->val);
            if (nodeTag(off_value) == T_Integer) {
                offset_val = intVal(off_value);
            } else if (nodeTag(off_value) != T_Null) {
                throw std::runtime_error("Forbidden expression in offset clause: allowed only OFFSET <integer>");
            }
        }
        if (node.limitCount || node.limitOffset) {
            logical_plan::limit_t limit;
            if (node.limitCount) {
                if (nodeTag(node.limitCount) != T_A_Const) {
                    throw std::runtime_error("Unknown node type in limit clause: " +
                                             node_tag_to_string(nodeTag(node.limitCount)));
                }

                auto* value = &(pg_ptr_cast<A_Const>(node.limitCount)->val);
                switch (nodeTag(value)) {
                    case T_Null: {
                        limit = logical_plan::limit_t::unlimit();
                        break;
                    }
                    case T_Integer:
                        limit = logical_plan::limit_t(intVal(value), offset_val);
                        break;
                    default:
                        throw std::runtime_error(
                            "Forbidden expression in limit clause: allowed only LIMIT <integer>/ALL");
                }
            } else {
                // OFFSET without LIMIT — keep limit unbounded but carry the offset.
                limit = logical_plan::limit_t(logical_plan::limit_t::unlimit().limit(), offset_val);
            }

            agg->append_child(logical_plan::make_node_limit(resource_, core::dbname_t{agg->dbname()}, core::relname_t{agg->relname()}, limit));
        }

        return agg;
    }
} // namespace components::sql::transform

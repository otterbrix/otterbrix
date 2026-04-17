#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
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
            auto j_type = jointype_to_ql(join);
            if (j_type == logical_plan::join_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid join type", resource_});
                return;
            }
            node_join = logical_plan::make_node_join(resource, {database_name_t(), collection_name_t()}, j_type);
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
            auto j_type = jointype_to_ql(join);
            if (j_type == logical_plan::join_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid join type", resource_});
                return;
            }
            node_join = logical_plan::make_node_join(resource, {}, j_type);
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
            auto j_type = jointype_to_ql(join);
            if (j_type == logical_plan::join_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid join type", resource_});
                return;
            }
            node_join = logical_plan::make_node_join(resource, {database_name_t(), collection_name_t()}, j_type);
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
            error_ = core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"incorrect type for join join->larg node" + node_tag_to_string(nodeTag(join->larg)),
                                 resource_});
            return;
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
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"incorrect type for join join->quals node" +
                                                            node_tag_to_string(nodeTag(join->quals)),
                                                        resource_});
                return;
            }
        } else {
            node_join->append_expression(make_compare_expression(resource, compare_type::all_true));
        }
    }

    logical_plan::node_ptr transformer::transform_select(SelectStmt& node, logical_plan::parameter_node_t* params) {
        if (node.op == SETOP_UNION) {
            error_ = core::error_t(core::error_code_t::unimplemented_yet,
                                   std::pmr::string{"Select with union is not supported yet", resource_});
            return nullptr;
        } else if (node.op == SETOP_INTERSECT) {
            error_ = core::error_t(core::error_code_t::unimplemented_yet,
                                   std::pmr::string{"Select with intersect is not supported yet", resource_});
            return nullptr;
        } else if (node.op == SETOP_EXCEPT) {
            error_ = core::error_t(core::error_code_t::unimplemented_yet,
                                   std::pmr::string{"Select with except is not supported yet", resource_});
            return nullptr;
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
            } else if (nodeTag(from_first) == T_RangeSubselect) {
                auto* sub_select = pg_ptr_cast<RangeSubselect>(from_first);
                agg = logical_plan::make_node_aggregate(resource_, {});
                agg->append_child(transform_select(*pg_ptr_cast<SelectStmt>(sub_select->subquery), params));

                if (sub_select->alias) {
                    agg->children().back()->set_result_alias(sub_select->alias->aliasname);
                    if (sub_select->alias->colnames &&
                        agg->children().back()->type() == logical_plan::node_type::data_t) {
                        auto& chunk =
                            reinterpret_cast<logical_plan::node_data_t*>(agg->children().back().get())->data_chunk();
                        if (sub_select->alias->colnames->lst.size() != chunk.column_count()) {
                            error_ = core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"column names count has to equal actual column count", resource_});
                            return nullptr;
                        }
                        size_t column_index = 0;
                        for (auto colname : sub_select->alias->colnames->lst) {
                            chunk.data[column_index].set_type_alias(strVal(colname.data));
                            column_index++;
                        }
                    }
                }
            } else {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"encountered unrecognized node", resource_});
                return nullptr;
            }
        } else {
            agg = logical_plan::make_node_aggregate(resource_, {});
        }
        if (node.valuesLists) {
            vector::data_chunk_t chunk(resource_, {}, node.valuesLists->lst.size());
            chunk.set_cardinality(node.valuesLists->lst.size());
            size_t row_index = 0;

            for (auto row : node.valuesLists->lst) {
                auto values = pg_ptr_cast<List>(row.data)->lst;

                size_t column_index = 0;
                for (auto it_value = values.begin(); it_value != values.end(); ++it_value, ++column_index) {
                    auto value = get_value(resource_, pg_ptr_cast<Node>(it_value->data));
                    if (value.has_error()) {
                        error_ = value.error();
                        return nullptr;
                    }
                    if (column_index >= chunk.data.size()) {
                        chunk.data.emplace_back(resource_, value.value().type(), chunk.capacity());
                    }
                    chunk.set_value(column_index, row_index, std::move(value.value()));
                }
                row_index++;
            }

            return logical_plan::make_node_raw_data(resource_, std::move(chunk));
        }

        auto group = logical_plan::make_node_group(resource_, agg->collection_full_name());
        auto select_node = logical_plan::make_node_select(resource_, agg->collection_full_name());

        // fields — collect SELECT expressions into select_node.
        // Star expressions (*) are skipped; an empty select_node means passthrough (SELECT *).
        bool has_non_star = false;
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                switch (nodeTag(res->val)) {
                    case T_FuncCall: {
                        // Aggregate function in SELECT
                        auto func = pg_ptr_cast<FuncCall>(res->val);

                        auto funcname = std::string{strVal(linitial(func->funcname))};
                        std::pmr::vector<param_storage> args{resource_};
                        args.reserve(func->args->lst.size());
                        // Note: AGGREGATE(*) invokes parameterless aggregate (agg_star is set to true)
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
                            } else if (nodeTag(arg_value) == T_FuncCall) {
                                args.emplace_back(
                                    transform_a_expr_func(pg_ptr_cast<FuncCall>(arg_value), names, params));
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
                        select_node->append_expression(expr);
                        has_non_star = true;
                        break;
                    }
                    case T_ColumnRef: {
                        auto col_ref = pg_ptr_cast<ColumnRef>(res->val);
                        // Check for star — add a star_expand marker (cleaned up below if it's the only expression)
                        if (col_ref->fields->lst.size() == 1 && nodeTag(col_ref->fields->lst.back().data) == T_A_Star) {
                            select_node->append_expression(make_scalar_expression(resource_,
                                                                                  scalar_type::star_expand,
                                                                                  expressions::key_t{resource_}));
                            has_non_star = true;
                            break;
                        }
                        has_non_star = true;
                        {
                            auto col = columnref_to_field(resource_, col_ref, names);
                            // Table-qualified wildcard (table.*) where the prefix is a recognized
                            // table alias → star_expand. Struct field wildcards (struct_col.*)
                            // have an unrecognized prefix (col.table is empty) → get_field.
                            if (nodeTag(col_ref->fields->lst.back().data) == T_A_Star && !col.table.empty()) {
                                select_node->append_expression(make_scalar_expression(resource_,
                                                                                      scalar_type::star_expand,
                                                                                      expressions::key_t{resource_}));
                                break;
                            }
                            if (res->name) {
                                select_node->append_expression(
                                    make_scalar_expression(resource_,
                                                           scalar_type::get_field,
                                                           expressions::key_t{resource_, res->name},
                                                           col.field));
                            } else {
                                select_node->append_expression(
                                    make_scalar_expression(resource_, scalar_type::get_field, col.field));
                            }
                        }
                        break;
                    }
                    case T_ParamRef: {
                        has_non_star = true;
                        auto expr = make_scalar_expression(
                            resource_,
                            scalar_type::get_field,
                            expressions::key_t{resource_, res->name ? res->name : get_str_value(res->val)});
                        expr->append_param(add_param_value(res->val, params));
                        select_node->append_expression(expr);
                        break;
                    }
                    case T_TypeCast: // fall-through
                    case T_A_Const: {
                        has_non_star = true;
                        auto expr = make_scalar_expression(resource_,
                                                           scalar_type::constant,
                                                           res->name ? expressions::key_t{resource_, res->name}
                                                                     : expressions::key_t{resource_});
                        expr->append_param(add_param_value(res->val, params));
                        select_node->append_expression(expr);
                        break;
                    }
                    case T_A_Expr: {
                        auto a_expr = pg_ptr_cast<A_Expr>(res->val);
                        if (a_expr->kind == AEXPR_OP) {
                            auto op_str = std::string_view(strVal(a_expr->name->lst.front().data));
                            if (is_arithmetic_operator(op_str)) {
                                has_non_star = true;
                                logical_plan::node_ptr sel_node = select_node;
                                transform_select_a_expr(a_expr, res->name, names, params, sel_node);
                                break;
                            }
                        }

                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"Unknown A_Expr kind in field clause", resource_});
                        return nullptr;
                    }
                    case T_A_Indirection: {
                        std::pmr::vector<std::pmr::string> path{resource_};
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
                                error_ = core::error_t(
                                    core::error_code_t::unimplemented_yet,
                                    std::pmr::string{
                                        "Otterbrix does not support field selection from function results for now",
                                        resource_});
                                return nullptr;
                            } else if (nodeTag(indirection->arg) == T_ColumnRef) {
                                path.emplace_back(
                                    pmrStrVal(pg_ptr_cast<ColumnRef>(indirection->arg)->fields->lst.back().data,
                                              resource_));
                                break;
                            } else {
                                error_ = core::error_t(
                                    core::error_code_t::unimplemented_yet,
                                    std::pmr::string{"Encountered unsupported expression on transform_select",
                                                     resource_});
                                return nullptr;
                            }
                        }
                        std::reverse(path.begin(), path.end());

                        // Check for star via path
                        if (path.size() == 1 && path[0] == "*") {
                            break; // skip star
                        }
                        has_non_star = true;
                        select_node->append_expression(make_scalar_expression(resource_,
                                                                              scalar_type::get_field,
                                                                              expressions::key_t{std::move(path)}));
                        break;
                    }
                    case T_CaseExpr: {
                        has_non_star = true;
                        logical_plan::node_ptr sel_node = select_node;
                        transform_select_case_expr(pg_ptr_cast<CaseExpr>(res->val), res->name, names, params, sel_node);
                        break;
                    }
                    case T_CoalesceExpr: {
                        has_non_star = true;
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
                        select_node->append_expression(expr);
                        break;
                    }
                    default:
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"Unknown node type in field clause: " +
                                                                    node_tag_to_string(nodeTag(res->val)),
                                                                resource_});
                        return nullptr;
                }
            }

            // If select_node holds exactly one star_expand (pure SELECT *), treat as passthrough.
            auto& sel_exprs = select_node->expressions();
            if (sel_exprs.size() == 1 && sel_exprs[0]->group() == expression_group::scalar) {
                auto* s = static_cast<const scalar_expression_t*>(sel_exprs[0].get());
                if (s->type() == scalar_type::star_expand) {
                    sel_exprs.clear();
                    has_non_star = false;
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

        bool has_group_by = node.groupClause && !node.groupClause->lst.empty();

        if (has_group_by) {
            // TODO: check GROUP BY & SELECT field correctness: every non-agg & non-const field MUST BE in GROUP BY!
            for (auto field : node.groupClause->lst) {
                if (nodeTag(field.data) != T_ColumnRef) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"Unknown node type in group by clause: " +
                                                                node_tag_to_string(nodeTag(field.data)),
                                                            resource_});
                    return nullptr;
                }

                group->append_expression(make_scalar_expression(
                    resource_,
                    scalar_type::group_field,
                    columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(field.data), names).field));
            }
        }

        // Route SELECT expressions and pending internal aggregates:
        // Built-in aggregates (count/sum/avg/min/max) move to group_node and are replaced by
        // get_field refs in select_node. Scalar UDFs stay in select_node as-is.
        // This applies both with and without an explicit GROUP BY — operator_group_t treats the
        // entire chunk as one group when its keys_ is empty.
        if (has_non_star) {
            std::vector<expression_ptr> new_sel_exprs;
            for (auto& expr : select_node->expressions()) {
                if (expr->group() == expression_group::aggregate) {
                    // Every aggregate_expression_t represents an aggregate function — always route
                    // to group_node so that operator_group_t handles it. validate_schema verifies
                    // the function exists. This covers both built-in (count/sum/avg/min/max) and
                    // user-registered aggregate functions.
                    auto* agg_expr = static_cast<const aggregate_expression_t*>(expr.get());
                    std::string alias = agg_expr->key().as_string();
                    group->append_expression(expr);
                    new_sel_exprs.push_back(make_scalar_expression(resource_,
                                                                   scalar_type::get_field,
                                                                   expressions::key_t{resource_, alias}));
                } else {
                    new_sel_exprs.push_back(expr);
                }
            }
            select_node->expressions().clear();
            for (auto& expr : new_sel_exprs) {
                select_node->append_expression(expr);
            }

            // Flush pending internal aggregates to group (sub-aggregates of arithmetic in select).
            // Do NOT set group->internal_aggregate_count: operator_select_t needs these columns
            // for post-aggregate arithmetic, so they must not be erased by operator_group_t.
            for (auto& internal_agg : pending_internal_aggs_) {
                group->append_expression(internal_agg);
            }
            group->internal_aggregate_count = 0;
        }
        pending_internal_aggs_.clear();

        // Having is parsed after aggregates are routed to group so resolve_having_operand can find them.
        expression_ptr having_expr;
        if (node.havingClause) {
            having_expr = transform_having_expr(node.havingClause, names, params, group);
        }

        if (!group->expressions().empty()) {
            if (having_expr) {
                auto final_group = logical_plan::make_node_group(resource_,
                                                                 agg->collection_full_name(),
                                                                 group->expressions(),
                                                                 std::move(having_expr));
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
        if (node.sortClause && !node.sortClause->lst.empty()) {
            std::vector<expression_ptr> sort_exprs;
            sort_exprs.reserve(node.sortClause->lst.size());
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                bool is_desc = sortby->sortby_dir == SORTBY_DESC;
                if (nodeTag(sortby->node) == T_ColumnRef) {
                    column_ref_t field = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(sortby->node), names);
                    sort_exprs.emplace_back(
                        make_sort_expression(field.field, is_desc ? sort_order::desc : sort_order::asc));
                } else if (nodeTag(sortby->node) == T_A_Indirection) {
                    column_ref_t field =
                        indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(sortby->node), names);
                    sort_exprs.emplace_back(
                        make_sort_expression(field.field, is_desc ? sort_order::desc : sort_order::asc));
                } else if (nodeTag(sortby->node) == T_A_Expr) {
                    // Arithmetic ORDER BY: encode as scalar_expression_t with sort order in key.path()[0]
                    // (0 = ascending, 1 = descending). create_plan_sort detects this and builds a
                    // computed_sort_key_t instead of a regular sort key.
                    auto a_expr = pg_ptr_cast<A_Expr>(sortby->node);
                    auto op_str = std::string_view(strVal(a_expr->name->lst.front().data));
                    if (!is_arithmetic_operator(op_str)) {
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"Unsupported operator in ORDER BY", resource_});
                        return nullptr;
                    }
                    std::string sort_alias = "__sort_expr_" + std::to_string(aggregate_counter_++);
                    auto stype = get_arithmetic_scalar_type(op_str);
                    expressions::key_t order_key(resource_);
                    order_key.set_path({is_desc ? size_t(1) : size_t(0)});
                    auto computed_sort = make_scalar_expression(resource_, stype, std::move(order_key));
                    // Resolve operands (without appending to any node — purely for sort)
                    logical_plan::node_ptr dummy_node = group; // resolve_select_operand needs a node_ptr
                    computed_sort->append_param(resolve_select_operand(a_expr->lexpr, names, params, dummy_node));
                    if (a_expr->rexpr) {
                        computed_sort->append_param(resolve_select_operand(a_expr->rexpr, names, params, dummy_node));
                    }
                    sort_exprs.emplace_back(std::move(computed_sort));
                } else {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Unknown node type in ORDER BY: " + node_tag_to_string(nodeTag(sortby->node)),
                                         resource_});
                    return nullptr;
                }
            }
            agg->append_child(logical_plan::make_node_sort(resource_, agg->collection_full_name(), sort_exprs));
        }

        // Append select_node as a child of agg (only if there are actual SELECT columns — not pure star)
        if (has_non_star) {
            agg->append_child(select_node);
        }

        // limit / offset
        if (node.limitCount || node.limitOffset) {
            int64_t limit_val = logical_plan::limit_t::unlimit().limit();
            int64_t offset_val = 0;

            if (node.limitCount) {
                if (nodeTag(node.limitCount) != T_A_Const) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"Unknown node type in limit clause: " +
                                                                node_tag_to_string(nodeTag(node.limitCount)),
                                                            resource_});
                    return nullptr;
                }
                auto* value = &(pg_ptr_cast<A_Const>(node.limitCount)->val);
                switch (nodeTag(value)) {
                    case T_Null:
                        break; // LIMIT ALL — keep unlimit_
                    case T_Integer:
                        limit_val = intVal(value);
                        break;
                    default:
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"Forbidden expression in limit clause: allowed only LIMIT <integer>/ALL",
                                             resource_});
                        return nullptr;
                }
            }

            if (node.limitOffset) {
                if (nodeTag(node.limitOffset) != T_A_Const) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,

                                           std::pmr::string{"Unknown node type in offset clause: " +
                                                                node_tag_to_string(nodeTag(node.limitOffset)),
                                                            resource_});
                    return nullptr;
                }
                auto* value = &(pg_ptr_cast<A_Const>(node.limitOffset)->val);
                switch (nodeTag(value)) {
                    case T_Null:
                        break; // OFFSET NULL — treat as 0
                    case T_Integer:
                        offset_val = intVal(value);
                        break;
                    default:
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,

                            std::pmr::string{"Forbidden expression in offset clause: allowed only OFFSET <integer>",
                                             resource_});
                        return nullptr;
                }
            }

            agg->append_child(logical_plan::make_node_limit(resource_,
                                                            agg->collection_full_name(),
                                                            logical_plan::limit_t(limit_val, offset_val)));
        }

        return agg;
    }
} // namespace components::sql::transform

#include <unordered_set>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_cte_scan.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_recursive_cte.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_union.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    logical_plan::node_aggregate_ptr transformer::build_recursive_cte_ref(const std::string& cte_name,
                                                                          const std::string& effective_alias,
                                                                          logical_plan::execution_plan_t* plan) {
        auto agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
        if (transforming_recursive_member_) {
            auto scan = logical_plan::make_node_cte_scan(resource_, std::pmr::string{cte_name, resource_});
            scan->set_result_alias(effective_alias);
            agg->append_child(std::move(scan));
        } else {
            auto cte_it = recursive_cte_queries_.find(cte_name);
            if (cte_it == recursive_cte_queries_.end()) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"recursive CTE not found: " + cte_name, resource_});
                return nullptr;
            }
            SelectStmt* union_stmt = cte_it->second;
            auto anchor_plan = transform_select(*union_stmt->larg, plan);
            if (has_error()) {
                return nullptr;
            }
            transforming_recursive_member_ = true;
            auto recursive_plan = transform_select(*union_stmt->rarg, plan);
            transforming_recursive_member_ = false;
            if (has_error()) {
                return nullptr;
            }
            auto recursive_cte = logical_plan::make_node_recursive_cte(resource_,
                                                                       std::pmr::string{cte_name, resource_},
                                                                       union_stmt->all,
                                                                       std::move(anchor_plan),
                                                                       std::move(recursive_plan));
            recursive_cte->set_result_alias(effective_alias);
            agg->append_child(std::move(recursive_cte));
        }
        return agg;
    }

    void transformer::join_dfs(std::pmr::memory_resource* resource,
                               JoinExpr* join,
                               logical_plan::node_join_ptr& node_join,
                               name_collection_t& names,
                               logical_plan::execution_plan_t* plan) {
        if (nodeTag(join->larg) == T_JoinExpr) {
            name_collection_t sub_query_names;
            join_dfs(resource, pg_ptr_cast<JoinExpr>(join->larg), node_join, sub_query_names, plan);

            // Snapshot the inner JOIN's full visible scope BEFORE we overwrite
            // sub_query_names.right_* with the outer JOIN's right side.
            auto carry_alias = [&](const std::string& alias) {
                if (!alias.empty()) {
                    names.extra_left_aliases.push_back(alias);
                }
            };
            auto carry_name = [&](const qualified_name& nm) {
                if (!nm.relname.empty()) {
                    names.extra_left_names.push_back(nm);
                }
            };

            carry_alias(sub_query_names.left_alias);
            carry_alias(sub_query_names.right_alias);
            carry_name(sub_query_names.left_name);
            carry_name(sub_query_names.right_name);
            for (const auto& a : sub_query_names.extra_left_aliases) {
                carry_alias(a);
            }
            for (const auto& nm : sub_query_names.extra_left_names) {
                carry_name(nm);
            }

            auto prev = node_join;
            auto j_type = jointype_to_ql(join);
            if (j_type == logical_plan::join_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid join type", resource_});
                return;
            }
            node_join = logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, j_type);
            node_join->append_child(prev);
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                sub_query_names.right_name = rangevar_to_qualified_name(table_r);
                sub_query_names.right_alias = construct_alias(table_r->alias);
                const std::string& effective_alias_r = sub_query_names.right_alias.empty()
                                                           ? sub_query_names.right_name.relname
                                                           : sub_query_names.right_alias;
                if (auto cte_it = cte_queries_.find(table_r->relname); cte_it != cte_queries_.end()) {
                    auto agg_r = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    agg_r->append_child(transform_select(*cte_it->second, plan));
                    agg_r->children().back()->set_result_alias(effective_alias_r);
                    node_join->append_child(std::move(agg_r));
                } else if (recursive_cte_queries_.count(table_r->relname)) {
                    auto agg_r = build_recursive_cte_ref(table_r->relname, effective_alias_r, plan);
                    if (has_error()) {
                        return;
                    }
                    node_join->append_child(std::move(agg_r));
                } else {
                    auto agg_r = logical_plan::make_node_aggregate(resource,
                                                                   core::uid_t{sub_query_names.right_name.uuid},
                                                                   core::dbname_t{sub_query_names.right_name.dbname},
                                                                   core::relname_t{sub_query_names.right_name.relname});
                    if (!sub_query_names.right_alias.empty()) {
                        agg_r->set_result_alias(sub_query_names.right_alias);
                    }
                    node_join->append_child(std::move(agg_r));
                }
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, sub_query_names, plan->parameters.get()));
            }
            names.right_name = sub_query_names.right_name;
            names.right_alias = sub_query_names.right_alias;
        } else if (nodeTag(join->larg) == T_RangeVar) {
            // bamboo end
            auto table_l = pg_ptr_cast<RangeVar>(join->larg);
            assert(!node_join);
            names.left_name = rangevar_to_qualified_name(table_l);
            names.left_alias = construct_alias(table_l->alias);
            auto j_type = jointype_to_ql(join);
            if (j_type == logical_plan::join_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid join type", resource_});
                return;
            }
            node_join = logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, j_type);
            {
                const std::string& effective_alias_l =
                    names.left_alias.empty() ? names.left_name.relname : names.left_alias;
                if (auto cte_it = cte_queries_.find(table_l->relname); cte_it != cte_queries_.end()) {
                    auto agg_l = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    agg_l->append_child(transform_select(*cte_it->second, plan));
                    agg_l->children().back()->set_result_alias(effective_alias_l);
                    node_join->append_child(std::move(agg_l));
                } else if (recursive_cte_queries_.count(table_l->relname)) {
                    auto agg_l = build_recursive_cte_ref(table_l->relname, effective_alias_l, plan);
                    if (has_error()) {
                        return;
                    }
                    node_join->append_child(std::move(agg_l));
                } else {
                    auto agg_l = logical_plan::make_node_aggregate(resource,
                                                                   core::uid_t{names.left_name.uuid},
                                                                   core::dbname_t{names.left_name.dbname},
                                                                   core::relname_t{names.left_name.relname});
                    if (!names.left_alias.empty()) {
                        agg_l->set_result_alias(names.left_alias);
                    }
                    node_join->append_child(std::move(agg_l));
                }
            }
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                names.right_name = rangevar_to_qualified_name(table_r);
                names.right_alias = construct_alias(table_r->alias);
                const std::string& effective_alias_r =
                    names.right_alias.empty() ? names.right_name.relname : names.right_alias;
                if (auto cte_it = cte_queries_.find(table_r->relname); cte_it != cte_queries_.end()) {
                    auto agg_r = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    agg_r->append_child(transform_select(*cte_it->second, plan));
                    agg_r->children().back()->set_result_alias(effective_alias_r);
                    node_join->append_child(std::move(agg_r));
                } else if (recursive_cte_queries_.count(table_r->relname)) {
                    auto agg_r = build_recursive_cte_ref(table_r->relname, effective_alias_r, plan);
                    if (has_error()) {
                        return;
                    }
                    node_join->append_child(std::move(agg_r));
                } else {
                    auto agg_r = logical_plan::make_node_aggregate(resource,
                                                                   core::uid_t{names.right_name.uuid},
                                                                   core::dbname_t{names.right_name.dbname},
                                                                   core::relname_t{names.right_name.relname});
                    if (!names.right_alias.empty()) {
                        agg_r->set_result_alias(names.right_alias);
                    }
                    node_join->append_child(std::move(agg_r));
                }
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, names, plan->parameters.get()));
            } else if (nodeTag(join->rarg) == T_RangeSubselect) {
                auto* sub_select = pg_ptr_cast<RangeSubselect>(join->rarg);
                auto agg_r = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                agg_r->append_child(transform_select(*pg_ptr_cast<SelectStmt>(sub_select->subquery), plan));

                if (sub_select->alias) {
                    agg_r->children().back()->set_result_alias(sub_select->alias->aliasname);
                    if (sub_select->alias->colnames &&
                        agg_r->children().back()->type() == logical_plan::node_type::data_t) {
                        auto& chunk =
                            reinterpret_cast<logical_plan::node_data_t*>(agg_r->children().back().get())->data_chunk();
                        if (sub_select->alias->colnames->lst.size() != chunk.column_count()) {
                            error_ = core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"column names count has to equal actual column count", resource_});
                            return;
                        }
                        size_t column_index = 0;
                        for (auto colname : sub_select->alias->colnames->lst) {
                            chunk.data[column_index].set_type_alias(strVal(colname.data));
                            column_index++;
                        }
                    }
                }
                node_join->append_child(std::move(agg_r));
            }
        } else if (nodeTag(join->larg) == T_RangeFunction) {
            assert(!node_join);
            auto j_type = jointype_to_ql(join);
            if (j_type == logical_plan::join_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid join type", resource_});
                return;
            }
            node_join = logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, j_type);
            node_join->append_child(
                transform_function(*pg_ptr_cast<RangeFunction>(join->larg), names, plan->parameters.get()));
            if (nodeTag(join->rarg) == T_RangeVar) {
                auto table_r = pg_ptr_cast<RangeVar>(join->rarg);
                names.right_name = rangevar_to_qualified_name(table_r);
                names.right_alias = construct_alias(table_r->alias);
                const std::string& effective_alias_r =
                    names.right_alias.empty() ? names.right_name.relname : names.right_alias;
                if (auto cte_it = cte_queries_.find(table_r->relname); cte_it != cte_queries_.end()) {
                    auto agg_r = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    agg_r->append_child(transform_select(*cte_it->second, plan));
                    agg_r->children().back()->set_result_alias(effective_alias_r);
                    node_join->append_child(std::move(agg_r));
                } else if (recursive_cte_queries_.count(table_r->relname)) {
                    auto agg_r = build_recursive_cte_ref(table_r->relname, effective_alias_r, plan);
                    if (has_error()) {
                        return;
                    }
                    node_join->append_child(std::move(agg_r));
                } else {
                    auto agg_r = logical_plan::make_node_aggregate(resource,
                                                                   core::uid_t{names.right_name.uuid},
                                                                   core::dbname_t{names.right_name.dbname},
                                                                   core::relname_t{names.right_name.relname});
                    if (!names.right_alias.empty()) {
                        agg_r->set_result_alias(names.right_alias);
                    }
                    node_join->append_child(std::move(agg_r));
                }
            } else if (nodeTag(join->rarg) == T_RangeFunction) {
                auto func = pg_ptr_cast<RangeFunction>(join->rarg);
                node_join->append_child(transform_function(*func, names, plan->parameters.get()));
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
                node_join->append_expression(transform_a_expr(pg_ptr_cast<A_Expr>(join->quals), names, plan));
            } else if (nodeTag(join->quals) == T_A_Indirection) {
                node_join->append_expression(
                    transform_a_indirection(pg_ptr_cast<A_Indirection>(join->quals), names, plan));
            } else if (nodeTag(join->quals) == T_FuncCall) {
                node_join->append_expression(
                    transform_a_expr_func(pg_ptr_cast<FuncCall>(join->quals), names, plan->parameters.get()));
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

    logical_plan::node_ptr transformer::transform_select(SelectStmt& node, logical_plan::execution_plan_t* plan) {
        // Set operations (UNION / INTERSECT / EXCEPT) are not yet wired
        // through the transformer. For a SETOP_* node, node.targetList is
        // null (the column projection lives on the larg / rarg children),
        // so the for-loop below would dereference null and SIGSEGV. Bail
        // out cleanly until proper set-operation lowering lands.
        // dynamic_schema_union sits on this path; lldb pinned the crash to
        // node.targetList->lst at line 137 here.
        if (node.op == SETOP_UNION) {
            auto left = transform_select(*node.larg, plan);
            auto right = transform_select(*node.rarg, plan);
            if (has_error()) {
                return nullptr;
            }
            return logical_plan::make_node_union(resource_, std::move(left), std::move(right), node.all);
        }
        if (node.op != SETOP_NONE || node.targetList == nullptr) {
            error_ = core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{
                    "SELECT set operations (INTERSECT / EXCEPT) are not yet supported by the SQL transformer",
                    resource_});
            return nullptr;
        }
        if (node.withClause) {
            if (node.withClause->recursive) {
                for (const auto& item : node.withClause->ctes->lst) {
                    auto* cte = pg_ptr_cast<CommonTableExpr>(item.data);
                    recursive_cte_queries_.emplace(cte->ctename, pg_ptr_cast<SelectStmt>(cte->ctequery));
                }
            } else {
                for (const auto& item : node.withClause->ctes->lst) {
                    auto* cte = pg_ptr_cast<CommonTableExpr>(item.data);
                    cte_queries_.emplace(cte->ctename, pg_ptr_cast<SelectStmt>(cte->ctequery));
                }
            }
        }
        logical_plan::node_aggregate_ptr agg = nullptr;
        logical_plan::node_join_ptr join = nullptr;
        name_collection_t names;

        if (node.fromClause && !node.fromClause->lst.empty()) {
            // SQL-89 comma-join: `FROM a, b [, c ...] WHERE a.x = b.y` arrives as
            // a fromClause->lst with multiple top-level entries. libpg_query does
            // NOT synthesize a FromExpr / JoinExpr in that case — each table is a
            // bare T_RangeVar (or T_RangeFunction / T_RangeSubselect) sibling.
            //
            // The downstream pipeline only knows how to consume a single join
            // root, so we synthesize a left-deep JoinExpr tree here with
            // jointype=JOIN_INNER and quals=NULL on every link. jointype_to_ql
            // promotes (JOIN_INNER, quals=NULL) -> join_type::cross, which
            // produces the cross-product. Inner-join semantics are recovered by
            // the user's WHERE clause, which the existing transform path lowers
            // into a sibling match_t on the aggregate root; that match_t
            // evaluates against the post-join merged chunk (operator_match feeds
            // the same chunk in as both left and right), so column refs resolve
            // through the join's merged schema regardless of side_t.
            //
            // The synthesized tree mutates `node.fromClause->lst.front()` so the
            // existing T_JoinExpr branch below picks it up unchanged.
            if (node.fromClause->lst.size() > 1) {
                // Synth parser-AST nodes — consumed within this function by
                // join_dfs which builds independent logical_plan nodes. Live in
                // a transient arena (upstream=resource_) so they don't outlive
                // their scope on the session resource.
                std::pmr::monotonic_buffer_resource transient(resource_);
                auto* resource = &transient; // makeNode macro reads `resource_` / `resource`
                auto it = node.fromClause->lst.begin();
                Node* acc = pg_ptr_cast<Node>(it->data);
                ++it;
                for (; it != node.fromClause->lst.end(); ++it) {
                    auto* rhs = pg_ptr_cast<Node>(it->data);
                    JoinExpr* synth = makeNode(resource, JoinExpr);
                    synth->jointype = JOIN_INNER;
                    synth->isNatural = false;
                    synth->larg = acc;
                    synth->rarg = rhs;
                    synth->usingClause = nullptr;
                    synth->quals = nullptr; // cross — WHERE supplies the predicate
                    synth->alias = nullptr;
                    synth->rtindex = 0;
                    acc = reinterpret_cast<Node*>(synth);
                }
                // Replace the original multi-entry fromClause with a single
                // top-level JoinExpr so the dispatch below sees T_JoinExpr.
                node.fromClause->lst.clear();
                node.fromClause->lst.push_back({acc});
            }

            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                // from table_name
                auto table = pg_ptr_cast<RangeVar>(from_first);
                names.left_name = rangevar_to_qualified_name(table);
                names.left_alias = construct_alias(table->alias);
                const std::string& effective_alias =
                    names.left_alias.empty() ? names.left_name.relname : names.left_alias;
                if (auto cte_it = cte_queries_.find(table->relname); cte_it != cte_queries_.end()) {
                    agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    agg->append_child(transform_select(*cte_it->second, plan));
                    agg->children().back()->set_result_alias(effective_alias);
                } else if (recursive_cte_queries_.count(table->relname)) {
                    agg = build_recursive_cte_ref(table->relname, effective_alias, plan);
                    if (has_error()) {
                        return nullptr;
                    }
                } else {
                    agg = logical_plan::make_node_aggregate(resource_,
                                                            core::uid_t{names.left_name.uuid},
                                                            core::dbname_t{names.left_name.dbname},
                                                            core::relname_t{names.left_name.relname});
                    if (!names.left_alias.empty()) {
                        agg->set_result_alias(names.left_alias);
                    }
                }
            } else if (nodeTag(from_first) == T_JoinExpr) {
                // from table_1 join table_2 on cond
                agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                join_dfs(resource_, pg_ptr_cast<JoinExpr>(from_first), join, names, plan);
                agg->append_child(join);
            } else if (nodeTag(from_first) == T_RangeFunction) {
                agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                auto range_func = *pg_ptr_cast<RangeFunction>(from_first);
                names.left_alias = construct_alias(range_func.alias);
                agg->append_child(transform_function(range_func, names, plan->parameters.get()));
            } else if (nodeTag(from_first) == T_RangeSubselect) {
                auto* sub_select = pg_ptr_cast<RangeSubselect>(from_first);
                agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                agg->append_child(transform_select(*pg_ptr_cast<SelectStmt>(sub_select->subquery), plan));

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
            agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
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

        auto group =
            logical_plan::make_node_group(resource_, core::dbname_t{agg->dbname()}, core::relname_t{agg->relname()});
        auto select_node =
            logical_plan::make_node_select(resource_, core::dbname_t{agg->dbname()}, core::relname_t{agg->relname()});

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
                                    args.emplace_back(transform_a_expr_arithmetic(sub, names, plan->parameters.get()));
                                } else {
                                    args.emplace_back(add_param_value(arg_value, plan->parameters.get()));
                                }
                            } else if (nodeTag(arg_value) == T_FuncCall) {
                                args.emplace_back(transform_a_expr_func(pg_ptr_cast<FuncCall>(arg_value),
                                                                        names,
                                                                        plan->parameters.get()));
                            } else if (nodeTag(arg_value) == T_CaseExpr) {
                                // CASE WHEN ... inside aggregate arg (SUM(CASE WHEN ...))
                                args.emplace_back(case_expr_to_scalar(pg_ptr_cast<CaseExpr>(arg_value),
                                                                      nullptr,
                                                                      names,
                                                                      plan,
                                                                      select_node));
                            } else {
                                args.emplace_back(add_param_value(arg_value, plan->parameters.get()));
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
                            if (nodeTag(col_ref->fields->lst.back().data) == T_A_Star && !col.table.empty()) {
                                // Carry the table qualifier so validator can expand t.x.* by result_alias.
                                std::pmr::vector<std::pmr::string> star_path{resource_};
                                star_path.emplace_back(std::pmr::string{col.table, resource_});
                                star_path.emplace_back(std::pmr::string{"*", resource_});
                                select_node->append_expression(
                                    make_scalar_expression(resource_,
                                                           scalar_type::star_expand,
                                                           expressions::key_t{std::move(star_path)}));
                                break;
                            }
                            if (res->name) {
                                // Carry side forward so validate_key doesn't fall back to LEFT on same_schema JOIN.
                                expressions::key_t out_key{resource_, res->name};
                                out_key.set_side(col.field.side());
                                select_node->append_expression(make_scalar_expression(resource_,
                                                                                      scalar_type::get_field,
                                                                                      std::move(out_key),
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
                        expr->append_param(add_param_value(res->val, plan->parameters.get()));
                        select_node->append_expression(expr);
                        break;
                    }
                    case T_TypeCast: {
                        auto cast = pg_ptr_cast<TypeCast>(res->val);
                        if (cast->arg && nodeTag(cast->arg) == T_ColumnRef) {
                            auto target_type_res = get_type(resource_, cast->typeName);
                            if (target_type_res.has_error()) {
                                error_ = target_type_res.error();
                                break;
                            }
                            auto col_ref = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(cast->arg), names);
                            auto field_name = std::string(col_ref.field.storage().back());
                            col_ref.field.set_cast_type(target_type_res.value());
                            if (cast->variant_select) {
                                col_ref.field.set_variant_select(true);
                            }
                            std::string alias = res->name ? res->name : field_name;
                            has_non_star = true;
                            select_node->append_expression(make_scalar_expression(resource_,
                                                                                  scalar_type::get_field,
                                                                                  expressions::key_t{resource_, alias},
                                                                                  std::move(col_ref.field)));
                            break;
                        }
                        // '<jsonb nav chain> ::? type' — e.g. `m -> 'a' ->> 'b' ::? string`.
                        // Resolve the chain to its flattened key, then attach the
                        // type so find_types picks the matching multi-type variant.
                        if (cast->arg && nodeTag(cast->arg) == T_A_Expr) {
                            auto* sub = pg_ptr_cast<A_Expr>(cast->arg);
                            if (sub->kind == AEXPR_OP && sub->name &&
                                nodeTag(sub->name->lst.front().data) == T_String &&
                                is_jsonb_nav_operator(strVal(sub->name->lst.front().data))) {
                                auto target_type_res = get_type(resource_, cast->typeName);
                                if (target_type_res.has_error()) {
                                    error_ = target_type_res.error();
                                    break;
                                }
                                expressions::key_t field_key{resource_};
                                if (!resolve_jsonb_scalar_key(sub, names, field_key)) {
                                    return nullptr;
                                }
                                field_key.set_cast_type(target_type_res.value());
                                if (cast->variant_select) {
                                    field_key.set_variant_select(true);
                                }
                                std::string alias = res->name ? res->name : std::string(field_key.storage().back());
                                has_non_star = true;
                                select_node->append_expression(
                                    make_scalar_expression(resource_,
                                                           scalar_type::get_field,
                                                           expressions::key_t{resource_, alias},
                                                           std::move(field_key)));
                                break;
                            }
                        }
                        [[fallthrough]];
                    }
                    case T_A_Const: {
                        has_non_star = true;
                        auto expr = make_scalar_expression(resource_,
                                                           scalar_type::constant,
                                                           res->name ? expressions::key_t{resource_, res->name}
                                                                     : expressions::key_t{resource_});
                        expr->append_param(add_param_value(res->val, plan->parameters.get()));
                        select_node->append_expression(expr);
                        break;
                    }
                    case T_A_Expr: {
                        auto a_expr = pg_ptr_cast<A_Expr>(res->val);
                        if (a_expr->kind == AEXPR_OP) {
                            auto op_str = std::string_view(strVal(a_expr->name->lst.front().data));
                            // JSONB delete: '#-' always; '-' only when the left side is
                            // the table itself (document root) — otherwise it is plain
                            // arithmetic subtraction. a_expr->lexpr is null for unary
                            // minus ('-x'), so guard before probing it.
                            if (op_str == "#-" ||
                                (op_str == "-" && a_expr->lexpr && jsonb_lhs_is_table(a_expr->lexpr, names))) {
                                has_non_star = true;
                                expressions::key_t prefix_key{resource_};
                                if (!resolve_jsonb_prefix_key(a_expr, names, prefix_key)) {
                                    return nullptr;
                                }
                                select_node->append_expression(
                                    make_scalar_expression(resource_, scalar_type::jsonb_delete, prefix_key));
                                break;
                            }
                            if (is_arithmetic_operator(op_str)) {
                                has_non_star = true;
                                logical_plan::node_ptr sel_node = select_node;
                                transform_select_a_expr(a_expr, res->name, names, plan, sel_node);
                                break;
                            }
                            if (is_jsonb_nav_operator(op_str)) {
                                has_non_star = true;
                                if (jsonb_nav_returns_scalar(op_str)) {
                                    // Scalar jsonb navigation (->> / #>>) collapses to a
                                    // get_field on the flattened slash-joined column key.
                                    expressions::key_t field_key{resource_};
                                    if (!resolve_jsonb_scalar_key(a_expr, names, field_key)) {
                                        return nullptr;
                                    }
                                    if (res->name) {
                                        select_node->append_expression(
                                            make_scalar_expression(resource_,
                                                                   scalar_type::get_field,
                                                                   expressions::key_t{resource_, res->name},
                                                                   field_key));
                                    } else {
                                        select_node->append_expression(
                                            make_scalar_expression(resource_, scalar_type::get_field, field_key));
                                    }
                                } else {
                                    // Table-valued navigation (-> / #>): expand the subtree
                                    // under the prefix into its (rerooted) columns.
                                    expressions::key_t prefix_key{resource_};
                                    if (!resolve_jsonb_prefix_key(a_expr, names, prefix_key)) {
                                        return nullptr;
                                    }
                                    select_node->append_expression(
                                        make_scalar_expression(resource_, scalar_type::jsonb_expand, prefix_key));
                                }
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
                                auto* cref = pg_ptr_cast<ColumnRef>(indirection->arg);
                                // (table_alias.struct_col).* needs schema-aware struct expansion;
                                // not supported — surface explicitly instead of silent miswiring.
                                if (cref->fields->lst.size() > 1 && !path.empty() && path.front() == "*") {
                                    error_ = core::error_t(
                                        core::error_code_t::unimplemented_yet,
                                        std::pmr::string{"struct field wildcard (alias.struct).* not supported",
                                                         resource_});
                                    return nullptr;
                                }
                                path.emplace_back(pmrStrVal(cref->fields->lst.back().data, resource_));
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
                        transform_select_case_expr(pg_ptr_cast<CaseExpr>(res->val), res->name, names, plan, sel_node);
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
                                expr->append_param(add_param_value(arg_node, plan->parameters.get()));
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

            // If select_node holds exactly one bare star_expand (pure SELECT *), treat as passthrough.
            // Qualified star (SELECT t.x.*) carries an alias key and must reach the validator's
            // pre-expand loop to be filtered by result_alias.
            auto& sel_exprs = select_node->expressions();
            if (sel_exprs.size() == 1 && sel_exprs[0]->group() == expression_group::scalar) {
                auto* s = static_cast<const scalar_expression_t*>(sel_exprs[0].get());
                if (s->type() == scalar_type::star_expand && s->key().storage().empty()) {
                    sel_exprs.clear();
                    has_non_star = false;
                }
            }
        }

        // where
        if (node.whereClause) {
            expression_ptr expr;
            if (nodeTag(node.whereClause) == T_FuncCall) {
                expr = transform_a_expr_func(pg_ptr_cast<FuncCall>(node.whereClause), names, plan->parameters.get());
            } else if (nodeTag(node.whereClause) == T_NullTest) {
                expr = transform_null_test(pg_ptr_cast<NullTest>(node.whereClause), names, plan->parameters.get());
            } else if (nodeTag(node.whereClause) == T_SubLink) {
                expr = transform_sublink_expr(pg_ptr_cast<SubLink>(node.whereClause), names, plan);
            } else {
                expr = transform_a_expr(pg_ptr_cast<A_Expr>(node.whereClause), names, plan);
            }
            if (expr) {
                agg->append_child(logical_plan::make_node_match(resource_,
                                                                core::dbname_t{agg->dbname()},
                                                                core::relname_t{agg->relname()},
                                                                expr));
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
            having_expr = transform_having_expr(node.havingClause, names, plan, group);
        }

        if (!group->expressions().empty()) {
            if (having_expr) {
                auto final_group = logical_plan::make_node_group(resource_,
                                                                 core::dbname_t{agg->dbname()},
                                                                 core::relname_t{agg->relname()},
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
                    computed_sort->append_param(resolve_select_operand(a_expr->lexpr, names, plan, dummy_node));
                    if (a_expr->rexpr) {
                        computed_sort->append_param(resolve_select_operand(a_expr->rexpr, names, plan, dummy_node));
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
            agg->append_child(logical_plan::make_node_sort(resource_,
                                                           core::dbname_t{agg->dbname()},
                                                           core::relname_t{agg->relname()},
                                                           sort_exprs));
        }

        // Append select_node as a child of agg (only if there are actual SELECT columns — not pure star)
        if (has_non_star) {
            agg->append_child(select_node);
        }

        // limit / offset
        if (node.limitCount || node.limitOffset) {
            int64_t limit_val = logical_plan::limit_t::unlimit().limit();
            int64_t offset_val = 0;
            std::optional<core::parameter_id_t> limit_param;
            std::optional<core::parameter_id_t> offset_param;

            if (node.limitCount) {
                switch (nodeTag(node.limitCount)) {
                    case T_A_Const: {
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
                                    std::pmr::string{
                                        "Forbidden expression in limit clause: allowed only LIMIT <integer>/ALL",
                                        resource_});
                                return nullptr;
                        }
                        break;
                    }
                    case T_ParamRef:
                        limit_param = add_param_value(node.limitCount, plan->parameters.get());
                        break;
                    default:
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"Unknown node type in limit clause: " +
                                                                    node_tag_to_string(nodeTag(node.limitCount)),
                                                                resource_});
                        return nullptr;
                }
            }

            if (node.limitOffset) {
                switch (nodeTag(node.limitOffset)) {
                    case T_A_Const: {
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
                                    std::pmr::string{
                                        "Forbidden expression in offset clause: allowed only OFFSET <integer>",
                                        resource_});
                                return nullptr;
                        }
                        break;
                    }
                    case T_ParamRef:
                        offset_param = add_param_value(node.limitOffset, plan->parameters.get());
                        break;
                    default:
                        error_ = core::error_t(core::error_code_t::sql_parse_error,
                                               std::pmr::string{"Unknown node type in offset clause: " +
                                                                    node_tag_to_string(nodeTag(node.limitOffset)),
                                                                resource_});
                        return nullptr;
                }
            }

            auto limit_node = logical_plan::make_node_limit(resource_,
                                                            core::dbname_t{agg->dbname()},
                                                            core::relname_t{agg->relname()},
                                                            logical_plan::limit_t(limit_val, offset_val));
            if (limit_param || offset_param) {
                deferred_limits_.push_back(deferred_limit_t{limit_node.get(), limit_param, offset_param});
            }
            agg->append_child(std::move(limit_node));
        }

        return agg;
    }
} // namespace components::sql::transform

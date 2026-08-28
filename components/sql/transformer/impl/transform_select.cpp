#include <algorithm>
#include <unordered_set>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/jsonb_path.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_cte_scan.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
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

    namespace {
        expressions::sort_null_order map_sortby_nulls(SortByNulls nulls) {
            switch (nulls) {
                case SORTBY_NULLS_FIRST:
                    return expressions::sort_null_order::nulls_first;
                case SORTBY_NULLS_LAST:
                    return expressions::sort_null_order::nulls_last;
                default:
                    return expressions::sort_null_order::nulls_default;
            }
        }

        expressions::expression_ptr using_predicate(std::pmr::memory_resource* resource, PGList* using_clause) {
            auto equate = [resource](const char* column) {
                return make_compare_expression(resource,
                                               compare_type::eq,
                                               expressions::key_t{resource, column, expressions::side_t::left},
                                               expressions::key_t{resource, column, expressions::side_t::right});
            };
            if (using_clause->lst.size() == 1) {
                return equate(strVal(using_clause->lst.front().data));
            }
            auto conjunction = make_compare_union_expression(resource, compare_type::union_and);
            for (const auto& column : using_clause->lst) {
                conjunction->append_child(equate(strVal(column.data)));
            }
            return conjunction;
        }

        bool has_using_join(Node* item) {
            if (!item || nodeTag(item) != T_JoinExpr) {
                return false;
            }
            auto* join = pg_ptr_cast<JoinExpr>(item);
            if (join->usingClause && !join->usingClause->lst.empty()) {
                return true;
            }
            return has_using_join(join->larg) || has_using_join(join->rarg);
        }

        // The visible name of a table function that carries no alias is the
        // function's own name.
        std::string range_function_name(RangeFunction& node) {
            if (!node.functions || node.functions->lst.empty()) {
                return {};
            }
            auto* list = pg_ptr_cast<List>(node.functions->lst.front().data);
            if (!list || list->lst.empty()) {
                return {};
            }
            auto* call = pg_ptr_cast<FuncCall>(list->lst.front().data);
            if (!call || !call->funcname || call->funcname->lst.empty()) {
                return {};
            }
            return strVal(call->funcname->lst.front().data);
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_aggregate_ptr>
    transformer::build_recursive_cte_ref(const std::string& cte_name,
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
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"recursive CTE not found: " + cte_name, resource_});
            }
            SelectStmt* union_stmt = cte_it->second;
            VALUE_OR_RETURN(auto anchor_plan, transform_select(*union_stmt->larg, plan));
            transforming_recursive_member_ = true;
            auto recursive_plan = transform_select(*union_stmt->rarg, plan);
            transforming_recursive_member_ = false;
            if (recursive_plan.has_error()) {
                return recursive_plan.error();
            }
            auto recursive_cte = logical_plan::make_node_recursive_cte(resource_,
                                                                       std::pmr::string{cte_name, resource_},
                                                                       union_stmt->all,
                                                                       std::move(anchor_plan),
                                                                       std::move(recursive_plan.value()));
            recursive_cte->set_result_alias(effective_alias);
            agg->append_child(std::move(recursive_cte));
        }
        return agg;
    }

    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_from_element(Node* item,
                                        qualified_name& slot_name,
                                        std::string& slot_alias,
                                        name_collection_t& names,
                                        logical_plan::node_join_ptr& node_join,
                                        logical_plan::execution_plan_t* plan) {
        slot_name = qualified_name{};
        slot_alias.clear();
        switch (nodeTag(item)) {
            case T_RangeVar: {
                auto* table = pg_ptr_cast<RangeVar>(item);
                auto written = rangevar_to_qualified_name(table);
                slot_alias = construct_alias(table->alias);
                const std::string& visible = slot_alias.empty() ? written.relname : slot_alias;
                // CTE name is a single segment and takes no qualification,
                // so a qualified item ALWAYS names a table
                const bool unqualified = written.dbname.empty() && written.schemaname.empty() && written.uuid.empty();
                if (unqualified) {
                    if (auto cte = cte_queries_.find(written.relname); cte != cte_queries_.end()) {
                        slot_name.relname = written.relname;
                        auto agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                        VALUE_OR_RETURN(auto body, transform_select(*cte->second, plan));
                        agg->append_child(std::move(body));
                        agg->children().back()->set_result_alias(visible);
                        return agg;
                    }
                    if (recursive_cte_queries_.count(written.relname)) {
                        slot_name.relname = written.relname;
                        VALUE_OR_RETURN(auto agg, build_recursive_cte_ref(written.relname, visible, plan));
                        return agg;
                    }
                }
                slot_name = std::move(written);
                auto agg = logical_plan::make_node_aggregate(resource_,
                                                             core::uid_t{slot_name.uuid},
                                                             core::dbname_t{slot_name.dbname},
                                                             core::relname_t{slot_name.relname});
                if (!slot_alias.empty()) {
                    agg->set_result_alias(slot_alias);
                }
                return agg;
            }
            case T_RangeSubselect: {
                auto* sub = pg_ptr_cast<RangeSubselect>(item);
                // A derived table fills no slot, its alias is its whole identity
                slot_alias = construct_alias(sub->alias);
                auto agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                if (sub->lateral && node_join) {
                    node_join->set_lateral(true);
                    auto* prev_outer = lateral_outer_names_;
                    auto* prev_join = lateral_join_;
                    auto* prev_plan = lateral_plan_;
                    auto prev_map = std::move(lateral_correlation_map_);
                    lateral_correlation_map_.clear();
                    lateral_outer_names_ = &names;
                    lateral_join_ = node_join.get();
                    lateral_plan_ = plan;
                    auto body = transform_select(*pg_ptr_cast<SelectStmt>(sub->subquery), plan);
                    lateral_outer_names_ = prev_outer;
                    lateral_join_ = prev_join;
                    lateral_plan_ = prev_plan;
                    lateral_correlation_map_ = std::move(prev_map);
                    if (body.has_error()) {
                        return body.error();
                    }
                    agg->append_child(std::move(body.value()));
                } else {
                    VALUE_OR_RETURN(auto body, transform_select(*pg_ptr_cast<SelectStmt>(sub->subquery), plan));
                    agg->append_child(std::move(body));
                }

                if (sub->alias) {
                    agg->children().back()->set_result_alias(sub->alias->aliasname);
                    if (sub->alias->colnames && agg->children().back()->type() == logical_plan::node_type::data_t) {
                        auto* data_node = reinterpret_cast<logical_plan::node_data_t*>(agg->children().back().get());
                        if (sub->alias->colnames->lst.size() != data_node->data_chunk().column_count()) {
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"column names count has to equal actual column count", resource_});
                        }
                        // All chunks share the same column shape; alias every chunk's columns.
                        for (auto& chunk : data_node->chunks()) {
                            size_t column_index = 0;
                            for (auto colname : sub->alias->colnames->lst) {
                                chunk.data[column_index].set_type_alias(strVal(colname.data));
                                column_index++;
                            }
                        }
                    }
                }
                return agg;
            }
            case T_RangeFunction: {
                auto* func = pg_ptr_cast<RangeFunction>(item);
                slot_alias = construct_alias(func->alias);
                if (slot_alias.empty()) {
                    slot_name.relname = range_function_name(*func);
                }
                VALUE_OR_RETURN(auto element,
                                node_join ? transform_from_function(*func, names, node_join, plan)
                                          : transform_function(*func, names, plan->parameters.get()));
                if (element && !slot_alias.empty()) {
                    element->set_result_alias(slot_alias);
                }
                return element;
            }
            default:
                return core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"unsupported FROM element " + node_tag_to_string(nodeTag(item)), resource_});
        }
    }

    core::error_t transformer::join_dfs(std::pmr::memory_resource* resource,
                                        JoinExpr* join,
                                        logical_plan::node_join_ptr& node_join,
                                        name_collection_t& names,
                                        logical_plan::execution_plan_t* plan) {
        if (join->isNatural) {
            // TODO: NATURAL needs the column lists of both sides to work out what it joins on
            // for now transformer has no schemas
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"NATURAL JOIN is not supported: it needs the column lists of "
                                                  "both sides. Name the columns with USING, or write the ON clause",
                                                  resource_});
        }
        const auto j_type = jointype_to_ql(join);
        if (j_type == logical_plan::join_type::invalid) {
            return core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{"invalid join type", resource_});
        }

        if (nodeTag(join->larg) == T_JoinExpr) {
            name_collection_t inner;
            RETURN_IF_ERROR(join_dfs(resource, pg_ptr_cast<JoinExpr>(join->larg), node_join, inner, plan));
            // Snapshot the inner join's visible scope before this level records
            // its own right side. Name and alias travel together: an alias hides
            // the relation name of its own element, nobody else's.
            auto carry = [&](const qualified_name& nm, const std::string& alias) {
                if (!nm.relname.empty() || !alias.empty()) {
                    names.extra_left.push_back({nm, alias});
                }
            };
            carry(inner.left_name, inner.left_alias);
            carry(inner.right_name, inner.right_alias);
            for (const auto& element : inner.extra_left) {
                carry(element.name, element.alias);
            }

            auto prev = node_join;
            node_join = logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, j_type);
            node_join->append_child(prev);
        } else {
            assert(!node_join);
            node_join = logical_plan::make_node_join(resource, core::dbname_t{}, core::relname_t{}, j_type);
            VALUE_OR_RETURN(
                auto left,
                transform_from_element(join->larg, names.left_name, names.left_alias, names, node_join, plan));
            if (!left) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"left side of a join lowered to an empty plan", resource});
            }
            node_join->append_child(left);
        }

        VALUE_OR_RETURN(
            auto right,
            transform_from_element(join->rarg, names.right_name, names.right_alias, names, node_join, plan));
        if (!right) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"right side of a join lowered to an empty plan", resource});
        }
        node_join->append_child(right);

        // on
        if (join->quals) {
            VALUE_OR_RETURN(auto expr, transform_predicate(join->quals, names, plan));
            node_join->append_expression(expr);
        } else if (join->usingClause && !join->usingClause->lst.empty()) {
            node_join->append_expression(using_predicate(resource, join->usingClause));
            for (const auto& column : join->usingClause->lst) {
                names.using_columns.push_back({strVal(column.data), jointype_to_ql(join)});
            }
        } else {
            node_join->append_expression(make_compare_expression(resource, compare_type::all_true));
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<logical_plan::node_aggregate_ptr>
    transformer::transform_from_source(List* from_items,
                                       name_collection_t& names,
                                       logical_plan::execution_plan_t* plan) {
        logical_plan::node_aggregate_ptr agg = nullptr;
        logical_plan::node_join_ptr join = nullptr;

        // SQL-89 comma-join: `FROM a, b [, c ...] WHERE a.x = b.y` arrives as a
        // from_items list with multiple top-level entries. libpg_query does NOT
        // synthesize a FromExpr / JoinExpr in that case — each table is a bare
        // T_RangeVar (or T_RangeFunction / T_RangeSubselect) sibling.
        //
        // The downstream pipeline only knows how to consume a single join root, so
        // we synthesize a left-deep JoinExpr tree here with jointype=JOIN_INNER and
        // quals=NULL on every link. jointype_to_ql promotes (JOIN_INNER, quals=NULL)
        // -> join_type::cross, which produces the cross-product. Inner-join semantics
        // are recovered by the user's WHERE clause, lowered into a sibling match_t on
        // the aggregate root; that match_t evaluates against the post-join merged
        // chunk, so column refs resolve through the join's merged schema regardless of
        // side_t.
        //
        // The synthesized tree mutates `from_items->lst.front()` so the existing
        // T_JoinExpr branch below picks it up unchanged.

        if (from_items->lst.size() > 1) {
            auto it = from_items->lst.begin();
            Node* acc = pg_ptr_cast<Node>(it->data);
            ++it;
            for (; it != from_items->lst.end(); ++it) {
                auto* rhs = pg_ptr_cast<Node>(it->data);
                JoinExpr* synth = makeNode(resource_, JoinExpr);
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
            // Replace the original multi-entry list with a single top-level JoinExpr
            // so the dispatch below sees T_JoinExpr.
            from_items->lst.clear();
            from_items->lst.push_back({acc});
        }

        auto from_first = from_items->lst.front().data;
        if (nodeTag(from_first) == T_JoinExpr) {
            // from table_1 join table_2 on cond
            agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
            RETURN_IF_ERROR(join_dfs(resource_, pg_ptr_cast<JoinExpr>(from_first), join, names, plan));
            RETURN_IF_ERROR(names.refuse_indistinguishable_elements(resource_));
            agg->append_child(join);
        } else {
            // A single FROM element goes through the same lowering as one inside
            // a join, so a table, a CTE reference, a derived table and a table
            // function are registered the same way in both places.
            logical_plan::node_join_ptr no_join;
            VALUE_OR_RETURN(auto element,
                            transform_from_element(pg_ptr_cast<Node>(from_first),
                                                   names.left_name,
                                                   names.left_alias,
                                                   names,
                                                   no_join,
                                                   plan));
            if (!element) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"FROM element lowered to an empty plan", resource_});
            }
            if (element->type() == logical_plan::node_type::aggregate_t) {
                agg = logical_plan::node_aggregate_ptr(static_cast<logical_plan::node_aggregate_t*>(element.get()));
            } else {
                agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                agg->append_child(element);
            }
        }
        return agg;
    }

    core::error_t transformer::register_with_ctes(WithClause* with_clause) {
        if (!with_clause) {
            return core::error_t::no_error();
        }
        for (const auto& item : with_clause->ctes->lst) {
            auto* cte = pg_ptr_cast<CommonTableExpr>(item.data);
            if (nodeTag(cte->ctequery) != T_SelectStmt) {
                // WITH x AS (DELETE/UPDATE/INSERT ... RETURNING ...) — a data-modifying CTE. Deferred:
                // reject cleanly instead of a bad SelectStmt cast.
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"data-modifying WITH (CTE) is not supported", resource_});
            }
            if (with_clause->recursive) {
                recursive_cte_queries_.emplace(cte->ctename, pg_ptr_cast<SelectStmt>(cte->ctequery));
            } else {
                cte_queries_.emplace(cte->ctename, pg_ptr_cast<SelectStmt>(cte->ctequery));
            }
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::build_limit_node(Node* limit_count,
                                                                                 Node* limit_offset,
                                                                                 const core::dbname_t& db,
                                                                                 const core::relname_t& rel,
                                                                                 logical_plan::execution_plan_t* plan) {
        if (!limit_count && !limit_offset) {
            return nullptr;
        }
        int64_t limit_val = logical_plan::limit_t::unlimit().limit();
        int64_t offset_val = 0;
        std::optional<core::parameter_id_t> limit_param;
        std::optional<core::parameter_id_t> offset_param;

        if (limit_count) {
            switch (nodeTag(limit_count)) {
                case T_A_Const: {
                    auto* value = &(pg_ptr_cast<A_Const>(limit_count)->val);
                    switch (nodeTag(value)) {
                        case T_Null:
                            break; // LIMIT ALL — keep unlimit_
                        case T_Integer:
                            limit_val = intVal(value);
                            break;
                        default:
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{
                                    "Forbidden expression in limit clause: allowed only LIMIT <integer>/ALL",
                                    resource_});
                    }
                    break;
                }
                case T_ParamRef: {
                    VALUE_OR_RETURN(auto param, add_param_value(limit_count, plan->parameters.get()));
                    limit_param = param;
                    break;
                }
                default:
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"Unknown node type in limit clause: " +
                                                              node_tag_to_string(nodeTag(limit_count)),
                                                          resource_});
            }
        }

        if (limit_offset) {
            switch (nodeTag(limit_offset)) {
                case T_A_Const: {
                    auto* value = &(pg_ptr_cast<A_Const>(limit_offset)->val);
                    switch (nodeTag(value)) {
                        case T_Null:
                            break; // OFFSET NULL — treat as 0
                        case T_Integer:
                            offset_val = intVal(value);
                            break;
                        default:
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"Forbidden expression in offset clause: allowed only OFFSET <integer>",
                                                 resource_});
                    }
                    break;
                }
                case T_ParamRef: {
                    VALUE_OR_RETURN(auto param, add_param_value(limit_offset, plan->parameters.get()));
                    offset_param = param;
                    break;
                }
                default:
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"Unknown node type in offset clause: " +
                                                              node_tag_to_string(nodeTag(limit_offset)),
                                                          resource_});
            }
        }

        auto limit_node =
            logical_plan::make_node_limit(resource_, db, rel, logical_plan::limit_t(limit_val, offset_val));
        if (limit_param || offset_param) {
            deferred_limits_.push_back(deferred_limit_t{limit_node.get(), limit_param, offset_param});
        }
        return limit_node;
    }

    core::result_wrapper_t<logical_plan::node_limit_ptr>
    transformer::build_dml_limit(Node* limit_count,
                                 const core::dbname_t& db,
                                 const core::relname_t& rel,
                                 logical_plan::execution_plan_t* plan) {
        if (!limit_count) {
            return logical_plan::make_node_limit(resource_, db, rel, logical_plan::limit_t::unlimit());
        }
        // DML has no OFFSET (grammar-enforced): pass a null offset. build_limit_node validates the
        // count (integer / bound parameter) and defers a ParamRef exactly like a SELECT limit; an
        // invalid expression comes back as a refusal.
        VALUE_OR_RETURN(auto built, build_limit_node(limit_count, nullptr, db, rel, plan));
        if (!built) {
            return logical_plan::node_limit_ptr{nullptr};
        }
        // build_limit_node always constructs a node_limit_t — downcast the base node_ptr.
        return logical_plan::node_limit_ptr{static_cast<logical_plan::node_limit_t*>(built.get())};
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_select(SelectStmt& node,
                                                                                 logical_plan::execution_plan_t* plan) {
        // Set operations (UNION / INTERSECT / EXCEPT) are not yet wired
        // through the transformer. For a SETOP_* node, node.targetList is
        // null (the column projection lives on the larg / rarg children),
        // so the for-loop below would dereference null and SIGSEGV. Bail
        // out cleanly until proper set-operation lowering lands.
        // dynamic_schema_union sits on this path; lldb pinned the crash to
        // node.targetList->lst at line 137 here.
        // Resolve a positional `ORDER BY <n>` (1-based) to the n-th output column's field:
        // `n` indexes `target_list` (the SELECT list; for a UNION the output names come from
        // the FIRST arm's list, PostgreSQL semantics). Refuses an out-of-range position, or a
        // computed column with no alias to name it; `out` is filled in place on success.
        auto positional_sort_field =
            [&](List* target_list, int64_t n, const name_collection_t& nm, column_ref_t& out) -> core::error_t {
            int64_t count = 0;
            ResTarget* res = nullptr;
            if (target_list) {
                for (auto t : target_list->lst) {
                    if (++count == n) {
                        res = pg_ptr_cast<ResTarget>(t.data);
                        break;
                    }
                }
            }
            if (res == nullptr) {
                return core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"ORDER BY position is out of range of the select list", resource_});
            }
            if (nodeTag(res->val) == T_ColumnRef) {
                VALUE_OR_RETURN(out, columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(res->val), nm));
                return core::error_t::no_error();
            }
            if (res->name) {
                out.field = expressions::key_t{resource_, res->name};
                return core::error_t::no_error();
            }
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"positional ORDER BY over a computed column requires an alias", resource_});
        };

        if (node.op == SETOP_UNION) {
            // gram.y attaches withClause / sortClause / limitCount / limitOffset / distinctClause to THIS
            // compound node (not to larg/rarg). The old early-return dropped all of them silently.
            // WITH must be registered BEFORE the arms so both can see the CTEs.
            RETURN_IF_ERROR(register_with_ctes(node.withClause));
            VALUE_OR_RETURN(auto left, transform_select(*node.larg, plan));
            VALUE_OR_RETURN(auto right, transform_select(*node.rarg, plan));
            logical_plan::node_ptr union_node =
                logical_plan::make_node_union(resource_, std::move(left), std::move(right), node.all);

            const bool has_sort = node.sortClause && !node.sortClause->lst.empty();
            const bool has_distinct = node.distinctClause && !node.distinctClause->lst.empty();
            if (!has_sort && !node.limitCount && !node.limitOffset && !has_distinct) {
                return union_node; // bare union — no tail clauses to apply
            }

            // Wrap the union in an aggregate so the existing sort / limit / distinct children apply:
            // create_plan_aggregate lowers a non-scan source through its default child_op branch
            // (union -> sort -> limit / distinct).
            auto agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
            agg->append_child(std::move(union_node));
            if (has_distinct) {
                // v1: DISTINCT ON over a compound/UNION query is not supported (plain DISTINCT is).
                // Plain DISTINCT is the NIL List sentinel; a real ON expression is anything else.
                if (nodeTag(node.distinctClause->lst.front().data) != T_List) {
                    return core::error_t(
                        core::error_code_t::unimplemented_yet,
                        std::pmr::string{"DISTINCT ON is not supported over a UNION query", resource_});
                }
                agg->set_distinct(true);
            }
            if (has_sort) {
                // Union output columns resolve by NAME at validation, so an empty name scope is fine.
                name_collection_t union_names;
                std::vector<expression_ptr> sort_exprs;
                sort_exprs.reserve(node.sortClause->lst.size());
                for (auto sort_it : node.sortClause->lst) {
                    auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                    bool is_desc = sortby->sortby_dir == SORTBY_DESC;
                    column_ref_t field(resource_);
                    if (nodeTag(sortby->node) == T_ColumnRef) {
                        VALUE_OR_RETURN(
                            auto resolved,
                            columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(sortby->node), union_names));
                        field = std::move(resolved);
                    } else if (nodeTag(sortby->node) == T_A_Indirection) {
                        VALUE_OR_RETURN(
                            field,
                            indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(sortby->node), union_names));
                    } else if (nodeTag(sortby->node) == T_A_Const) {
                        // Positional `ORDER BY <n>`: map to the n-th UNION output column (the
                        // output names come from the first arm, node.larg's select list).
                        auto* value = &(pg_ptr_cast<A_Const>(sortby->node)->val);
                        if (nodeTag(value) != T_Integer) {
                            return core::error_t(core::error_code_t::sql_parse_error,
                                                 std::pmr::string{"non-integer constant in ORDER BY", resource_});
                        }
                        List* out_list = node.larg ? node.larg->targetList : nullptr;
                        RETURN_IF_ERROR(positional_sort_field(out_list, intVal(value), union_names, field));
                    } else {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"ORDER BY over UNION supports only column references", resource_});
                    }
                    sort_exprs.emplace_back(make_sort_expression(field.field,
                                                                 is_desc ? sort_order::desc : sort_order::asc,
                                                                 map_sortby_nulls(sortby->sortby_nulls)));
                }
                agg->append_child(
                    logical_plan::make_node_sort(resource_, core::dbname_t{}, core::relname_t{}, sort_exprs));
            }
            VALUE_OR_RETURN(
                auto limit_node,
                build_limit_node(node.limitCount, node.limitOffset, core::dbname_t{}, core::relname_t{}, plan));
            if (limit_node) {
                agg->append_child(std::move(limit_node));
            }
            return agg;
        }
        if (node.op != SETOP_NONE || (node.targetList == nullptr && node.valuesLists == nullptr)) {
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{
                    "SELECT set operations (INTERSECT / EXCEPT) are not yet supported by the SQL transformer",
                    resource_});
        }
        RETURN_IF_ERROR(register_with_ctes(node.withClause));
        logical_plan::node_aggregate_ptr agg = nullptr;
        name_collection_t names;

        if (node.fromClause && !node.fromClause->lst.empty()) {
            VALUE_OR_RETURN(agg, transform_from_source(node.fromClause, names, plan));
        } else {
            agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
        }
        if (node.valuesLists) {
            // Split the literal rows into uniform ≤CAP chunks (only the last is smaller) so
            // no oversized data_chunk_t is built.
            const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
            const uint64_t total = node.valuesLists->lst.size();
            std::pmr::vector<vector::data_chunk_t> chunks(resource_);
            auto row_it = node.valuesLists->lst.begin();
            uint64_t global_row = 0;
            while (global_row < total) {
                const uint64_t batch = std::min<uint64_t>(cap, total - global_row);
                vector::data_chunk_t chunk(resource_, {}, batch);
                chunk.set_cardinality(batch);
                for (uint64_t chunk_row = 0; chunk_row < batch; ++chunk_row, ++row_it, ++global_row) {
                    auto values = pg_ptr_cast<List>(row_it->data)->lst;
                    size_t column_index = 0;
                    for (auto it_value = values.begin(); it_value != values.end(); ++it_value, ++column_index) {
                        VALUE_OR_RETURN(auto value, get_value(resource_, pg_ptr_cast<Node>(it_value->data)));
                        if (column_index >= chunk.data.size()) {
                            chunk.data.emplace_back(resource_, value.type(), chunk.capacity());
                            // PostgreSQL names unlabeled VALUES columns column1, column2, ... —
                            // an aggregate wrapper (LIMIT/ORDER BY tail) and the result cursor
                            // read a column alias, and an untitled VALUES column would abort in
                            // complex_logical_type::alias(). Only name columns left unaliased.
                            if (!chunk.data[column_index].type().has_alias()) {
                                chunk.data[column_index].set_type_alias("column" + std::to_string(column_index + 1));
                            }
                        }
                        chunk.set_value(column_index, chunk_row, std::move(value));
                    }
                }
                chunks.emplace_back(std::move(chunk));
            }

            auto raw = logical_plan::make_node_raw_data(resource_, std::move(chunks));
            const bool values_has_sort = node.sortClause && !node.sortClause->lst.empty();
            if (!values_has_sort && !node.limitCount && !node.limitOffset) {
                return raw; // bare VALUES — no tail clauses to apply
            }
            if (values_has_sort) {
                // A top-level VALUES row has no named columns to resolve a sort key against;
                // ORDER BY over VALUES is not yet supported (LIMIT/OFFSET are). Clean error,
                // never a silently dropped ORDER BY.
                return core::error_t(
                    core::error_code_t::unimplemented_yet,
                    std::pmr::string{"ORDER BY over a top-level VALUES list is not yet supported", resource_});
            }
            // Honor VALUES … LIMIT/OFFSET: wrap in an aggregate so create_plan_aggregate lowers
            // the data source through its default (non-scan) child branch with the authoritative
            // operator_limit on top (VALUES keeps OFFSET, unlike DML).
            auto values_agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
            values_agg->append_child(std::move(raw));
            VALUE_OR_RETURN(
                auto limit_node,
                build_limit_node(node.limitCount, node.limitOffset, core::dbname_t{}, core::relname_t{}, plan));
            if (limit_node) {
                values_agg->append_child(std::move(limit_node));
            }
            return values_agg;
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
                // `SELECT +x` projects x itself: peel the identity layers so the stripped
                // node dispatches to its own arm (column, constant, expression) below.
                res->val = strip_unary_plus(res->val);
                if (!res->val) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"operator is missing its operand", resource_});
                }
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
                                VALUE_OR_RETURN(
                                    auto key,
                                    columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg_value), names));
                                args.emplace_back(std::move(key.field));
                            } else if (nodeTag(arg_value) == T_A_Expr) {
                                auto sub = pg_ptr_cast<A_Expr>(arg_value);
                                if (sub->kind == AEXPR_OP &&
                                    is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                                    VALUE_OR_RETURN(auto arith,
                                                    transform_a_expr_arithmetic(sub, names, plan->parameters.get()));
                                    args.emplace_back(std::move(arith));
                                } else {
                                    VALUE_OR_RETURN(auto param, add_param_value(arg_value, plan->parameters.get()));
                                    args.emplace_back(param);
                                }
                            } else if (nodeTag(arg_value) == T_A_Indirection) {
                                // sum(v[2]) / sum((s).f): an indirection over a column reference is
                                // still a column, not a constant — reading it as one made the whole
                                // statement fail to parse.
                                VALUE_OR_RETURN(auto key, node_to_field(resource_, arg_value, names));
                                args.emplace_back(std::move(key.field));
                            } else if (nodeTag(arg_value) == T_FuncCall) {
                                VALUE_OR_RETURN(auto call,
                                                transform_a_expr_func(pg_ptr_cast<FuncCall>(arg_value),
                                                                      names,
                                                                      plan->parameters.get()));
                                args.emplace_back(std::move(call));
                            } else if (nodeTag(arg_value) == T_CaseExpr) {
                                // CASE WHEN ... inside aggregate arg (SUM(CASE WHEN ...))
                                VALUE_OR_RETURN(auto case_expr,
                                                case_expr_to_scalar(pg_ptr_cast<CaseExpr>(arg_value),
                                                                    nullptr,
                                                                    names,
                                                                    plan,
                                                                    select_node));
                                args.emplace_back(std::move(case_expr));
                            } else {
                                VALUE_OR_RETURN(auto param, add_param_value(arg_value, plan->parameters.get()));
                                args.emplace_back(param);
                            }
                        }

                        // FILTER (WHERE p): lower to a CASE over each argument (or COUNT(CASE ...)
                        // for a bare aggregate) so only qualifying rows reach the aggregate.
                        VALUE_OR_RETURN(args, apply_aggregate_filter(func->agg_filter, std::move(args), names, plan));

                        std::string expr_name;
                        if (res->name) {
                            expr_name = res->name;
                        } else {
                            expr_name = funcname;
                        }

                        auto expr = make_function_expression(resource_, std::move(funcname), std::move(args));
                        expr->set_key(expressions::key_t{resource_, std::move(expr_name)});
                        if (func->agg_distinct) {
                            expr->set_distinct(true);
                        }
                        expr->set_star_argument(func->agg_star);
                        select_node->append_expression(expr);
                        has_non_star = true;
                        break;
                    }
                    case T_ColumnRef: {
                        auto col_ref = pg_ptr_cast<ColumnRef>(res->val);
                        // Check for star — add a star_expand marker (cleaned up below if it's the only expression)
                        if (col_ref->fields->lst.size() == 1 && nodeTag(col_ref->fields->lst.back().data) == T_A_Star) {
                            if (node.fromClause && !node.fromClause->lst.empty() &&
                                has_using_join(pg_ptr_cast<Node>(node.fromClause->lst.front().data))) {
                                return core::error_t(
                                    core::error_code_t::unimplemented_yet,
                                    std::pmr::string{"SELECT * over a USING join is not supported yet: the joined "
                                                     "column would come back twice. Name the columns you want",
                                                     resource_});
                            }
                            select_node->append_expression(make_scalar_expression(resource_,
                                                                                  scalar_type::star_expand,
                                                                                  expressions::key_t{resource_}));
                            has_non_star = true;
                            break;
                        }
                        // Correlated outer column projected inside the subquery: emit it
                        // as a constant fed by the correlation parameter. operator_select
                        // reads that parameter live per projection, so the value the
                        // lateral join rebinds per outer row is honoured (create_plan_select
                        // keeps the parameter id rather than baking the value).
                        if (auto corr = try_lateral_correlate(col_ref, names)) {
                            has_non_star = true;
                            std::string out_name =
                                res->name ? res->name : std::string(strVal(col_ref->fields->lst.back().data));
                            auto expr = make_scalar_expression(resource_,
                                                               scalar_type::constant,
                                                               expressions::key_t{resource_, out_name});
                            expr->append_param(*corr);
                            select_node->append_expression(expr);
                            break;
                        }
                        has_non_star = true;
                        {
                            VALUE_OR_RETURN(auto col, columnref_to_field(resource_, col_ref, names));
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
                        std::string field_name;
                        if (res->name) {
                            field_name = res->name;
                        } else {
                            VALUE_OR_RETURN(auto name_res, get_str_value(res->val));
                            field_name = name_res;
                        }
                        auto expr = make_scalar_expression(resource_,
                                                           scalar_type::get_field,
                                                           expressions::key_t{resource_, std::move(field_name)});
                        VALUE_OR_RETURN(auto param, add_param_value(res->val, plan->parameters.get()));
                        expr->append_param(param);
                        select_node->append_expression(expr);
                        break;
                    }
                    case T_TypeCast: {
                        auto cast = pg_ptr_cast<TypeCast>(res->val);
                        if (cast->arg && nodeTag(cast->arg) == T_ColumnRef) {
                            VALUE_OR_RETURN(auto target_type_res, get_type(resource_, cast->typeName));
                            VALUE_OR_RETURN(auto col_ref,
                                            columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(cast->arg), names));
                            auto field_name = std::string(col_ref.field.storage().back());
                            std::string alias = res->name ? res->name : field_name;
                            has_non_star = true;
                            auto conversion = make_cast_expression(resource_,
                                                                   param_storage{std::move(col_ref.field)},
                                                                   target_type_res,
                                                                   casts::cast_t{},
                                                                   cast->try_cast ? casts::cast_kind::try_cast
                                                                                  : casts::cast_kind::cast);
                            conversion->key() = expressions::key_t{resource_, alias};
                            select_node->append_expression(conversion);
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
                                VALUE_OR_RETURN(auto target_type_res, get_type(resource_, cast->typeName));
                                VALUE_OR_RETURN(auto field_key_res, resolve_jsonb_scalar_key(sub, names));
                                auto& field_key = field_key_res;
                                field_key.set_cast_type(target_type_res);
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
                        VALUE_OR_RETURN(auto param, add_param_value(res->val, plan->parameters.get()));
                        expr->append_param(param);
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
                                // '-' with a text-array operand '{a,b}' deletes several
                                // top-level keys at once (postgres `jsonb - text[]`).
                                // Each key becomes one delete prefix carried as a param;
                                // the empty array '{}' deletes nothing. Every other
                                // spelling ('- key', '#- path') is a single prefix.
                                if (op_str == "-") {
                                    VALUE_OR_RETURN(auto rhs_res, get_str_value(a_expr->rexpr));
                                    const std::string& rhs = rhs_res;
                                    if (rhs.size() >= 2 && rhs.front() == '{' && rhs.back() == '}') {
                                        expressions::side_t side = expressions::side_t::undefined;
                                        std::pmr::vector<std::pmr::string> base(resource_);
                                        RETURN_IF_ERROR(resolve_jsonb_base(a_expr->lexpr, names, base, side));
                                        if (side == expressions::side_t::undefined && names.right_name.empty() &&
                                            names.right_alias.empty()) {
                                            side = expressions::side_t::left;
                                        }
                                        auto del = make_scalar_expression(resource_, scalar_type::jsonb_delete);
                                        for (auto& k : jsonb_path::split_operand(rhs, resource_)) {
                                            std::pmr::vector<std::pmr::string> segs(base);
                                            segs.emplace_back(std::move(k));
                                            del->append_param(expressions::key_t(resource_,
                                                                                 jsonb_path::flatten(segs, resource_),
                                                                                 side));
                                        }
                                        select_node->append_expression(del);
                                        break;
                                    }
                                }
                                VALUE_OR_RETURN(auto prefix_key, resolve_jsonb_prefix_key(a_expr, names));
                                select_node->append_expression(
                                    make_scalar_expression(resource_, scalar_type::jsonb_delete, prefix_key));
                                break;
                            }
                            if (is_arithmetic_operator(op_str)) {
                                has_non_star = true;
                                logical_plan::node_ptr sel_node = select_node;
                                RETURN_IF_ERROR(transform_select_a_expr(a_expr, res->name, names, plan, sel_node));
                                break;
                            }
                            if (auto compare_op = get_compare_type(op_str); compare_op != compare_type::invalid &&
                                                                            compare_op != compare_type::regex &&
                                                                            a_expr->lexpr) {
                                has_non_star = true;
                                logical_plan::node_ptr sel_node = select_node;
                                VALUE_OR_RETURN(auto lhs, resolve_select_operand(a_expr->lexpr, names, plan, sel_node));
                                VALUE_OR_RETURN(auto rhs, resolve_select_operand(a_expr->rexpr, names, plan, sel_node));
                                auto compare = make_compare_expression(resource_, compare_op, lhs, rhs);
                                compare->set_key(
                                    expressions::key_t{resource_,
                                                       res->name ? std::string{res->name} : std::string{op_str}});
                                select_node->append_expression(compare);
                                break;
                            }
                            if (is_jsonb_nav_operator(op_str)) {
                                has_non_star = true;
                                if (jsonb_nav_returns_scalar(op_str)) {
                                    // Scalar jsonb navigation (->> / #>>) collapses to a
                                    // get_field on the flattened slash-joined column key.
                                    VALUE_OR_RETURN(auto field_key, resolve_jsonb_scalar_key(a_expr, names));
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
                                    VALUE_OR_RETURN(auto prefix_key, resolve_jsonb_prefix_key(a_expr, names));
                                    select_node->append_expression(
                                        make_scalar_expression(resource_, scalar_type::jsonb_expand, prefix_key));
                                }
                                break;
                            }
                        }

                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"Unknown A_Expr kind in field clause", resource_});
                    }
                    case T_A_Indirection: {
                        auto* indirection = pg_ptr_cast<A_Indirection>(res->val);
                        Node* base = indirection->arg;
                        while (nodeTag(base) == T_A_Indirection) {
                            base = pg_ptr_cast<A_Indirection>(base)->arg;
                        }
                        if (nodeTag(base) == T_FuncCall) {
                            // function here is an aggregate_expr and field selection is a scalar_expr
                            // TODO: proper expression chaining support
                            return core::error_t(
                                core::error_code_t::unimplemented_yet,
                                std::pmr::string{
                                    "Otterbrix does not support field selection from function results for now",
                                    resource_});
                        }
                        // (table_alias.struct_col).* needs schema-aware struct expansion;
                        // not supported — surface explicitly instead of silent miswiring.
                        if (nodeTag(indirection->indirection->lst.back().data) == T_A_Star &&
                            nodeTag(base) == T_ColumnRef && pg_ptr_cast<ColumnRef>(base)->fields->lst.size() > 1) {
                            return core::error_t(
                                core::error_code_t::unimplemented_yet,
                                std::pmr::string{"struct field wildcard (alias.struct).* not supported", resource_});
                        }
                        // The reference itself goes through the same reader as
                        // every other clause uses, so a projection and a predicate
                        // over one field cannot disagree about which field it is.
                        VALUE_OR_RETURN(auto col, node_to_field(resource_, res->val, names));
                        auto& field = col.field;
                        if (field.storage().size() == 1 && field.storage().front() == "*") {
                            break; // skip star
                        }
                        has_non_star = true;
                        select_node->append_expression(
                            make_scalar_expression(resource_, scalar_type::get_field, std::move(field)));
                        break;
                    }
                    case T_CaseExpr: {
                        has_non_star = true;
                        logical_plan::node_ptr sel_node = select_node;
                        if (auto err = transform_select_case_expr(pg_ptr_cast<CaseExpr>(res->val),
                                                                  res->name,
                                                                  names,
                                                                  plan,
                                                                  sel_node);
                            err.contains_error()) {
                            return err;
                        }
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
                        logical_plan::node_ptr coalesce_node = select_node;
                        for (auto& arg_item : coalesce->args->lst) {
                            auto arg_node = pg_ptr_cast<Node>(arg_item.data);
                            VALUE_OR_RETURN(auto arg, resolve_select_operand(arg_node, names, plan, coalesce_node));
                            expr->append_param(std::move(arg));
                        }
                        select_node->append_expression(expr);
                        break;
                    }
                    case T_SubLink: {
                        auto sub = pg_ptr_cast<SubLink>(res->val);
                        if (sub->subLinkType != EXPR_SUBLINK) {
                            // ARRAY(SELECT ...) / EXISTS(...) / other sub-link kinds projected in the SELECT
                            // list are not yet supported (deferred, tracked in #559); only a scalar
                            // EXPR_SUBLINK is handled. Report it clearly rather than as an "unknown node type".
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"unsupported subquery in the SELECT list; only a scalar subquery "
                                                 "is supported",
                                                 resource_});
                        }
                        has_non_star = true;
                        // Scalar sub-query as a projected value: flatten it into a sub-query whose single
                        // compacted result binds to a parameter, then project that parameter as a constant
                        // column (read live at execution; a NULL/0-row result is typed from the sub-query's
                        // output types). Save/restore the pending internal-aggregate stash so the
                        // inner transform's clear() does not drop this level's aggregates.
                        auto param_id =
                            plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                        auto prev_pending = std::move(pending_internal_aggs_);
                        pending_internal_aggs_.clear();
                        auto sub_node = transform(*sub->subselect, plan);
                        pending_internal_aggs_ = std::move(prev_pending);
                        if (sub_node.has_error()) {
                            return sub_node.error();
                        }
                        plan->sub_query_results.emplace_back(&vector::compact_to_single_value, param_id);
                        plan->sub_queries.emplace_back(std::move(sub_node.value()));
                        auto expr = make_scalar_expression(resource_,
                                                           scalar_type::constant,
                                                           res->name ? expressions::key_t{resource_, res->name}
                                                                     : expressions::key_t{resource_});
                        expr->append_param(param_id);
                        select_node->append_expression(expr);
                        break;
                    }
                    default:
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"Unknown node type in field clause: " +
                                                                  node_tag_to_string(nodeTag(res->val)),
                                                              resource_});
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

        // Correlated EXISTS / NOT EXISTS in WHERE -> LATERAL semi- / anti-join.
        // A sole-predicate `WHERE EXISTS (SELECT ... WHERE inner.k = outer.k)` is the
        // canonical SEMI join (emit each outer row iff the inner side has >=1 match);
        // `WHERE NOT EXISTS (...)` is the ANTI join (emit iff the inner side has none).
        // The flatten path cannot resolve an outer column, so we speculatively
        // transform the EXISTS body with lateral correlation scope active: if it
        // references an outer column (correlations captured), the outer FROM source
        // becomes the join's left child and the inner sub-plan its right child,
        // re-rooted under a fresh container aggregate. An UNCORRELATED EXISTS keeps
        // the (single-pass) flatten path. Only a correct plan can result: a
        // mis-detected correlation either errors or runs a slower-but-correct
        // per-row lateral join — never a wrong answer.
        bool where_consumed_by_semi_anti = false;
        if (node.whereClause && agg) {
            SubLink* exists_sub = nullptr;
            logical_plan::join_type semi_anti_type = logical_plan::join_type::invalid;
            if (nodeTag(node.whereClause) == T_SubLink) {
                auto* sl = pg_ptr_cast<SubLink>(node.whereClause);
                if (sl->subLinkType == EXISTS_SUBLINK) {
                    exists_sub = sl;
                    semi_anti_type = logical_plan::join_type::semi;
                }
            } else if (nodeTag(node.whereClause) == T_A_Expr) {
                // `NOT EXISTS (...)` parses as A_Expr(AEXPR_NOT, rexpr = SubLink[EXISTS]).
                auto* ae = pg_ptr_cast<A_Expr>(node.whereClause);
                if (ae->kind == AEXPR_NOT && ae->rexpr && nodeTag(ae->rexpr) == T_SubLink) {
                    auto* sl = pg_ptr_cast<SubLink>(ae->rexpr);
                    if (sl->subLinkType == EXISTS_SUBLINK) {
                        exists_sub = sl;
                        semi_anti_type = logical_plan::join_type::anti;
                    }
                }
            }
            if (exists_sub && exists_sub->subselect && nodeTag(exists_sub->subselect) == T_SelectStmt) {
                auto join =
                    logical_plan::make_node_join(resource_, core::dbname_t{}, core::relname_t{}, semi_anti_type);
                join->set_lateral(true);
                join->append_child(agg); // outer / left = the FROM source (single-table or FROM-join)

                const std::size_t saved_subq = plan->sub_queries.size();

                // Expose the outer scope so a correlated inner column lowers to a
                // correlation parameter (see try_lateral_correlate); mirrors join_dfs's
                // FROM-clause LATERAL path.
                auto* prev_outer = lateral_outer_names_;
                auto* prev_join = lateral_join_;
                auto* prev_plan = lateral_plan_;
                auto prev_map = std::move(lateral_correlation_map_);
                lateral_correlation_map_.clear();
                // The speculative inner transform must not steal this level's pending
                // internal aggregates (its epilogue flushes + clears the stash) — the
                // clobber would persist even when the speculative build is discarded.
                auto prev_pending = std::move(pending_internal_aggs_);
                pending_internal_aggs_.clear();
                lateral_outer_names_ = &names;
                lateral_join_ = join.get();
                lateral_plan_ = plan;
                auto body_res = transform_select(*pg_ptr_cast<SelectStmt>(exists_sub->subselect), plan);
                lateral_outer_names_ = prev_outer;
                lateral_join_ = prev_join;
                lateral_plan_ = prev_plan;
                lateral_correlation_map_ = std::move(prev_map);
                pending_internal_aggs_ = std::move(prev_pending);
                // The inner build is speculative only in the sense that it may be reused on either
                // branch below; a refusal from it is final either way.
                if (body_res.has_error()) {
                    return body_res.error();
                }
                auto body = std::move(body_res.value());

                // Route to the semi/anti join when the body is correlated. If it was
                // uncorrelated but its speculative transform already appended nested
                // sub-queries, keep the (correct, per-row) lateral join rather than
                // rebuild it as a flattened duplicate. Otherwise (the common
                // uncorrelated case) the ALREADY-transformed body is reused on the
                // flatten path below — one transform, no re-parse and no dead
                // parameter bindings from a discarded pass left in plan->parameters.
                const bool correlated = !join->correlations().empty();
                if (correlated || plan->sub_queries.size() != saved_subq) {
                    auto inner_agg = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    inner_agg->append_child(std::move(body));
                    join->append_child(inner_agg);
                    // ON = all_true: the inner sub-plan already filters via the bound
                    // correlation parameters, so the existence of any inner row is the match.
                    join->append_expression(make_compare_expression(resource_, compare_type::all_true));
                    auto container = logical_plan::make_node_aggregate(resource_, core::dbname_t{}, core::relname_t{});
                    container->append_child(join);
                    agg = container;
                } else {
                    // Uncorrelated EXISTS: flatten the reused body exactly like
                    // transform_sublink_expr's EXISTS_SUBLINK arm (plus the AEXPR_NOT
                    // union_not wrap for NOT EXISTS) and transform()'s T_SelectStmt
                    // resolve wrap for the sub-query's primary table.
                    auto param_true = plan->parameters->add_parameter(types::logical_value_t{resource_, true});
                    auto param_exists =
                        plan->parameters->add_parameter(types::logical_value_t{resource_, types::logical_type::NA});
                    plan->sub_query_results.emplace_back(&vector::compact_to_bool_value, param_exists);
                    if (body && body->type() == logical_plan::node_type::aggregate_t) {
                        const auto* body_agg = static_cast<const logical_plan::node_aggregate_t*>(body.get());
                        const auto& rel = static_cast<const std::string&>(body_agg->relname());
                        if (!rel.empty()) {
                            register_catalog_resolve_table(resource_,
                                                           &catalog_resolves_,
                                                           static_cast<const std::string&>(body_agg->dbname()),
                                                           rel);
                        }
                    }
                    plan->sub_queries.emplace_back(std::move(body));
                    auto exists_eq = make_compare_expression(resource_, compare_type::eq, param_true, param_exists);
                    exists_eq->make_unfoldable();
                    expression_ptr where_expr = exists_eq;
                    if (semi_anti_type == logical_plan::join_type::anti) {
                        auto not_expr = make_compare_union_expression(resource_, compare_type::union_not);
                        not_expr->append_child(std::move(exists_eq));
                        where_expr = std::move(not_expr);
                    }
                    agg->append_child(logical_plan::make_node_match(resource_,
                                                                    core::dbname_t{agg->dbname()},
                                                                    core::relname_t{agg->relname()},
                                                                    std::move(where_expr)));
                }
                where_consumed_by_semi_anti = true;
            }
        }

        // where
        if (node.whereClause && !where_consumed_by_semi_anti) {
            VALUE_OR_RETURN(auto expr_res, transform_predicate(node.whereClause, names, plan));
            expression_ptr expr = std::move(expr_res);
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
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"Unknown node type in group by clause: " +
                                                              node_tag_to_string(nodeTag(field.data)),
                                                          resource_});
                }

                VALUE_OR_RETURN(auto key, columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(field.data), names));
                group->append_expression(
                    make_scalar_expression(resource_, scalar_type::group_field, std::move(key.field)));
            }
        }

        // Parser/Transformer can not distinguish regular function from aggregate one
        // So we have to pick: place all in select and create group node later, or
        // place all in group, and disassemble it, if there is no actual grouping to be done
        // If we add arena allocator for the plan it will be safer to allocate upfront
        // (so everything is placed in group noe)
        if (has_non_star) {
            for (auto& expr : select_node->expressions()) {
                group->append_expression(expr);
            }
            select_node->expressions().clear();
            group->internal_aggregate_count = 0;
        } else if (has_group_by) {
            // SELECT * over a grouped query projects the GROUPING KEYS: they are the only columns
            // with one value per group. The group emits its target list and a group_field is a
            // reduction key rather than an output column, so the keys have to be NAMED in that
            // target list like any other projected column. Snapshot the size first -- the loop
            // appends to the very vector it reads.
            const size_t key_count = group->expressions().size();
            for (size_t i = 0; i < key_count; i++) {
                const auto& key_expr = group->expressions()[i];
                if (key_expr->group() != expression_group::scalar) {
                    continue;
                }
                const auto* key_scalar = static_cast<const scalar_expression_t*>(key_expr.get());
                if (key_scalar->type() != scalar_type::group_field) {
                    continue;
                }
                group->append_expression(make_scalar_expression(resource_, scalar_type::get_field, key_scalar->key()));
            }
        }
        pending_internal_aggs_.clear();

        // Having is transformed AFTER aggregates are routed to the group so resolve_having_operand
        // can reuse them; a HAVING aggregate not already in SELECT is appended to the group as a
        // hidden __having_<fn>_<n> column. Snapshot the group size first so those hidden HAVING-only
        // aggregates (the tail the group grows by here) can be told apart from the visible columns.
        size_t visible_group_count = group->expressions().size();
        expression_ptr having_expr;
        if (node.havingClause) {
            VALUE_OR_RETURN(having_expr, transform_having_expr(node.havingClause, names, plan, group));
        }
        size_t hidden_having_count = group->expressions().size() - visible_group_count;

        // HAVING is a first-class post-aggregation stage: it is lowered to a SEPARATE $having node
        // (an operator_match above the group), never folded into the group node. A HAVING clause
        // also makes the query grouped (implicit GROUP BY ()) — force a scalar (0-key) group even
        // when nothing else populated it, so a bare HAVING TRUE/FALSE is APPLIED above a single
        // collapsed row rather than silently dropped.
        if (!group->expressions().empty() || having_expr) {
            agg->append_child(group);
            if (having_expr) {
                agg->append_child(logical_plan::make_node_having(resource_,
                                                                 core::dbname_t{agg->dbname()},
                                                                 core::relname_t{agg->relname()},
                                                                 having_expr));
            }
        }

        // distinct
        if (node.distinctClause && !node.distinctClause->lst.empty()) {
            agg->set_distinct(true);
            // Plain DISTINCT is the grammar sentinel list_make1(resource, NIL): a single element that
            // IS the NIL List node (nodeTag == T_List). DISTINCT ON (...) carries the real ON
            // expression nodes (ColumnRef / A_Indirection / ...) instead.
            if (nodeTag(node.distinctClause->lst.front().data) != T_List) {
                std::pmr::vector<expressions::key_t> on_keys(resource_);
                for (auto on_it : node.distinctClause->lst) {
                    if (nodeTag(on_it.data) == T_ColumnRef) {
                        VALUE_OR_RETURN(auto key,
                                        columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(on_it.data), names));
                        on_keys.emplace_back(std::move(key.field));
                    } else if (nodeTag(on_it.data) == T_A_Indirection) {
                        VALUE_OR_RETURN(auto res,
                                        indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(on_it.data), names));
                        on_keys.emplace_back(std::move(res.field));
                    } else {
                        // v1: only plain column references. DISTINCT ON (a + b) etc. is a follow-up.
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"DISTINCT ON supports only plain column references", resource_});
                    }
                }
                // PostgreSQL rule: when ORDER BY is present the ON keys must be its leading keys.
                // Without ORDER BY, DISTINCT ON keeps the first row per key in input order.
                if (node.sortClause && !node.sortClause->lst.empty()) {
                    std::pmr::vector<std::pmr::string> lead_sort_names(resource_);
                    for (auto sort_it : node.sortClause->lst) {
                        auto* sortby = pg_ptr_cast<SortBy>(sort_it.data);
                        if (nodeTag(sortby->node) == T_ColumnRef) {
                            VALUE_OR_RETURN(auto key,
                                            columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(sortby->node), names));
                            lead_sort_names.emplace_back(key.field.as_pmr_string());
                        } else if (nodeTag(sortby->node) == T_A_Indirection) {
                            VALUE_OR_RETURN(
                                auto res,
                                indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(sortby->node), names));
                            lead_sort_names.emplace_back(res.field.as_pmr_string());
                        } else {
                            lead_sort_names
                                .emplace_back(); // empty sentinel: a non-column sort key can't match an ON key
                        }
                    }
                    for (size_t i = 0; i < on_keys.size(); ++i) {
                        if (i >= lead_sort_names.size() || lead_sort_names[i] != on_keys[i].as_pmr_string()) {
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{
                                    "SELECT DISTINCT ON expressions must match initial ORDER BY expressions",
                                    resource_});
                        }
                    }
                }
                agg->set_distinct_on_keys(std::move(on_keys));
            }
        }

        // order by
        if (node.sortClause && !node.sortClause->lst.empty()) {
            std::vector<expression_ptr> sort_exprs;
            sort_exprs.reserve(node.sortClause->lst.size());
            for (auto sort_it : node.sortClause->lst) {
                auto sortby = pg_ptr_cast<SortBy>(sort_it.data);
                bool is_desc = sortby->sortby_dir == SORTBY_DESC;
                auto null_ord = map_sortby_nulls(sortby->sortby_nulls);
                // Unary plus is the identity: strip every `+`-layer and dispatch on what remains
                // (`+v` sorts as v, `+(a+b)` as the expression, `+2` as the positional constant).
                // A unary operator arrives as A_Expr{op, lexpr = NULL, rexpr = operand}.
                Node* sort_node = sortby->node;
                while (sort_node && nodeTag(sort_node) == T_A_Expr) {
                    auto* plus = pg_ptr_cast<A_Expr>(sort_node);
                    if (plus->lexpr != nullptr || !plus->name || plus->name->lst.empty() ||
                        std::string_view(strVal(plus->name->lst.front().data)) != "+") {
                        break;
                    }
                    sort_node = plus->rexpr;
                }
                if (!sort_node) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"ORDER BY operator is missing its operand", resource_});
                }
                if (nodeTag(sort_node) == T_ColumnRef) {
                    VALUE_OR_RETURN(auto field,
                                    columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(sort_node), names));
                    sort_exprs.emplace_back(
                        make_sort_expression(field.field, is_desc ? sort_order::desc : sort_order::asc, null_ord));
                } else if (nodeTag(sort_node) == T_A_Indirection) {
                    VALUE_OR_RETURN(auto res,
                                    indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(sort_node), names));
                    column_ref_t field = std::move(res);
                    sort_exprs.emplace_back(
                        make_sort_expression(field.field, is_desc ? sort_order::desc : sort_order::asc, null_ord));
                } else if (nodeTag(sort_node) == T_A_Expr) {
                    // Arithmetic ORDER BY: encode as scalar_expression_t with sort order in key.path()[0]
                    // (0 = ascending, 1 = descending) and the NULLS placement in key.path()[1]
                    // (0 = default, 1 = first, 2 = last). create_plan_sort detects this and builds a
                    // computed sort-key spec instead of a regular sort key.
                    auto a_expr = pg_ptr_cast<A_Expr>(sort_node);
                    if (!a_expr->name || a_expr->name->lst.empty()) {
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"Unsupported operator in ORDER BY", resource_});
                    }
                    auto op_str = std::string_view(strVal(a_expr->name->lst.front().data));
                    if (!is_arithmetic_operator(op_str)) {
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"Unsupported operator in ORDER BY", resource_});
                    }
                    // A unary operator carries its operand in rexpr only; after `+`-stripping the
                    // only arithmetic one left is negation, evaluated as the one-operand
                    // scalar_type::unary_minus (never as a one-legged binary subtract).
                    const bool is_unary = a_expr->lexpr == nullptr;
                    if (!a_expr->rexpr || (is_unary && op_str != "-")) {
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"Unsupported operator in ORDER BY", resource_});
                    }
                    auto stype = is_unary ? expressions::scalar_type::unary_minus : get_arithmetic_scalar_type(op_str);
                    expressions::key_t order_key(resource_);
                    order_key.set_path({is_desc ? size_t(1) : size_t(0), static_cast<size_t>(null_ord)});
                    auto computed_sort = make_scalar_expression(resource_, stype, std::move(order_key));
                    // Resolve operands (without appending to any node — purely for sort)
                    logical_plan::node_ptr dummy_node = group; // resolve_select_operand needs a node_ptr
                    if (!is_unary) {
                        VALUE_OR_RETURN(auto lhs, resolve_select_operand(a_expr->lexpr, names, plan, dummy_node));
                        computed_sort->append_param(std::move(lhs));
                    }
                    VALUE_OR_RETURN(auto rhs, resolve_select_operand(a_expr->rexpr, names, plan, dummy_node));
                    computed_sort->append_param(std::move(rhs));
                    sort_exprs.emplace_back(std::move(computed_sort));
                } else if (nodeTag(sort_node) == T_A_Const) {
                    // Positional `ORDER BY <n>`: map to the n-th output column of this SELECT.
                    auto* value = &(pg_ptr_cast<A_Const>(sort_node)->val);
                    if (nodeTag(value) != T_Integer) {
                        return core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"non-integer constant in ORDER BY", resource_});
                    }
                    column_ref_t field(resource_);
                    RETURN_IF_ERROR(positional_sort_field(node.targetList, intVal(value), names, field));
                    sort_exprs.emplace_back(
                        make_sort_expression(field.field, is_desc ? sort_order::desc : sort_order::asc, null_ord));
                } else {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Unknown node type in ORDER BY: " + node_tag_to_string(nodeTag(sort_node)),
                                         resource_});
                }
            }
            agg->append_child(logical_plan::make_node_sort(resource_,
                                                           core::dbname_t{agg->dbname()},
                                                           core::relname_t{agg->relname()},
                                                           sort_exprs));
        }

        if (having_expr && !has_group_by) {
            for (const auto& ge : group->expressions()) {
                if (ge->group() == expression_group::scalar &&
                    static_cast<const scalar_expression_t*>(ge.get())->type() == scalar_type::get_field) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"column must appear in a GROUP BY clause or be "
                                                          "used in an aggregate function",
                                                          resource_});
                }
            }
        }
        if (!has_non_star && hidden_having_count > 0 && visible_group_count == 0) {
            // Pure SELECT * with an aggregate-only HAVING and no GROUP BY (SELECT * FROM t HAVING
            // count(*) > 5): with no GROUP BY the star routes nothing to the group, so the visible
            // set is empty and there is nothing well-defined to project. PostgreSQL and
            // default-mode MySQL error here (no engine returns the base rows).
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"column must appear in a GROUP BY clause or be used in "
                                                  "an aggregate function",
                                                  resource_});
        }
        if (hidden_having_count > 0) {
            // ONLY the visible group-output columns — the first visible_group_count
            // expressions. This sits ABOVE the sort, which is why the group cannot strip them
            // itself: an ORDER BY key hidden in the group output has to survive that far.
            // internal_aggregate_count stays 0 on purpose (setting it >0 is a BLOCKER: the
            // validator would drop the __having_* column the HAVING match resolves against).
            select_node->expressions().clear();
            for (size_t i = 0; i < visible_group_count; ++i) {
                const auto& ge = group->expressions()[i];
                // A group_field is a reduction key, not an output column — the target list names
                // the key separately (as a get_field), and that entry is what projects it.
                if (ge->group() == expression_group::scalar &&
                    static_cast<const scalar_expression_t*>(ge.get())->type() == scalar_type::group_field) {
                    continue;
                }
                select_node->append_expression(make_scalar_expression(resource_, scalar_type::get_field, ge->key()));
            }
        }
        // TODO: do we even need it anymore?
        if (has_non_star || hidden_having_count > 0) {
            agg->append_child(select_node);
        }

        // limit / offset
        VALUE_OR_RETURN(auto limit_node,
                        build_limit_node(node.limitCount,
                                         node.limitOffset,
                                         core::dbname_t{agg->dbname()},
                                         core::relname_t{agg->relname()},
                                         plan));
        if (limit_node) {
            agg->append_child(std::move(limit_node));
        }

        return agg;
    }
} // namespace components::sql::transform

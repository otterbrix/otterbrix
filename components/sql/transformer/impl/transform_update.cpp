#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    core::result_wrapper_t<expressions::expression_ptr>
    transformer::transform_update_expr(Node* node,
                                       const name_collection_t& names,
                                       logical_plan::execution_plan_t* plan) {
        VALUE_OR_RETURN(auto operand, transform_expression(node, expression_context_t{names, plan}));
        if (std::holds_alternative<expression_ptr>(operand) && !std::get<expression_ptr>(operand)) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"unsupported expression in SET", resource_});
        }
        return as_expression(std::move(operand));
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_update(UpdateStmt& node,
                                                                                 logical_plan::execution_plan_t* plan) {
        // A leading WITH must be registered before the body so the WHERE / FROM can reference the CTE.
        RETURN_IF_ERROR(register_with_ctes(node.withClause));
        logical_plan::node_match_ptr match;
        std::pmr::vector<expression_ptr> updates(resource_);
        name_collection_t names;
        names.left_name = rangevar_to_qualified_name(node.relation);
        names.left_alias = construct_alias(node.relation->alias);

        // UPDATE ... FROM: build the FROM clause as a source sub-plan (a table, a
        // join tree, a table function, or a — possibly LATERAL — derived table) that
        // becomes the RIGHT side of the update join. The source is a plain child node
        // whose scans self-resolve by name, exactly like SELECT's FROM; the operator
        // consumes its materialized output. target = left, source = right for the
        // predicate: register the source's primary relation as right and let validate
        // resolve any further source columns against the source schema.
        logical_plan::node_ptr source_child = nullptr;
        if (node.fromClause && !node.fromClause->lst.empty()) {
            name_collection_t source_names;
            VALUE_OR_RETURN(source_child, transform_from_source(node.fromClause, source_names, plan));
            names.right_name = source_names.left_name;
            names.right_alias = source_names.left_alias;
        }
        // set
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                expressions::key_t target_key{resource_, res->name, side_t::left};
                if (!res->indirection->lst.empty()) {
                    std::pmr::vector<std::pmr::string> path{resource_};
                    path.emplace_back(std::pmr::string{res->name, resource_});
                    for (const auto& val : res->indirection->lst) {
                        if (nodeTag(val.data) == T_A_Indices) {
                            auto indices = pg_ptr_cast<A_Indices>(val.data);
                            path.emplace_back(indices_to_str(resource_, indices));
                        } else {
                            path.emplace_back(pmrStrVal(val.data, resource_));
                        }
                    }
                    target_key = expressions::key_t{std::move(path), side_t::left};
                }
                VALUE_OR_RETURN(auto value, transform_update_expr(res->val, names, plan));
                value->key() = std::move(target_key);
                updates.emplace_back(std::move(value));
            }
        }

        // where
        if (node.whereClause) {
            VALUE_OR_RETURN(auto where_res, transform_predicate(node.whereClause, names, plan));
            expressions::expression_ptr where_expr = std::move(where_res);
            match = logical_plan::make_node_match(resource_,
                                                  core::dbname_t{names.left_name.dbname},
                                                  core::relname_t{names.left_name.relname},
                                                  where_expr);
        } else {
            match = logical_plan::make_node_match(resource_,
                                                  core::dbname_t{names.left_name.dbname},
                                                  core::relname_t{names.left_name.relname},
                                                  make_compare_expression(resource_, compare_type::all_true));
        }

        VALUE_OR_RETURN(auto upd_limit_res,
                        build_dml_limit(node.limitCount,
                                        core::dbname_t{names.left_name.dbname},
                                        core::relname_t{names.left_name.relname},
                                        plan));
        auto upd_limit = std::move(upd_limit_res);
        auto upd = logical_plan::make_node_update(resource_, match, upd_limit, updates, false);
        upd->set_dbname(names.left_name.dbname);
        upd->set_relname(names.left_name.relname);
        // The FROM source is a child sub-plan (the RIGHT side of the update join).
        // Its scans self-resolve by name during enrich, so no table_oid_from / sibling
        // resolve_table splice is needed.
        if (source_child) {
            upd->append_child(source_child);
        }
        if (node.returningList) {
            VALUE_OR_RETURN(upd->returning(), transform_returning(node.returningList, names, plan));
        }
        // Catalog-resolve for the UPDATE target table, with the outgoing
        // constraint gather so enrich reads FKs stamped by
        // operator_resolve_constraint_t.
        register_catalog_resolve_table(resource_,
                                       &catalog_resolves_,
                                       names.left_name.dbname,
                                       names.left_name.relname,
                                       constraint_resolve_kind::outgoing);
        return upd;
    }
} // namespace components::sql::transform

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

    expressions::expression_ptr transformer::transform_update_expr(Node* node,
                                                                   const name_collection_t& names,
                                                                   logical_plan::parameter_node_t* params) {
        auto operand = transform_a_expr_operand(node, names, params);
        if (has_error()) {
            return nullptr;
        }
        if (std::holds_alternative<expression_ptr>(operand)) {
            return std::get<expression_ptr>(operand);
        }
        auto value = make_scalar_expression(
            resource_,
            std::holds_alternative<expressions::key_t>(operand) ? scalar_type::get_field : scalar_type::constant);
        value->append_param(std::move(operand));
        return value;
    }

    logical_plan::node_ptr transformer::transform_update(UpdateStmt& node, logical_plan::execution_plan_t* plan) {
        // A leading WITH must be registered before the body so the WHERE / FROM can reference the CTE.
        register_with_ctes(node.withClause);
        if (has_error()) {
            return nullptr;
        }
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
            source_child = transform_from_source(node.fromClause, source_names, plan);
            if (has_error()) {
                return nullptr;
            }
            names.right_name = source_names.left_name;
            names.right_alias = source_names.left_alias;
        }
        // set
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                expressions::key_t target_key{resource_, res->name, side_t::left};
                if (!res->indirection->lst.empty()) {
                    // The write path nulls whole columns for a NULL literal but has
                    // no NA cast kernel for element writes — reject those here.
                    if (nodeTag(res->val) == T_A_Const && nodeTag(&pg_ptr_cast<A_Const>(res->val)->val) == T_Null) {
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"setting a nested element to NULL is not supported", resource_});
                        return nullptr;
                    }
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
                auto value = transform_update_expr(res->val, names, plan->parameters.get());
                if (has_error()) {
                    return nullptr;
                }
                value->key() = std::move(target_key);
                updates.emplace_back(std::move(value));
            }
        }

        // where
        if (node.whereClause) {
            expressions::expression_ptr where_expr = transform_predicate(node.whereClause, names, plan);
            if (has_error()) {
                return nullptr;
            }
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

        // Identity travels via the catalog-resolve wrap; the update node itself
        // carries only payload + table_oid() (stamped at enrich time from the
        // sibling resolve_table for the target, and table_oid_from() for the
        // UPDATE ... FROM source).
        auto upd_limit = build_dml_limit(node.limitCount,
                                         core::dbname_t{names.left_name.dbname},
                                         core::relname_t{names.left_name.relname},
                                         plan);
        if (has_error()) {
            return nullptr;
        }
        auto upd = logical_plan::make_node_update(resource_, match, upd_limit, updates, false);
        // The FROM source is a child sub-plan (the RIGHT side of the update join).
        // Its scans self-resolve by name during enrich, so no table_oid_from / sibling
        // resolve_table splice is needed.
        if (source_child) {
            upd->append_child(source_child);
        }
        if (node.returningList) {
            upd->returning() = transform_returning(node.returningList, names, plan);
            if (error_.contains_error()) {
                return nullptr;
            }
        }
        // Catalog-resolve wrap for UPDATE target table. Emit
        // resolve_constraint(outgoing) so enrich reads FKs from the plan tree
        // (FK info stamped by operator_resolve_constraint_t).
        return maybe_wrap_with_catalog_resolve_table(resource_,
                                                     names.left_name.dbname,
                                                     names.left_name.relname,
                                                     std::move(upd),
                                                     constraint_resolve_kind::outgoing);
    }
} // namespace components::sql::transform

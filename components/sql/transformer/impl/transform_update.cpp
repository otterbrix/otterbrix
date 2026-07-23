#include <components/expressions/aggregate_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    update_expr_ptr transformer::transform_update_expr(Node* node,
                                                       const name_collection_t& names,
                                                       logical_plan::parameter_node_t* params) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto res = get_value(resource_, node);
                if (res.has_error()) {
                    error_ = res.error();
                    return nullptr;
                }
                core::parameter_id_t id = params->add_parameter(std::move(res.value()));
                return {new update_expr_get_const_value_t(id)};
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                core::parameter_id_t id;
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        id = params->add_parameter(types::logical_value_t(resource_, str));
                        break;
                    }
                    case T_Integer: {
                        int64_t int_value = intVal(value);
                        id = params->add_parameter(types::logical_value_t(resource_, int_value));
                        break;
                    }
                    case T_Float: {
                        float float_value = floatVal(value);
                        id = params->add_parameter(types::logical_value_t(resource_, float_value));
                        break;
                    }
                    default: {
                        // NULL and any other literal kind (get_value maps T_Null to an
                        // NA value; the old assert left `id` uninitialized in Release).
                        auto res = get_value(resource_, node);
                        if (res.has_error()) {
                            error_ = res.error();
                            return nullptr;
                        }
                        id = params->add_parameter(std::move(res.value()));
                        break;
                    }
                }
                return {new update_expr_get_const_value_t(id)};
            }
            case T_A_ArrayExpr: {
                auto array = pg_ptr_cast<A_ArrayExpr>(node);
                if (auto res = get_array(resource_, array->elements); res.has_error()) {
                    error_ = res.error();
                    return nullptr;
                } else {
                    auto id = params->add_parameter(std::move(res.value()));
                    return {new update_expr_get_const_value_t(id)};
                }
            }
            case T_ParamRef: {
                return {new update_expr_get_const_value_t(add_param_value(node, params))};
            }
            case T_A_Expr: {
                auto expr = pg_ptr_cast<A_Expr>(node);
                switch (expr->kind) {
                    case AEXPR_OP: {
                        auto t = pg_ptr_cast<ResTarget>(expr->name->lst.front().data);
                        // Dispatch on the FULL operator name: a prefix match would
                        // swallow multi-char operators that merely share a first
                        // character with an arithmetic one (e.g. jsonb '->', '#>').
                        const std::string op{t->name};
                        update_expr_type type;
                        if (op == "+") {
                            type = update_expr_type::add;
                        } else if (op == "-") {
                            type = update_expr_type::sub;
                        } else if (op == "*") {
                            type = update_expr_type::mult;
                        } else if (op == "/") {
                            type = update_expr_type::div;
                        } else if (op == "%") {
                            type = update_expr_type::mod;
                        } else if (op == "^") {
                            type = update_expr_type::exp;
                        } else if (op == "!") {
                            type = update_expr_type::factorial;
                        } else if (op == "@") {
                            type = update_expr_type::abs;
                        } else if (op == "<<") {
                            type = update_expr_type::shift_left;
                        } else if (op == ">>") {
                            type = update_expr_type::shift_right;
                        } else if (op == "~") {
                            type = update_expr_type::NOT;
                        } else if (op == "&") {
                            type = update_expr_type::AND;
                        } else if (op == "|") {
                            type = update_expr_type::OR;
                        } else if (op == "#") {
                            type = update_expr_type::XOR;
                        } else if (op == "|/") {
                            type = update_expr_type::sqr_root;
                        } else if (op == "||/") {
                            type = update_expr_type::cube_root;
                        } else {
                            error_ = core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"unsupported operator '" + op + "' in UPDATE SET expression",
                                                 resource_});
                            return nullptr;
                        }
                        update_expr_ptr res{new update_expr_calculate_t(type)};
                        if (expr->lexpr) {
                            res->left() = transform_update_expr(expr->lexpr, names, params);
                            if (has_error()) {
                                return nullptr;
                            }
                        }
                        if (expr->rexpr) {
                            res->right() = transform_update_expr(expr->rexpr, names, params);
                            if (has_error()) {
                                return nullptr;
                            }
                        }
                        // The calculate executor evaluates unary operators on its LEFT
                        // operand, but a prefix operator parses with the operand on the
                        // right; binary operators dereference both operands. Enforce
                        // arity here — a missing operand used to reach the executor as
                        // a null child and crash it.
                        const bool is_unary = type == update_expr_type::sqr_root ||
                                              type == update_expr_type::cube_root ||
                                              type == update_expr_type::factorial || type == update_expr_type::abs ||
                                              type == update_expr_type::NOT;
                        if (is_unary) {
                            if (!res->left() && res->right()) {
                                res->left() = std::move(res->right());
                                res->right() = nullptr;
                            }
                            if (!res->left() || res->right()) {
                                error_ = core::error_t(
                                    core::error_code_t::sql_parse_error,
                                    std::pmr::string{"operator '" + op +
                                                         "' takes exactly one operand in UPDATE SET expression",
                                                     resource_});
                                return nullptr;
                            }
                        } else if (!res->left() || !res->right()) {
                            error_ = core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"operator '" + op + "' requires two operands in UPDATE SET expression",
                                                 resource_});
                            return nullptr;
                        }
                        return res;
                    }
                    default:
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"unsupported expression kind in UPDATE SET expression", resource_});
                        return nullptr;
                }
            }
            case T_A_Indirection: {
                auto indirection = pg_ptr_cast<A_Indirection>(node);
                if (indirection->indirection->lst.empty()) {
                    return transform_update_expr(indirection->arg, names, params);
                } else {
                    auto key = indirection_to_field(resource_, indirection, names);
                    key.deduce_side(names);
                    return {new update_expr_get_value_t(std::move(key.field))};
                }
            }
            case T_ColumnRef: {
                auto ref = pg_ptr_cast<ColumnRef>(node);
                auto key = columnref_to_field(resource_, ref, names);
                key.deduce_side(names);
                return {new update_expr_get_value_t(std::move(key.field))};
            }
            default:
                break;
        }
        // Returning a bare nullptr here would ship a null child in the update
        // expression tree and crash (or silently no-op) at execution.
        error_ = core::error_t(core::error_code_t::sql_parse_error,
                               std::pmr::string{"unsupported expression in UPDATE SET (function calls, "
                                                "subqueries and CASE are not supported here)",
                                                resource_});
        return nullptr;
    }

    logical_plan::node_ptr transformer::transform_update(UpdateStmt& node, logical_plan::execution_plan_t* plan) {
        // A leading WITH must be registered before the body so the WHERE / FROM can reference the CTE.
        register_with_ctes(node.withClause);
        if (has_error()) {
            return nullptr;
        }
        logical_plan::node_match_ptr match;
        std::pmr::vector<update_expr_ptr> updates(resource_);
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
                if (res->indirection->lst.empty()) {
                    updates.emplace_back(new update_expr_set_t(expressions::key_t{resource_, res->name, side_t::left}));
                    updates.back()->left() = transform_update_expr(res->val, names, plan->parameters.get());
                } else {
                    // The set executor nulls whole columns for a NULL literal but has
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
                    updates.emplace_back(new update_expr_set_t(expressions::key_t{std::move(path), side_t::left}));
                    updates.back()->left() = transform_update_expr(res->val, names, plan->parameters.get());
                }
                if (has_error()) {
                    return nullptr;
                }
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

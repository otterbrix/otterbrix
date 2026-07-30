#include "create_plan_select.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_select.hpp>

#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_group.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace services::planner::impl {

    namespace {

        using components::expressions::expression_group;
        using components::expressions::scalar_type;

        bool is_arithmetic_scalar_type(scalar_type t) {
            return t == scalar_type::add || t == scalar_type::subtract || t == scalar_type::multiply ||
                   t == scalar_type::divide || t == scalar_type::mod || t == scalar_type::case_expr ||
                   t == scalar_type::unary_minus;
        }

        // Build a select_column_t from a scalar_expression_t into `col`.
        //
        // Returns false on a defensive validation failure so the caller can return nullptr ->
        // executor surfaces the error (rule 9: no throw on the operator-build path). Mirrors
        // add_group_scalar in create_plan_group.cpp.
        bool make_select_column_scalar(std::pmr::memory_resource* resource,
                                       const components::expressions::scalar_expression_t* expr,
                                       const components::logical_plan::storage_parameters* storage_params,
                                       components::operators::select_column_t& col) {
            // Determine output alias
            if (!expr->key().storage().empty()) {
                col.key.name = std::pmr::string(expr->key().storage().back(), resource);
            }

            switch (expr->type()) {
                case scalar_type::get_field: {
                    col.type = components::operators::select_column_t::kind::field_ref;
                    // The operand of a field reference names a column. A bound parameter or a
                    // nested expression there is not a column reference.
                    if (!expr->params().empty() && !components::expressions::is_key(expr->params().front())) {
                        return false;
                    }
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : components::expressions::as_key(expr->params().front());
                    col.key.type = components::operators::group_key_t::kind::column;
                    col.key.full_path = field.path();
                    // Validation always resolves a concrete side; carry it so a
                    // joined DELETE/UPDATE RETURNING can pick the correct input
                    // chunk. (Ignored when the operator feeds a single chunk.)
                    col.key.side = field.side();
                    // If alias was set but name is still empty, use field name
                    if (col.key.name.empty() && !field.storage().empty()) {
                        col.key.name = std::pmr::string(field.storage().back(), resource);
                    }
                    break;
                }
                case scalar_type::constant: {
                    col.type = components::operators::select_column_t::kind::constant;
                    col.key.type = components::operators::group_key_t::kind::column;
                    if (!expr->params().empty() &&
                        components::expressions::is_parameter(expr->params().front()) && storage_params) {
                        auto id = components::expressions::as_parameter(expr->params().front());
                        col.constant_param_id = id;
                        const auto* bound = components::logical_plan::get_parameter(storage_params, id);
                        if (!bound) {
                            return false;
                        }
                        col.constant_value = *bound;
                    }
                    break;
                }
                case scalar_type::coalesce: {
                    col.type = components::operators::select_column_t::kind::coalesce;
                    col.key.type = components::operators::group_key_t::kind::coalesce;
                    col.key.coalesce_entries =
                        std::pmr::vector<components::operators::group_key_t::coalesce_entry>(resource);
                    for (const auto& param : expr->params()) {
                        components::operators::group_key_t::coalesce_entry entry(resource);
                        if (components::expressions::is_key(param)) {
                            auto& k = components::expressions::as_key(param);
                            entry.type = components::operators::group_key_t::coalesce_entry::source::column;
                            assert(!k.path().empty() && "coalesce column path must be resolved before execution");
                            entry.col_index = k.path()[0];
                            entry.constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        } else if (components::expressions::is_parameter(param) && storage_params) {
                            auto id = components::expressions::as_parameter(param);
                            entry.type = components::operators::group_key_t::coalesce_entry::source::constant;
                            entry.col_index = 0;
                            const auto* bound = components::logical_plan::get_parameter(storage_params, id);
                            if (!bound) {
                                return false;
                            }
                            entry.constant = *bound;
                        } else {
                            entry.type = components::operators::group_key_t::coalesce_entry::source::constant;
                            entry.col_index = 0;
                            entry.constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        }
                        col.key.coalesce_entries.push_back(std::move(entry));
                    }
                    break;
                }
                case scalar_type::case_when: {
                    col.type = components::operators::select_column_t::kind::case_when;
                    col.key.type = components::operators::group_key_t::kind::case_when;
                    col.key.case_clauses = std::pmr::vector<components::operators::group_key_t::case_clause>(resource);
                    auto& params = expr->params();
                    size_t i = 0;
                    while (i + 3 < params.size()) {
                        components::operators::group_key_t::case_clause clause(resource);
                        if (components::expressions::is_key(params[i])) {
                            auto& k = components::expressions::as_key(params[i]);
                            assert(!k.path().empty() && "case_when condition path must be resolved before execution");
                            clause.condition_col = k.path()[0];
                        }
                        if (components::expressions::is_expr(params[i + 1])) {
                            auto& cmp_expr = components::expressions::as_expr(params[i + 1]);
                            if (cmp_expr->group() == expression_group::compare) {
                                auto* cmp =
                                    static_cast<const components::expressions::compare_expression_t*>(cmp_expr.get());
                                clause.cmp = cmp->type();
                            } else {
                                clause.cmp = components::expressions::compare_type::eq;
                            }
                        } else {
                            clause.cmp = components::expressions::compare_type::eq;
                        }
                        if (components::expressions::is_parameter(params[i + 2]) && storage_params) {
                            const auto* bound = components::logical_plan::get_parameter(
                                storage_params, components::expressions::as_parameter(params[i + 2]));
                            if (!bound) {
                                return false;
                            }
                            clause.condition_value = *bound;
                        } else {
                            clause.condition_value = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        }
                        if (components::expressions::is_key(params[i + 3])) {
                            auto& k = components::expressions::as_key(params[i + 3]);
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::column;
                            assert(!k.path().empty() && "case_when result path must be resolved before execution");
                            clause.res_col = k.path()[0];
                            clause.res_constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        } else if (components::expressions::is_parameter(params[i + 3]) && storage_params) {
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::constant;
                            clause.res_col = 0;
                            const auto* bound = components::logical_plan::get_parameter(
                                storage_params, components::expressions::as_parameter(params[i + 3]));
                            if (!bound) {
                                return false;
                            }
                            clause.res_constant = *bound;
                        } else {
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::constant;
                            clause.res_col = 0;
                            clause.res_constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        }
                        col.key.case_clauses.push_back(std::move(clause));
                        i += 4;
                    }
                    // else clause
                    if (i < params.size()) {
                        if (components::expressions::is_key(params[i])) {
                            auto& k = components::expressions::as_key(params[i]);
                            col.key.else_type = components::operators::group_key_t::else_source::column;
                            col.key.else_col = k.path().empty() ? 0 : k.path()[0];
                        } else if (components::expressions::is_parameter(params[i]) && storage_params) {
                            col.key.else_type = components::operators::group_key_t::else_source::constant;
                            const auto* bound = components::logical_plan::get_parameter(
                                storage_params, components::expressions::as_parameter(params[i]));
                            if (!bound) {
                                return false;
                            }
                            col.key.else_constant = *bound;
                        } else {
                            col.key.else_type = components::operators::group_key_t::else_source::null_value;
                        }
                    }
                    break;
                }
                case scalar_type::star_expand: {
                    col.type = components::operators::select_column_t::kind::star_expand;
                    col.key.type = components::operators::group_key_t::kind::column;
                    break;
                }
                default: {
                    if (is_arithmetic_scalar_type(expr->type())) {
                        col.type = components::operators::select_column_t::kind::arithmetic;
                        col.arith_op = expr->type();
                        col.operands = expr->params();
                    }
                    break;
                }
            }
            return true;
        }

    } // namespace

    bool build_returning_columns(std::pmr::memory_resource* resource,
                                 const std::pmr::vector<components::expressions::expression_ptr>& returning,
                                 const components::logical_plan::storage_parameters* params,
                                 std::pmr::vector<components::operators::select_column_t>& columns) {
        columns.reserve(returning.size());
        for (const auto& expr : returning) {
            if (expr && expr->group() == expression_group::scalar) {
                auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                components::operators::select_column_t col(resource);
                if (!make_select_column_scalar(resource, scalar_expr, params, col)) {
                    return false;
                }
                columns.push_back(std::move(col));
            }
        }
        return true;
    }

    components::operators::operator_ptr create_plan_select(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           const components::logical_plan::storage_parameters* params) {
        auto table_oid = node->table_oid();
        bool known = context.has_table_oid(table_oid);
        auto plan_resource = known ? context.resource : node->resource();
        auto plan_log = known ? context.log.clone() : log_t{};

        auto op = boost::intrusive_ptr(new components::operators::operator_select_t(plan_resource, plan_log));

        // Aggregates are always handled by operator_group_t upstream; node_select_t only contains
        // scalar expressions (get_field, arithmetic, constant, star_expand, coalesce, case_when).
        for (const auto& expr : node->expressions()) {
            if (expr->group() == expression_group::scalar) {
                auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                components::operators::select_column_t col(plan_resource);
                if (!make_select_column_scalar(plan_resource, scalar_expr, params, col)) {
                    // Defensive guard tripped: return nullptr -> executor surfaces the error
                    // (rule 9: no throw on the operator-build path).
                    return nullptr;
                }
                op->add_column(std::move(col));
            }
        }

        return op;
    }

} // namespace services::planner::impl

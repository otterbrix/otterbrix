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

        // Build a select_column_t from a scalar_expression_t
        components::operators::select_column_t
        make_select_column_scalar(std::pmr::memory_resource* resource,
                                  const components::expressions::scalar_expression_t* expr,
                                  const components::logical_plan::storage_parameters* storage_params) {
            components::operators::select_column_t col(resource);

            // Determine output alias
            if (!expr->key().storage().empty()) {
                col.key.name = std::pmr::string(expr->key().storage().back(), resource);
            }

            switch (expr->type()) {
                case scalar_type::get_field: {
                    col.type = components::operators::select_column_t::kind::field_ref;
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : std::get<components::expressions::key_t>(expr->params().front());
                    col.key.type = components::operators::group_key_t::kind::column;
                    col.key.full_path = field.path();
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
                        std::holds_alternative<core::parameter_id_t>(expr->params().front()) && storage_params) {
                        auto id = std::get<core::parameter_id_t>(expr->params().front());
                        col.constant_value = storage_params->parameters.at(id);
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
                        if (std::holds_alternative<components::expressions::key_t>(param)) {
                            auto& k = std::get<components::expressions::key_t>(param);
                            entry.type = components::operators::group_key_t::coalesce_entry::source::column;
                            assert(!k.path().empty() && "coalesce column path must be resolved before execution");
                            entry.col_index = k.path()[0];
                            entry.constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        } else if (std::holds_alternative<core::parameter_id_t>(param) && storage_params) {
                            auto id = std::get<core::parameter_id_t>(param);
                            entry.type = components::operators::group_key_t::coalesce_entry::source::constant;
                            entry.col_index = 0;
                            entry.constant = storage_params->parameters.at(id);
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
                        if (std::holds_alternative<components::expressions::key_t>(params[i])) {
                            auto& k = std::get<components::expressions::key_t>(params[i]);
                            assert(!k.path().empty() && "case_when condition path must be resolved before execution");
                            clause.condition_col = k.path()[0];
                        }
                        if (std::holds_alternative<components::expressions::expression_ptr>(params[i + 1])) {
                            auto& cmp_expr = std::get<components::expressions::expression_ptr>(params[i + 1]);
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
                        if (std::holds_alternative<core::parameter_id_t>(params[i + 2]) && storage_params) {
                            clause.condition_value =
                                storage_params->parameters.at(std::get<core::parameter_id_t>(params[i + 2]));
                        } else {
                            clause.condition_value = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        }
                        if (std::holds_alternative<components::expressions::key_t>(params[i + 3])) {
                            auto& k = std::get<components::expressions::key_t>(params[i + 3]);
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::column;
                            assert(!k.path().empty() && "case_when result path must be resolved before execution");
                            clause.res_col = k.path()[0];
                            clause.res_constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        } else if (std::holds_alternative<core::parameter_id_t>(params[i + 3]) && storage_params) {
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::constant;
                            clause.res_col = 0;
                            clause.res_constant =
                                storage_params->parameters.at(std::get<core::parameter_id_t>(params[i + 3]));
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
                        if (std::holds_alternative<components::expressions::key_t>(params[i])) {
                            auto& k = std::get<components::expressions::key_t>(params[i]);
                            col.key.else_type = components::operators::group_key_t::else_source::column;
                            col.key.else_col = k.path().empty() ? 0 : k.path()[0];
                        } else if (std::holds_alternative<core::parameter_id_t>(params[i]) && storage_params) {
                            col.key.else_type = components::operators::group_key_t::else_source::constant;
                            col.key.else_constant =
                                storage_params->parameters.at(std::get<core::parameter_id_t>(params[i]));
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
            return col;
        }

    } // namespace

    components::operators::operator_ptr create_plan_select(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node,
                                                           const components::logical_plan::storage_parameters* params) {
        auto coll_name = node->collection_full_name();
        bool known = context.has_collection(coll_name);
        auto plan_resource = known ? context.resource : node->resource();
        auto plan_log = known ? context.log.clone() : log_t{};

        auto op = boost::intrusive_ptr(new components::operators::operator_select_t(plan_resource, plan_log));

        // Aggregates are always handled by operator_group_t upstream; node_select_t only contains
        // scalar expressions (get_field, arithmetic, constant, star_expand, coalesce, case_when).
        for (const auto& expr : node->expressions()) {
            if (expr->group() == expression_group::scalar) {
                auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                op->add_column(make_select_column_scalar(plan_resource, scalar_expr, params));
            }
        }

        return op;
    }

} // namespace services::planner::impl

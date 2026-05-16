#include "create_plan_group.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_group.hpp>

#include <components/physical_plan/operators/operator_group.hpp>

#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_group.hpp>

namespace services::planner::impl {

    namespace {

        using components::expressions::expression_group;
        using components::expressions::scalar_type;

        bool is_arithmetic_scalar_type(scalar_type t) {
            return t == scalar_type::add || t == scalar_type::subtract || t == scalar_type::multiply ||
                   t == scalar_type::divide || t == scalar_type::mod || t == scalar_type::case_expr ||
                   t == scalar_type::unary_minus;
        }

        void add_group_scalar(boost::intrusive_ptr<components::operators::operator_group_t>& group,
                              const components::expressions::scalar_expression_t* expr,
                              std::pmr::memory_resource* resource,
                              const components::logical_plan::storage_parameters* storage_params,
                              size_t key_idx = SIZE_MAX) {
            switch (expr->type()) {
                case scalar_type::group_field: {
                    // GROUP BY field: add as a visible output key so operator_select_t can reference by name.
                    const auto& path = expr->key().path();
                    components::operators::group_key_t key(resource);
                    key.name = std::pmr::string(expr->key().storage().back(), resource);
                    key.type = components::operators::group_key_t::kind::column;
                    key.full_path = path;
                    group->add_key(std::move(key));
                    break;
                }
                case scalar_type::get_field: {
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : std::get<components::expressions::key_t>(expr->params().front());
                    const auto& path = field.path();
                    components::operators::group_key_t key(resource);
                    key.name = std::pmr::string(expr->key().storage().back(), resource);
                    key.type = components::operators::group_key_t::kind::column;
                    key.full_path = path;
                    group->add_key(std::move(key));
                    break;
                }
                case scalar_type::coalesce: {
                    components::operators::group_key_t key(resource);
                    key.name = std::pmr::string(expr->key().storage().back(), resource);
                    key.type = components::operators::group_key_t::kind::coalesce;
                    key.coalesce_entries =
                        std::pmr::vector<components::operators::group_key_t::coalesce_entry>(resource);
                    for (const auto& param : expr->params()) {
                        components::operators::group_key_t::coalesce_entry entry(resource);
                        if (std::holds_alternative<components::expressions::key_t>(param)) {
                            auto& k = std::get<components::expressions::key_t>(param);
                            entry.type = components::operators::group_key_t::coalesce_entry::source::column;
                            entry.col_index = k.path().empty() ? 0 : k.path()[0];
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
                        key.coalesce_entries.push_back(std::move(entry));
                    }
                    group->add_key(std::move(key));
                    break;
                }
                case scalar_type::case_when: {
                    components::operators::group_key_t key(resource);
                    key.name = std::pmr::string(expr->key().storage().back(), resource);
                    key.type = components::operators::group_key_t::kind::case_when;
                    key.case_clauses = std::pmr::vector<components::operators::group_key_t::case_clause>(resource);

                    // case_when params: triplets of (condition_col, condition_value, result)
                    // Format: [cond_key, cmp_type_expr, cond_val, result, ...], else_result
                    auto& params = expr->params();
                    size_t i = 0;
                    while (i + 3 < params.size()) {
                        components::operators::group_key_t::case_clause clause(resource);
                        // condition column
                        if (std::holds_alternative<components::expressions::key_t>(params[i])) {
                            auto& k = std::get<components::expressions::key_t>(params[i]);
                            clause.condition_col = k.path().empty() ? 0 : k.path()[0];
                        }
                        // comparison type - encoded as expression
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
                        // condition value
                        if (std::holds_alternative<core::parameter_id_t>(params[i + 2]) && storage_params) {
                            auto id = std::get<core::parameter_id_t>(params[i + 2]);
                            clause.condition_value = storage_params->parameters.at(id);
                        } else {
                            clause.condition_value = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        }
                        // result
                        if (std::holds_alternative<components::expressions::key_t>(params[i + 3])) {
                            auto& k = std::get<components::expressions::key_t>(params[i + 3]);
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::column;
                            clause.res_col = k.path().empty() ? 0 : k.path()[0];
                            clause.res_constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        } else if (std::holds_alternative<core::parameter_id_t>(params[i + 3]) && storage_params) {
                            auto id = std::get<core::parameter_id_t>(params[i + 3]);
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::constant;
                            clause.res_col = 0;
                            clause.res_constant = storage_params->parameters.at(id);
                        } else {
                            clause.res_type = components::operators::group_key_t::case_clause::result_source::constant;
                            clause.res_col = 0;
                            clause.res_constant = components::types::logical_value_t(
                                resource,
                                components::types::complex_logical_type{components::types::logical_type::NA});
                        }
                        key.case_clauses.push_back(std::move(clause));
                        i += 4;
                    }
                    // else clause (remaining param if any)
                    if (i < params.size()) {
                        if (std::holds_alternative<components::expressions::key_t>(params[i])) {
                            auto& k = std::get<components::expressions::key_t>(params[i]);
                            key.else_type = components::operators::group_key_t::else_source::column;
                            key.else_col = k.path().empty() ? 0 : k.path()[0];
                        } else if (std::holds_alternative<core::parameter_id_t>(params[i]) && storage_params) {
                            auto id = std::get<core::parameter_id_t>(params[i]);
                            key.else_type = components::operators::group_key_t::else_source::constant;
                            key.else_constant = storage_params->parameters.at(id);
                        } else {
                            key.else_type = components::operators::group_key_t::else_source::null_value;
                        }
                    }
                    group->add_key(std::move(key));
                    break;
                }
                default: {
                    if (is_arithmetic_scalar_type(expr->type())) {
                        if (expr->key().storage().empty()) {
                            throw std::logic_error(
                                "create_plan_group: arithmetic expression has empty storage for key: " +
                                expr->key().as_string());
                        }
                        auto alias = std::pmr::string(expr->key().storage().back(), resource);
                        if (!expr->key().path().empty() && expr->key().path()[0] == SIZE_MAX) {
                            // Post-aggregate arithmetic (marked by validator)
                            components::operators::post_aggregate_column_t post{alias, expr->type(), expr->params()};
                            group->add_post_aggregate(std::move(post));
                        } else {
                            // Pre-group computed column
                            components::operators::computed_column_t comp{alias, expr->type(), expr->params(), key_idx};
                            group->add_computed_column(std::move(comp));
                            group->add_key(std::pmr::string(expr->key().as_string(), resource));
                        }
                    }
                    break;
                }
            }
        }

        void add_group_aggregate(std::pmr::memory_resource* resource,
                                 log_t log,
                                 const components::compute::function_registry_t& function_registry,
                                 boost::intrusive_ptr<components::operators::operator_group_t>& group,
                                 const components::expressions::aggregate_expression_t* expr) {
            group->add_value(expr->key().as_pmr_string(),
                             boost::intrusive_ptr(new components::operators::aggregate::operator_func_t(
                                 resource,
                                 log,
                                 function_registry.get_function(expr->function_uid()),
                                 expr->params(),
                                 expr->is_distinct())));
        }

    } // namespace

    components::operators::operator_ptr
    create_plan_group(const context_storage_t& context,
                      const components::compute::function_registry_t& function_registry,
                      const components::logical_plan::node_ptr& node,
                      const components::logical_plan::storage_parameters* params) {
        boost::intrusive_ptr<components::operators::operator_group_t> group;
        auto coll_name = node->collection_full_name();
        bool known = context.has_collection(coll_name);

        components::expressions::expression_ptr having;
        size_t internal_aggregate_count = 0;
        if (auto* group_node = dynamic_cast<const components::logical_plan::node_group_t*>(node.get())) {
            having = group_node->having();
            internal_aggregate_count = group_node->internal_aggregate_count;
        }

        if (known) {
            group = new components::operators::operator_group_t(context.resource,
                                                                context.log.clone(),
                                                                std::move(having),
                                                                internal_aggregate_count);
        } else {
            group = new components::operators::operator_group_t(node->resource(),
                                                                log_t{},
                                                                std::move(having),
                                                                internal_aggregate_count);
        }

        // Build group operator from node expressions
        auto plan_resource = known ? context.resource : node->resource();
        size_t key_idx = 0;

        for (size_t i = 0; i < node->expressions().size(); i++) {
            const auto& expr = node->expressions()[i];

            if (expr->group() == expression_group::scalar) {
                auto* scalar_expr = static_cast<const components::expressions::scalar_expression_t*>(expr.get());
                add_group_scalar(group, scalar_expr, plan_resource, params, key_idx);
                // Track key_idx for arithmetic computed columns
                switch (scalar_expr->type()) {
                    case scalar_type::group_field:
                        key_idx++;
                        break;
                    case scalar_type::get_field:
                    case scalar_type::coalesce:
                    case scalar_type::case_when:
                        key_idx++;
                        break;
                    default:
                        if (is_arithmetic_scalar_type(scalar_expr->type())) {
                            if (scalar_expr->key().path().empty() || scalar_expr->key().path()[0] != SIZE_MAX) {
                                key_idx++;
                            }
                        }
                        break;
                }

            } else if (expr->group() == expression_group::aggregate) {
                add_group_aggregate(plan_resource,
                                    known ? context.log.clone() : log_t{},
                                    function_registry,
                                    group,
                                    static_cast<const components::expressions::aggregate_expression_t*>(expr.get()));
            }
        }

        return group;
    }

} // namespace services::planner::impl

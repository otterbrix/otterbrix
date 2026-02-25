#include "create_plan_group.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_group.hpp>

#include <components/physical_plan/operators/operator_group.hpp>

#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_group.hpp>

namespace services::planner::impl {

    namespace {

        using components::expressions::scalar_type;
        using components::expressions::expression_group;

        bool is_arithmetic_scalar_type(scalar_type t) {
            return t == scalar_type::add || t == scalar_type::subtract ||
                   t == scalar_type::multiply || t == scalar_type::divide ||
                   t == scalar_type::mod || t == scalar_type::case_expr;
        }

        // Check if any operand (recursively) references an aggregate result
        bool has_aggregate_operand(const std::pmr::vector<components::expressions::param_storage>& operands,
                                   const std::vector<std::string>& aggregate_aliases) {
            for (const auto& op : operands) {
                if (std::holds_alternative<components::expressions::key_t>(op)) {
                    auto& key = std::get<components::expressions::key_t>(op);
                    for (const auto& alias : aggregate_aliases) {
                        if (key.as_string() == alias) {
                            return true;
                        }
                    }
                } else if (std::holds_alternative<components::expressions::expression_ptr>(op)) {
                    auto& sub_expr = std::get<components::expressions::expression_ptr>(op);
                    if (sub_expr->group() == expression_group::scalar) {
                        auto* sub_scalar =
                            static_cast<const components::expressions::scalar_expression_t*>(sub_expr.get());
                        if (has_aggregate_operand(sub_scalar->params(), aggregate_aliases)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        void add_group_scalar(boost::intrusive_ptr<components::operators::operator_group_t>& group,
                              const components::expressions::scalar_expression_t* expr,
                              const std::vector<std::string>& aggregate_aliases,
                              std::pmr::memory_resource* resource) {
            switch (expr->type()) {
                case scalar_type::group_field:
                    break;
                case scalar_type::get_field: {
                    group->add_key(std::pmr::string(expr->key().as_string(), resource));
                    break;
                }
                default: {
                    if (is_arithmetic_scalar_type(expr->type())) {
                        if (has_aggregate_operand(expr->params(), aggregate_aliases)) {
                            // Post-aggregate arithmetic
                            components::operators::post_aggregate_column_t post{
                                std::pmr::string(expr->key().as_string(), resource),
                                expr->type(),
                                expr->params()};
                            group->add_post_aggregate(std::move(post));
                        } else {
                            // Pre-group computed column
                            components::operators::computed_column_t comp{
                                std::pmr::string(expr->key().as_string(), resource),
                                expr->type(),
                                expr->params()};
                            group->add_computed_column(std::move(comp));
                            // Also add as key for output projection
                            group->add_key(
                                std::pmr::string(expr->key().as_string(), resource));
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
                                 expr->params())));
        }

    } // namespace

    components::operators::operator_ptr
    create_plan_group(const context_storage_t& context,
                      const components::compute::function_registry_t& function_registry,
                      const components::logical_plan::node_ptr& node) {
        boost::intrusive_ptr<components::operators::operator_group_t> group;
        auto coll_name = node->collection_full_name();
        bool known = context.has_collection(coll_name);
        
        components::expressions::expression_ptr having;
        if (auto* group_node = dynamic_cast<const components::logical_plan::node_group_t*>(node.get())) {
            having = group_node->having();
        }

        if (known) {
            group = new components::operators::operator_group_t(context.resource, context.log.clone(), std::move(having));
        } else {
            group = new components::operators::operator_group_t(node->resource(), log_t{}, std::move(having));
        }

        // First pass: collect aggregate aliases
        std::vector<std::string> aggregate_aliases;
        for (const auto& expr : node->expressions()) {
            if (expr->group() == expression_group::aggregate) {
                auto* agg_expr =
                    static_cast<const components::expressions::aggregate_expression_t*>(expr.get());
                aggregate_aliases.push_back(agg_expr->key().as_string());
            }
        }

        // Second pass: create operators
        auto plan_resource = known ? context.resource : node->resource();
        std::for_each(node->expressions().begin(),
                      node->expressions().end(),
                      [&](const components::expressions::expression_ptr& expr) {
                          if (expr->group() == expression_group::scalar) {
                              add_group_scalar(
                                  group,
                                  static_cast<const components::expressions::scalar_expression_t*>(expr.get()),
                                  aggregate_aliases,
                                  plan_resource);
                          } else if (expr->group() == expression_group::aggregate) {
                              add_group_aggregate(
                                  plan_resource,
                                  known ? context.log.clone() : log_t{},
                                  function_registry,
                                  group,
                                  static_cast<const components::expressions::aggregate_expression_t*>(expr.get()));
                          }
                      });
        return group;
    }

} // namespace services::planner::impl

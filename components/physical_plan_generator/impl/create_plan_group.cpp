#include "create_plan_group.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <components/physical_plan/collection/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/collection/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/collection/operators/get/simple_value.hpp>
#include <components/physical_plan/collection/operators/operator_group.hpp>

#include <components/physical_plan/table/operators/aggregate/operator_avg.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_count.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_max.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_min.hpp>
#include <components/physical_plan/table/operators/aggregate/operator_sum.hpp>
#include <components/physical_plan/table/operators/get/simple_value.hpp>
#include <components/physical_plan/table/operators/operator_group.hpp>

namespace services::collection::planner::impl {

    namespace {

        void add_group_scalar(boost::intrusive_ptr<components::collection::operators::operator_group_t>& group,
                              const components::expressions::scalar_expression_t* expr) {
            using components::expressions::scalar_type;

            switch (expr->type()) {
                case scalar_type::get_field: {
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : std::get<components::expressions::key_t>(expr->params().front());
                    group->add_key(expr->key().storage().back(),
                                   components::collection::operators::get::simple_value_t::create(field));
                    break;
                }
                case scalar_type::group_field:
                    // TODO: no plan for now, as every SELECT field is included in GROUP BY implicitly, implement
                    break;
                default:
                    assert(false && "not implemented create plan to scalar exression");
                    break;
            }
        }

        void add_group_aggregate(context_collection_t* context,
                                 boost::intrusive_ptr<components::collection::operators::operator_group_t>& group,
                                 const components::expressions::aggregate_expression_t* expr) {
            using components::expressions::aggregate_type;

            switch (expr->type()) {
                case aggregate_type::count: {
                    group->add_value(expr->key().as_pmr_string(),
                                     boost::intrusive_ptr(
                                         new components::collection::operators::aggregate::operator_count_t(context)));
                    break;
                }
                case aggregate_type::sum: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::sum:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_pmr_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_sum_t(context, field)));
                    break;
                }
                case aggregate_type::avg: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::avg:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_pmr_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_avg_t(context, field)));
                    break;
                }
                case aggregate_type::min: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::min:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_pmr_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_min_t(context, field)));
                    break;
                }
                case aggregate_type::max: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::max:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(
                        expr->key().as_pmr_string(),
                        boost::intrusive_ptr(
                            new components::collection::operators::aggregate::operator_max_t(context, field)));
                    break;
                }
                default:
                    assert(false && "not implemented create plan to aggregate exression");
                    break;
            }
        }

    } // namespace

    components::collection::operators::operator_ptr create_plan_group(const context_storage_t& context,
                                                                      const components::logical_plan::node_ptr& node) {
        boost::intrusive_ptr<components::collection::operators::operator_group_t> group;
        auto collection_context = context.at(node->collection_full_name());
        if (collection_context) {
            group = new components::collection::operators::operator_group_t(collection_context);
        } else {
            group = new components::collection::operators::operator_group_t(node->resource());
        }
        std::for_each(node->expressions().begin(),
                      node->expressions().end(),
                      [&](const components::expressions::expression_ptr& expr) {
                          if (expr->group() == components::expressions::expression_group::scalar) {
                              add_group_scalar(
                                  group,
                                  static_cast<const components::expressions::scalar_expression_t*>(expr.get()));
                          } else if (expr->group() == components::expressions::expression_group::aggregate) {
                              add_group_aggregate(
                                  context.at(node->collection_full_name()),
                                  group,
                                  static_cast<const components::expressions::aggregate_expression_t*>(expr.get()));
                          }
                      });
        return group;
    }

} // namespace services::collection::planner::impl

namespace services::table::planner::impl {

    namespace {

        void add_group_scalar(boost::intrusive_ptr<components::table::operators::operator_group_t>& group,
                              const components::expressions::scalar_expression_t* expr) {
            using components::expressions::scalar_type;

            switch (expr->type()) {
                case scalar_type::get_field: {
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : std::get<components::expressions::key_t>(expr->params().front());
                    group->add_key(expr->key().storage().back(),
                                   components::table::operators::get::simple_value_t::create(field));
                    break;
                }
                default:
                    assert(false && "not implemented create plan to scalar exression");
                    break;
            }
        }

        void add_group_aggregate(collection::context_collection_t* context,
                                 boost::intrusive_ptr<components::table::operators::operator_group_t>& group,
                                 const components::expressions::aggregate_expression_t* expr) {
            using components::expressions::aggregate_type;

            switch (expr->type()) {
                case aggregate_type::count: {
                    group->add_value(
                        expr->key().as_pmr_string(),
                        boost::intrusive_ptr(new components::table::operators::aggregate::operator_count_t(context)));
                    break;
                }
                case aggregate_type::sum: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::sum:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(expr->key().as_pmr_string(),
                                     boost::intrusive_ptr(
                                         new components::table::operators::aggregate::operator_sum_t(context, field)));
                    break;
                }
                case aggregate_type::avg: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::avg:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(expr->key().as_pmr_string(),
                                     boost::intrusive_ptr(
                                         new components::table::operators::aggregate::operator_avg_t(context, field)));
                    break;
                }
                case aggregate_type::min: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::min:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(expr->key().as_pmr_string(),
                                     boost::intrusive_ptr(
                                         new components::table::operators::aggregate::operator_min_t(context, field)));
                    break;
                }
                case aggregate_type::max: {
                    assert(std::holds_alternative<components::expressions::key_t>(expr->params().front()) &&
                           "[add_group_aggregate] aggregate_type::max:  variant intermediate_store_ holds the "
                           "alternative components::expressions::key_t");
                    auto field = std::get<components::expressions::key_t>(expr->params().front());
                    group->add_value(expr->key().as_pmr_string(),
                                     boost::intrusive_ptr(
                                         new components::table::operators::aggregate::operator_max_t(context, field)));
                    break;
                }
                default:
                    assert(false && "not implemented create plan to aggregate exression");
                    break;
            }
        }

    } // namespace

    components::base::operators::operator_ptr create_plan_group(const context_storage_t& context,
                                                                const components::logical_plan::node_ptr& node) {
        boost::intrusive_ptr<components::table::operators::operator_group_t> group;
        auto collection_context = context.at(node->collection_full_name());
        if (collection_context) {
            group = new components::table::operators::operator_group_t(collection_context);
        } else {
            group = new components::table::operators::operator_group_t(node->resource());
        }
        std::for_each(node->expressions().begin(),
                      node->expressions().end(),
                      [&](const components::expressions::expression_ptr& expr) {
                          if (expr->group() == components::expressions::expression_group::scalar) {
                              add_group_scalar(
                                  group,
                                  static_cast<const components::expressions::scalar_expression_t*>(expr.get()));
                          } else if (expr->group() == components::expressions::expression_group::aggregate) {
                              add_group_aggregate(
                                  context.at(node->collection_full_name()),
                                  group,
                                  static_cast<const components::expressions::aggregate_expression_t*>(expr.get()));
                          }
                      });
        return group;
    }

} // namespace services::table::planner::impl

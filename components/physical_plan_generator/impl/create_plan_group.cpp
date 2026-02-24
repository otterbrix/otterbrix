#include "create_plan_group.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <components/physical_plan/operators/operator_group.hpp>

#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/get/simple_value.hpp>
#include <components/physical_plan/operators/operator_group.hpp>

namespace services::planner::impl {

    namespace {

        void add_group_scalar(boost::intrusive_ptr<components::operators::operator_group_t>& group,
                              const components::expressions::scalar_expression_t* expr) {
            using components::expressions::scalar_type;

            switch (expr->type()) {
                case scalar_type::group_field:
                    break;
                case scalar_type::get_field: {
                    auto field = expr->params().empty()
                                     ? expr->key()
                                     : std::get<components::expressions::key_t>(expr->params().front());
                    group->add_key(expr->key().storage().back(),
                                   components::operators::get::simple_value_t::create(field));
                    break;
                }
                default:
                    break;
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
        if (known) {
            group = new components::operators::operator_group_t(context.resource, context.log.clone());
        } else {
            group = new components::operators::operator_group_t(node->resource(), log_t{});
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
                                  known ? context.resource : node->resource(),
                                  known ? context.log.clone() : log_t{},
                                  function_registry,
                                  group,
                                  static_cast<const components::expressions::aggregate_expression_t*>(expr.get()));
                          }
                      });
        return group;
    }

} // namespace services::planner::impl

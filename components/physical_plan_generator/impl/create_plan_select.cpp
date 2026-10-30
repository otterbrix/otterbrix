#include "create_plan_select.hpp"

#include <components/expressions/projected_name.hpp>
#include <components/logical_plan/node_select.hpp>

#include <components/physical_plan/operators/operator_select.hpp>

namespace services::planner::impl {

    namespace {

        using components::expressions::expression_group;
        using components::expressions::expression_ptr;

        bool projects_column(const expression_ptr& expression) noexcept {
            if (!expression) {
                return false;
            }
            switch (expression->group()) {
                case expression_group::scalar:
                case expression_group::function:
                case expression_group::cast:
                case expression_group::compare:
                    return true;
                default:
                    return false;
            }
        }

        components::operators::projected_column_t projected_column(std::pmr::memory_resource* resource,
                                                                   const expression_ptr& expression) {
            return {resource, components::expressions::projected_name(expression), expression};
        }

    } // namespace

    std::pmr::vector<components::operators::projected_column_t>
    build_returning_columns(std::pmr::memory_resource* resource,
                            const std::pmr::vector<components::expressions::expression_ptr>& returning) {
        std::pmr::vector<components::operators::projected_column_t> columns(resource);
        columns.reserve(returning.size());
        for (const auto& expression : returning) {
            if (projects_column(expression)) {
                columns.push_back(projected_column(resource, expression));
            }
        }
        return columns;
    }

    components::operators::operator_ptr create_plan_select(const context_storage_t& context,
                                                           const components::logical_plan::node_ptr& node) {
        auto* plan_resource = context.resource;
        auto op =
            boost::intrusive_ptr(new components::operators::operator_select_t(plan_resource, context.log.clone()));

        for (const auto& expression : node->expressions()) {
            if (projects_column(expression)) {
                op->add_column(projected_column(plan_resource, expression));
            }
        }

        return op;
    }

} // namespace services::planner::impl

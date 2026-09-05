#include "create_plan_select.hpp"

#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_select.hpp>

#include <components/physical_plan/operators/operator_select.hpp>

namespace services::planner::impl {

    namespace {

        using components::expressions::expression_group;
        using components::expressions::expression_ptr;
        using components::expressions::key_t;
        using components::expressions::scalar_expression_t;
        using components::expressions::scalar_type;

        std::pmr::string projected_name(std::pmr::memory_resource* resource, const expression_ptr& expression) {
            if (expression->group() != expression_group::scalar) {
                return std::pmr::string(expression->key().as_string(), resource);
            }
            if (!expression->key().storage().empty()) {
                return std::pmr::string(expression->key().storage().back(), resource);
            }
            const auto* scalar = static_cast<const scalar_expression_t*>(expression.get());
            if (scalar->type() == scalar_type::get_field && !scalar->params().empty()) {
                const auto* field = std::get_if<key_t>(&scalar->params().front());
                if (field != nullptr && !field->storage().empty()) {
                    return std::pmr::string(field->storage().back(), resource);
                }
            }
            return std::pmr::string(resource);
        }

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
            return {resource, projected_name(resource, expression), expression};
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
        auto table_oid = node->table_oid();
        bool known = context.has_table_oid(table_oid);
        auto plan_resource = known ? context.resource : node->resource();
        auto plan_log = known ? context.log.clone() : log_t{};

        auto op = boost::intrusive_ptr(new components::operators::operator_select_t(plan_resource, plan_log));

        for (const auto& expression : node->expressions()) {
            if (projects_column(expression)) {
                op->add_column(projected_column(plan_resource, expression));
            }
        }

        return op;
    }

} // namespace services::planner::impl

#include "utils.hpp"
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>

namespace components::operators::predicates::impl {

    // Forward declaration
    value_getter create_arithmetic_value_getter(std::pmr::memory_resource* resource,
                                                const compute::function_registry_t* function_registry,
                                                const expressions::scalar_expression_ptr& expr,
                                                const logical_plan::storage_parameters* parameters);

    value_getter create_value_getter(const expressions::key_t& key) {
        if (!key.path().empty()) {
            if (key.side() == expressions::side_t::right) {
                return [path = key.path()](const vector::data_chunk_t&,
                                           const vector::data_chunk_t& chunk_right,
                                           size_t,
                                           size_t index_right) -> types::logical_value_t {
                    return chunk_right.at(path)->value(index_right);
                };
            } else {
                return [path = key.path()](const vector::data_chunk_t& chunk_left,
                                           const vector::data_chunk_t&,
                                           size_t index_left,
                                           size_t) -> types::logical_value_t {
                    return chunk_left.at(path)->value(index_left);
                };
            }
        }
        auto name = key.as_string();
        if (key.side() == expressions::side_t::right) {
            return [name](const vector::data_chunk_t&,
                          const vector::data_chunk_t& chunk_right,
                          size_t,
                          size_t index_right) -> types::logical_value_t {
                auto col_idx = chunk_right.column_index(name);
                return chunk_right.data[col_idx].value(index_right);
            };
        } else {
            return [name](const vector::data_chunk_t& chunk_left,
                          const vector::data_chunk_t&,
                          size_t index_left,
                          size_t) -> types::logical_value_t {
                auto col_idx = chunk_left.column_index(name);
                return chunk_left.data[col_idx].value(index_left);
            };
        }
    }

    value_getter create_value_getter(core::parameter_id_t id, const logical_plan::storage_parameters* parameters) {
        return [val = parameters->parameters.at(
                    id)](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return val; };
    }

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::function_expression_ptr& expr,
                                     const logical_plan::storage_parameters* parameters) {
        std::pmr::vector<value_getter> args_getters(resource);
        for (const auto& arg : expr->args()) {
            if (std::holds_alternative<expressions::key_t>(arg)) {
                const auto& key = std::get<expressions::key_t>(arg);
                args_getters.emplace_back(create_value_getter(key));
            } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                auto id = std::get<core::parameter_id_t>(arg);
                args_getters.emplace_back(create_value_getter(id, parameters));
            } else {
                const auto& sub_expr_ptr = std::get<expressions::expression_ptr>(arg);
                if (sub_expr_ptr->group() == expressions::expression_group::scalar) {
                    const auto& scalar_expr =
                        reinterpret_cast<const expressions::scalar_expression_ptr&>(sub_expr_ptr);
                    args_getters.emplace_back(
                        create_arithmetic_value_getter(resource, function_registry, scalar_expr, parameters));
                } else {
                    const auto& func_expr =
                        reinterpret_cast<const expressions::function_expression_ptr&>(sub_expr_ptr);
                    args_getters.emplace_back(
                        create_value_getter(resource, function_registry, func_expr, parameters));
                }
            }
        }

        return [resource,
                args_getters = std::move(args_getters),
                function_ptr =
                    function_registry->get_function(expr->function_uid())](const vector::data_chunk_t& chunk_left,
                                                                           const vector::data_chunk_t& chunk_right,
                                                                           size_t index_left,
                                                                           size_t index_right) {
            std::pmr::vector<types::logical_value_t> args(resource);
            args.reserve(args_getters.size());
            for (const auto& getter : args_getters) {
                args.emplace_back(getter.operator()(chunk_left, chunk_right, index_left, index_right));
            }
            auto res = function_ptr->execute(args);
            if (res.status() != compute::compute_status::ok()) {
                throw std::runtime_error("operators::predicates: undefined error during function "
                                         "execution in value_getter_t");
            }
            return std::get<std::pmr::vector<types::logical_value_t>>(res.value()).front();
        };
    }

    value_getter create_arithmetic_value_getter(std::pmr::memory_resource* resource,
                                                const compute::function_registry_t* function_registry,
                                                const expressions::scalar_expression_ptr& expr,
                                                const logical_plan::storage_parameters* parameters) {
        // Build sub-getters for each operand
        std::pmr::vector<value_getter> operand_getters(resource);
        for (const auto& param : expr->params()) {
            operand_getters.emplace_back(
                create_value_getter(resource, function_registry, param, parameters));
        }

        auto op = expr->type();
        return [operand_getters = std::move(operand_getters), op](
                   const vector::data_chunk_t& chunk_left,
                   const vector::data_chunk_t& chunk_right,
                   size_t index_left,
                   size_t index_right) -> types::logical_value_t {
            auto left_val = operand_getters[0](chunk_left, chunk_right, index_left, index_right);
            auto right_val = operand_getters[1](chunk_left, chunk_right, index_left, index_right);
            switch (op) {
                case expressions::scalar_type::add:
                    return types::logical_value_t::sum(left_val, right_val);
                case expressions::scalar_type::subtract:
                    return types::logical_value_t::subtract(left_val, right_val);
                case expressions::scalar_type::multiply:
                    return types::logical_value_t::mult(left_val, right_val);
                case expressions::scalar_type::divide:
                    return types::logical_value_t::divide(left_val, right_val);
                case expressions::scalar_type::mod:
                    return types::logical_value_t::modulus(left_val, right_val);
                default:
                    throw std::logic_error("Unsupported arithmetic op in predicate");
            }
        };
    }

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::param_storage& var,
                                     const logical_plan::storage_parameters* parameters) {
        if (std::holds_alternative<expressions::key_t>(var)) {
            const auto& key = std::get<expressions::key_t>(var);
            return create_value_getter(key);
        } else if (std::holds_alternative<core::parameter_id_t>(var)) {
            auto id = std::get<core::parameter_id_t>(var);
            return create_value_getter(id, parameters);
        } else {
            const auto& sub_expr = std::get<expressions::expression_ptr>(var);
            if (sub_expr->group() == expressions::expression_group::scalar) {
                const auto& scalar_expr =
                    reinterpret_cast<const expressions::scalar_expression_ptr&>(sub_expr);
                return create_arithmetic_value_getter(resource, function_registry, scalar_expr, parameters);
            } else {
                const auto& func_expr =
                    reinterpret_cast<const expressions::function_expression_ptr&>(sub_expr);
                return create_value_getter(resource, function_registry, func_expr, parameters);
            }
        }
    }

} // namespace components::operators::predicates::impl
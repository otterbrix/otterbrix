#include "utils.hpp"
#include <components/expressions/key.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <stdexcept>

namespace components::operators::predicates::impl {

    // Forward declaration
    value_getter create_arithmetic_value_getter(std::pmr::memory_resource* resource,
                                                const compute::function_registry_t* function_registry,
                                                const expressions::scalar_expression_ptr& expr,
                                                const logical_plan::storage_parameters* parameters);

    value_getter create_value_getter(std::pmr::memory_resource* resource, const expressions::key_t& key) {
        if (key.side() == expressions::side_t::undefined) {
            return [error = core::result_wrapper_t<types::logical_value_t>{core::error_t(
                        core::error_code_t::comparison_failure,

                        std::pmr::string{"create_value_getter: key side is undefined", resource})}](
                       const vector::data_chunk_t&,
                       const vector::data_chunk_t&,
                       size_t,
                       size_t) { return error; };
        }
        if (key.path().empty()) {
            return [error = core::result_wrapper_t<types::logical_value_t>{core::error_t(
                        core::error_code_t::comparison_failure,

                        std::pmr::string{"create_value_getter: key path is empty", resource})}](
                       const vector::data_chunk_t&,
                       const vector::data_chunk_t&,
                       size_t,
                       size_t) { return error; };
        }
        if (key.side() == expressions::side_t::left) {
            return [path = key.path()](const vector::data_chunk_t& chunk_left,
                                       const vector::data_chunk_t&,
                                       size_t index_left,
                                       size_t) -> core::result_wrapper_t<types::logical_value_t> {
                auto* vec = chunk_left.at(path);
                if (!vec->validity().row_is_valid(index_left)) {
                    return types::logical_value_t(chunk_left.resource(), nullptr);
                }
                return vec->value(index_left);
            };
        } else {
            return [path = key.path()](const vector::data_chunk_t&,
                                       const vector::data_chunk_t& chunk_right,
                                       size_t,
                                       size_t index_right) -> core::result_wrapper_t<types::logical_value_t> {
                auto* vec = chunk_right.at(path);
                if (!vec->validity().row_is_valid(index_right)) {
                    return types::logical_value_t(chunk_right.resource(), nullptr);
                }
                return vec->value(index_right);
            };
        }
    }

    value_getter create_value_getter(std::pmr::memory_resource*,
                                     core::parameter_id_t id,
                                     const logical_plan::storage_parameters* parameters) {
        return [val = core::result_wrapper_t<types::logical_value_t>{parameters->parameters.at(
                    id)}](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return val; };
    }

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::function_expression_ptr& expr,
                                     const logical_plan::storage_parameters* parameters) {
        std::pmr::vector<value_getter> args_getters(resource);
        for (const auto& arg : expr->args()) {
            if (std::holds_alternative<expressions::key_t>(arg)) {
                const auto& key = std::get<expressions::key_t>(arg);
                args_getters.emplace_back(create_value_getter(resource, key));
            } else if (std::holds_alternative<core::parameter_id_t>(arg)) {
                auto id = std::get<core::parameter_id_t>(arg);
                args_getters.emplace_back(create_value_getter(resource, id, parameters));
            } else {
                const auto& sub_expr_ptr = std::get<expressions::expression_ptr>(arg);
                if (sub_expr_ptr->group() == expressions::expression_group::scalar) {
                    const auto& scalar_expr = reinterpret_cast<const expressions::scalar_expression_ptr&>(sub_expr_ptr);
                    args_getters.emplace_back(
                        create_arithmetic_value_getter(resource, function_registry, scalar_expr, parameters));
                } else {
                    const auto& func_expr = reinterpret_cast<const expressions::function_expression_ptr&>(sub_expr_ptr);
                    args_getters.emplace_back(create_value_getter(resource, function_registry, func_expr, parameters));
                }
            }
        }

        return [resource,
                args_getters = std::move(args_getters),
                function_ptr = function_registry->get_function(expr->function_uid())](
                   const vector::data_chunk_t& chunk_left,
                   const vector::data_chunk_t& chunk_right,
                   size_t index_left,
                   size_t index_right) -> core::result_wrapper_t<types::logical_value_t> {
            std::pmr::vector<types::logical_value_t> args(resource);
            args.reserve(args_getters.size());
            for (const auto& getter : args_getters) {
                auto res = getter.operator()(chunk_left, chunk_right, index_left, index_right);
                if (res.has_error()) {
                    return res;
                } else {
                    args.emplace_back(std::move(res.value()));
                }
            }
            auto res = function_ptr->execute(args);
            if (res.has_error()) {
                return res.convert_error<types::logical_value_t>();
            }
            return std::get<std::pmr::vector<types::logical_value_t>>(res.value()).front();
        };
    }

    value_getter create_arithmetic_value_getter(std::pmr::memory_resource* resource,
                                                const compute::function_registry_t* function_registry,
                                                const expressions::scalar_expression_ptr& expr,
                                                const logical_plan::storage_parameters* parameters) {
        assert(expr->params().size() >= 2);
        auto left_getter = create_value_getter(resource, function_registry, expr->params()[0], parameters);
        auto right_getter = create_value_getter(resource, function_registry, expr->params()[1], parameters);

        auto op = expr->type();
        return [resource, left_getter = std::move(left_getter), right_getter = std::move(right_getter), op](
                   const vector::data_chunk_t& chunk_left,
                   const vector::data_chunk_t& chunk_right,
                   size_t index_left,
                   size_t index_right) -> core::result_wrapper_t<types::logical_value_t> {
            auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
            auto right_val = right_getter(chunk_left, chunk_right, index_left, index_right);
            if (left_val.has_error()) {
                return left_val;
            }
            if (right_val.has_error()) {
                return right_val;
            }
            switch (op) {
                case expressions::scalar_type::add:
                    return types::logical_value_t::sum(left_val.value(), right_val.value());
                case expressions::scalar_type::subtract:
                    return types::logical_value_t::subtract(left_val.value(), right_val.value());
                case expressions::scalar_type::multiply:
                    return types::logical_value_t::mult(left_val.value(), right_val.value());
                case expressions::scalar_type::divide:
                    return types::logical_value_t::divide(left_val.value(), right_val.value());
                case expressions::scalar_type::mod:
                    return types::logical_value_t::modulus(left_val.value(), right_val.value());
                default:
                    return core::error_t(core::error_code_t::comparison_failure,

                                         std::pmr::string{"Unsupported arithmetic op in predicate", resource});
            }
        };
    }

    value_getter create_value_getter(std::pmr::memory_resource* resource,
                                     const compute::function_registry_t* function_registry,
                                     const expressions::param_storage& var,
                                     const logical_plan::storage_parameters* parameters) {
        if (std::holds_alternative<expressions::key_t>(var)) {
            const auto& key = std::get<expressions::key_t>(var);
            return create_value_getter(resource, key);
        } else if (std::holds_alternative<core::parameter_id_t>(var)) {
            auto id = std::get<core::parameter_id_t>(var);
            return create_value_getter(resource, id, parameters);
        } else {
            const auto& sub_expr = std::get<expressions::expression_ptr>(var);
            if (sub_expr->group() == expressions::expression_group::scalar) {
                const auto& scalar_expr = reinterpret_cast<const expressions::scalar_expression_ptr&>(sub_expr);
                return create_arithmetic_value_getter(resource, function_registry, scalar_expr, parameters);
            } else {
                const auto& func_expr = reinterpret_cast<const expressions::function_expression_ptr&>(sub_expr);
                return create_value_getter(resource, function_registry, func_expr, parameters);
            }
        }
    }

} // namespace components::operators::predicates::impl
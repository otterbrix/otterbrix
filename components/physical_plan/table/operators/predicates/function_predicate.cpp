#include "function_predicate.hpp"
#include "utils.hpp"

namespace components::table::operators::predicates {

    function_predicate::function_predicate(check_function_t func)
        : func_(func) {}

    bool function_predicate::check_impl(const vector::data_chunk_t& chunk_left,
                                        const vector::data_chunk_t& chunk_right,
                                        size_t index_left,
                                        size_t index_right) {
        return func_(chunk_left, chunk_right, index_left, index_right);
    }

    predicate_ptr create_complex_function_predicate(std::pmr::memory_resource* resource,
                                                    const compute::function_registry_t* function_registry,
                                                    const expressions::function_expression_ptr& expr,
                                                    const logical_plan::storage_parameters* parameters) {
        std::pmr::vector<impl::value_getter> arg_getters(resource);
        arg_getters.reserve(expr->args().size());
        for (const auto& arg : expr->args()) {
            arg_getters.emplace_back(impl::create_value_getter(resource, function_registry, arg, parameters));
        }
        const auto* function = function_registry->get_function(expr->function_uid());
        return {new function_predicate(
            [resource, expr, arg_getters = std::move(arg_getters), function](const vector::data_chunk_t& left,
                                                                             const vector::data_chunk_t& right,
                                                                             size_t left_index,
                                                                             size_t right_index) {
                std::pmr::vector<types::logical_value_t> args(resource);
                args.reserve(expr->args().size());
                for (const auto& arg : arg_getters) {
                    args.emplace_back(arg(left, right, left_index, right_index));
                }
                auto res = function->execute(args);
                if (!res) {
                    throw std::runtime_error(res.status().message());
                }
                return std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0].value<bool>();
            })};
    }

    predicate_ptr create_function_predicate(std::pmr::memory_resource* resource,
                                            const compute::function_registry_t* function_registry,
                                            const expressions::function_expression_ptr& expr,
                                            const logical_plan::storage_parameters* parameters) {
        // if any of the function arguments is a function call, we have to use
        for (const auto& arg : expr->args()) {
            if (std::holds_alternative<expressions::expression_ptr>(arg)) {
                return create_complex_function_predicate(resource, function_registry, expr, parameters);
            }
        }

        const auto* function = function_registry->get_function(expr->function_uid());
        return {new function_predicate([resource, expr, function, parameters](const vector::data_chunk_t& left,
                                                                              const vector::data_chunk_t& right,
                                                                              size_t left_index,
                                                                              size_t right_index) {
            std::pmr::vector<types::logical_value_t> args(resource);
            args.reserve(expr->args().size());
            for (const auto& arg : expr->args()) {
                if (std::holds_alternative<expressions::key_t>(arg)) {
                    const auto& key = std::get<expressions::key_t>(arg);
                    if (key.side() == expressions::side_t::left) {
                        args.emplace_back(left.at(key.path())->value(left_index));
                    } else {
                        args.emplace_back(right.at(key.path())->value(right_index));
                    }
                } else {
                    auto id = std::get<core::parameter_id_t>(arg);
                    args.emplace_back(parameters->parameters.at(id));
                }
            }
            auto res = function->execute(args);
            if (!res) {
                throw std::runtime_error(res.status().message());
            }
            return std::get<std::pmr::vector<types::logical_value_t>>(res.value())[0].value<bool>();
        })};
    }

} // namespace components::table::operators::predicates
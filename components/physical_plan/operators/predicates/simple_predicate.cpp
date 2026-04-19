#include "simple_predicate.hpp"
#include "utils.hpp"

#include <regex>

namespace components::operators::predicates {

    namespace {

        // Because regex is not a constexpr(?), we use it to dispatch function
        template<typename T = void>
        struct regex;

        template<>
        struct regex<void> {};

        template<typename COMP, typename T, typename U>
        core::result_wrapper_t<bool>
        evaluate_comp(std::pmr::memory_resource*, T left, U right) requires(!std::is_same_v<COMP, regex<>>) {
            return COMP{}(left, right);
        }

        template<typename COMP, typename T, typename U>
        core::result_wrapper_t<bool>
        evaluate_comp(std::pmr::memory_resource* resource, T, U) requires(std::is_same_v<COMP, regex<>>) {
            return core::error_t(core::error_code_t::comparison_failure,
                                 std::pmr::string{"incorrect argument type for regex", resource});
        }

        template<typename COMP>
        core::result_wrapper_t<bool> evaluate_comp(std::pmr::memory_resource*,
                                                   std::string_view left,
                                                   std::string_view right) requires(std::is_same_v<COMP, regex<>>) {
            return std::regex_search(std::string(left), std::regex(std::string(right)));
        }

        template<typename COMP>
        core::result_wrapper_t<bool>
        evaluate_comp(std::pmr::memory_resource* resource,
                      const types::logical_value_t& left,
                      const types::logical_value_t& right) requires(std::is_same_v<COMP, regex<>>) {
            return evaluate_comp<COMP>(resource, left.value<std::string_view>(), right.value<std::string_view>());
        }

        template<typename COMP>
        simple_predicate::row_check_fn_t make_comparator(std::pmr::memory_resource* resource,
                                                         const compute::function_registry_t* function_registry,
                                                         const expressions::compare_expression_ptr& expr,
                                                         const logical_plan::storage_parameters* parameters) {
            auto left_getter = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
            auto right_getter = impl::create_value_getter(resource, function_registry, expr->right(), parameters);
            return [resource, left_getter = std::move(left_getter), right_getter = std::move(right_getter)](
                       const vector::data_chunk_t& chunk_left,
                       const vector::data_chunk_t& chunk_right,
                       size_t index_left,
                       size_t index_right) -> core::result_wrapper_t<bool> {
                auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
                auto right_val = right_getter(chunk_left, chunk_right, index_left, index_right);
                if (left_val.has_error()) {
                    return left_val.convert_error<bool>();
                }
                if (right_val.has_error()) {
                    return right_val.convert_error<bool>();
                }
                // Technically this will be neither true nor false, but for simplicity we use false
                if (left_val.value().is_null() || right_val.value().is_null()) {
                    return false;
                }
                return evaluate_comp<COMP>(resource, left_val.value(), right_val.value());
            };
        }

    } // anonymous namespace

    simple_predicate::simple_predicate(std::pmr::memory_resource* resource, row_check_fn_t func)
        : resource_(resource)
        , func_(std::move(func)) {}

    simple_predicate::simple_predicate(std::pmr::memory_resource* resource,
                                       std::pmr::vector<predicate_ptr>&& nested,
                                       expressions::compare_type nested_type)
        : resource_(resource)
        , nested_(std::move(nested))
        , nested_type_(nested_type) {}

    core::result_wrapper_t<std::vector<bool>>
    simple_predicate::batch_check_impl(const vector::data_chunk_t& left,
                                       const vector::data_chunk_t& right,
                                       const vector::indexing_vector_t& left_indices,
                                       const vector::indexing_vector_t& right_indices,
                                       uint64_t count) {
        switch (nested_type_) {
            case expressions::compare_type::union_and: {
                std::vector<bool> result(count, true);
                for (const auto& child : nested_) {
                    auto child_res = child->batch_check(left, right, left_indices, right_indices, count);
                    if (child_res.has_error()) {
                        return child_res;
                    }
                    for (uint64_t k = 0; k < count; ++k) {
                        result[k] = result[k] && child_res.value()[k];
                    }
                }
                return result;
            }
            case expressions::compare_type::union_or: {
                std::vector<bool> result(count, false);
                for (const auto& child : nested_) {
                    auto child_res = child->batch_check(left, right, left_indices, right_indices, count);
                    if (child_res.has_error()) {
                        return child_res;
                    }
                    for (uint64_t k = 0; k < count; ++k) {
                        result[k] = result[k] || child_res.value()[k];
                    }
                }
                return result;
            }
            case expressions::compare_type::union_not: {
                auto result = nested_.front()->batch_check(left, right, left_indices, right_indices, count);
                if (result.has_error()) {
                    return result;
                }
                for (size_t i = 0; i < result.value().size(); ++i) {
                    result.value()[i] = !result.value()[i];
                }
                return result;
            }
            default:
                // fallback to row-by-row via func_
                std::vector<bool> result(count);
                for (uint64_t k = 0; k < count; ++k) {
                    if (auto res = func_(left, right, left_indices.get_index(k), right_indices.get_index(k));
                        res.has_error()) {
                        return res.convert_error<std::vector<bool>>();
                    } else {
                        result[k] = res.value();
                    }
                }
                return result;
        }
    }

    core::result_wrapper_t<bool> simple_predicate::check_impl(const vector::data_chunk_t& chunk_left,
                                                              const vector::data_chunk_t& chunk_right,
                                                              size_t index_left,
                                                              size_t index_right) {
        switch (nested_type_) {
            case expressions::compare_type::union_and:
                for (const auto& predicate : nested_) {
                    if (auto res = predicate->check(chunk_left, chunk_right, index_left, index_right);
                        res.has_error() || !res.value()) {
                        return res;
                    }
                }
                return true;
            case expressions::compare_type::union_or:
                for (const auto& predicate : nested_) {
                    if (auto res = predicate->check(chunk_left, chunk_right, index_left, index_right);
                        res.has_error() || res.value()) {
                        return res;
                    }
                }
                return false;
            case expressions::compare_type::union_not: {
                auto res = nested_.front()->check(chunk_left, chunk_right, index_left, index_right);
                if (!res.has_error()) {
                    res.value() = !res.value();
                }
                return res;
            }
            default:
                break;
        }
        return func_(chunk_left, chunk_right, index_left, index_right);
    }

    predicate_ptr create_simple_predicate(std::pmr::memory_resource* resource,
                                          const compute::function_registry_t* function_registry,
                                          const expressions::compare_expression_ptr& expr,
                                          const std::pmr::vector<types::complex_logical_type>& types_left,
                                          const std::pmr::vector<types::complex_logical_type>& types_right,
                                          const logical_plan::storage_parameters* parameters) {
        using expressions::compare_type;

        switch (expr->type()) {
            case compare_type::union_and:
            case compare_type::union_or:
            case compare_type::union_not: {
                std::pmr::vector<predicate_ptr> nested{resource};
                nested.reserve(expr->children().size());
                for (const auto& nested_expr : expr->children()) {
                    nested.emplace_back(create_predicate(resource,
                                                         function_registry,
                                                         nested_expr,
                                                         types_left,
                                                         types_right,
                                                         parameters));
                }
                return {new simple_predicate(resource, std::move(nested), expr->type())};
            }
            case compare_type::eq:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::equal_to<>>(resource, function_registry, expr, parameters))};
            case compare_type::ne:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::not_equal_to<>>(resource, function_registry, expr, parameters))};
            case compare_type::gt:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::greater<>>(resource, function_registry, expr, parameters))};
            case compare_type::gte:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::greater_equal<>>(resource, function_registry, expr, parameters))};
            case compare_type::lt:
                return {
                    new simple_predicate(resource,
                                         make_comparator<std::less<>>(resource, function_registry, expr, parameters))};
            case compare_type::lte:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::less_equal<>>(resource, function_registry, expr, parameters))};
            case compare_type::regex:
                return {new simple_predicate(resource,
                                             make_comparator<regex<>>(resource, function_registry, expr, parameters))};
            case compare_type::all_false:
                return {new simple_predicate(
                    resource,
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; })};
            case compare_type::is_null: {
                return {new simple_predicate(
                    resource,
                    [column_path = std::get<expressions::key_t>(expr->left()).path()](
                        const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t&,
                        size_t index_left,
                        size_t) { return !chunk_left.at(column_path)->validity().row_is_valid(index_left); })};
            }
            case compare_type::is_not_null: {
                return {new simple_predicate(resource,
                                             [column_path = std::get<expressions::key_t>(expr->left()).path()](
                                                 const vector::data_chunk_t& chunk_left,
                                                 const vector::data_chunk_t&,
                                                 size_t index_left,
                                                 size_t) -> core::result_wrapper_t<bool> {
                                                 return chunk_left.at(column_path)->validity().row_is_valid(index_left);
                                             })};
            }
            case compare_type::all_true:
            default:
                return {
                    new simple_predicate(resource,
                                         [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t)
                                             -> core::result_wrapper_t<bool> { return true; })};
        }
    }

} // namespace components::operators::predicates

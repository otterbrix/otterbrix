#include "simple_predicate.hpp"
#include "utils.hpp"

#include <optional>
#include <regex>

namespace components::operators::predicates {

    namespace {

        // Because regex is not a constexpr(?), we use it to dispatch function
        template<typename T = void>
        struct regex;

        template<>
        struct regex<void> {};

        template<typename COMP, typename T, typename U>
        bool evaluate_comp(T left, U right) requires(!std::is_same_v<COMP, regex<>>) {
            return COMP{}(left, right);
        }

        template<typename COMP, typename T, typename U>
        bool evaluate_comp(T, U) requires(std::is_same_v<COMP, regex<>>) {
            throw std::runtime_error("incorrect argument type for regex");
        }

        template<typename COMP>
        bool evaluate_comp(std::string_view left, std::string_view right) requires(std::is_same_v<COMP, regex<>>) {
            return std::regex_search(std::string(left), std::regex(std::string(right)));
        }

        template<typename COMP>
        bool evaluate_comp(const types::logical_value_t& left,
                           const types::logical_value_t& right) requires(std::is_same_v<COMP, regex<>>) {
            return evaluate_comp<COMP>(left.value<std::string_view>(), right.value<std::string_view>());
        }

        // Typed fast path: compare a flat column directly against a constant, no boxing.
        // Returns nullopt if the pattern doesn't match (falls back to boxing path).
        template<typename COMP, typename T>
        simple_predicate::check_function_t make_typed_comparator(size_t col_idx, T constant) {
            return [col_idx, constant](const vector::data_chunk_t& chunk_left,
                                       const vector::data_chunk_t&,
                                       size_t index_left,
                                       size_t) {
                const auto& vec = chunk_left.data[col_idx];
                if (!vec.validity().row_is_valid(index_left)) return false;
                return COMP{}(vec.data<T>()[index_left], constant);
            };
        }

        // String specialization: store std::string, compare as string_view
        template<typename COMP>
        simple_predicate::check_function_t make_typed_comparator_str(size_t col_idx, std::string constant) {
            return [col_idx, constant = std::move(constant)](const vector::data_chunk_t& chunk_left,
                                                              const vector::data_chunk_t&,
                                                              size_t index_left,
                                                              size_t) {
                const auto& vec = chunk_left.data[col_idx];
                if (!vec.validity().row_is_valid(index_left)) return false;
                return COMP{}(vec.data<std::string_view>()[index_left], std::string_view(constant));
            };
        }

        template<typename COMP>
        std::optional<simple_predicate::check_function_t>
        try_typed_comparator(const expressions::compare_expression_ptr& expr,
                             const std::pmr::vector<types::complex_logical_type>& types_left,
                             const logical_plan::storage_parameters* parameters) {
            if (!parameters) return std::nullopt;
            // Pattern: left = key_t, right = parameter_id_t (constant)
            if (!std::holds_alternative<expressions::key_t>(expr->left())) return std::nullopt;
            if (!std::holds_alternative<core::parameter_id_t>(expr->right())) return std::nullopt;

            const auto& key = std::get<expressions::key_t>(expr->left());
            if (key.side() != expressions::side_t::left) return std::nullopt;
            if (key.path().size() != 1) return std::nullopt;
            size_t col_idx = key.path()[0];
            if (col_idx >= types_left.size()) return std::nullopt;

            auto id = std::get<core::parameter_id_t>(expr->right());
            auto it = parameters->parameters.find(id);
            if (it == parameters->parameters.end()) return std::nullopt;
            const auto& constant = it->second;

            using PT = types::physical_type;
            switch (types_left[col_idx].to_physical_type()) {
                case PT::INT8:   return make_typed_comparator<COMP, int8_t>(col_idx, constant.value<int8_t>());
                case PT::INT16:  return make_typed_comparator<COMP, int16_t>(col_idx, constant.value<int16_t>());
                case PT::INT32:  return make_typed_comparator<COMP, int32_t>(col_idx, constant.value<int32_t>());
                case PT::INT64:  return make_typed_comparator<COMP, int64_t>(col_idx, constant.value<int64_t>());
                case PT::UINT8:  return make_typed_comparator<COMP, uint8_t>(col_idx, constant.value<uint8_t>());
                case PT::UINT16: return make_typed_comparator<COMP, uint16_t>(col_idx, constant.value<uint16_t>());
                case PT::UINT32: return make_typed_comparator<COMP, uint32_t>(col_idx, constant.value<uint32_t>());
                case PT::UINT64: return make_typed_comparator<COMP, uint64_t>(col_idx, constant.value<uint64_t>());
                case PT::FLOAT:  return make_typed_comparator<COMP, float>(col_idx, constant.value<float>());
                case PT::DOUBLE: return make_typed_comparator<COMP, double>(col_idx, constant.value<double>());
                case PT::STRING:
                    return make_typed_comparator_str<COMP>(col_idx, std::string(constant.value<std::string_view>()));
                default: return std::nullopt;
            }
        }

        template<typename COMP>
        simple_predicate::check_function_t make_comparator(std::pmr::memory_resource* resource,
                                                           const compute::function_registry_t* function_registry,
                                                           const expressions::compare_expression_ptr& expr,
                                                           const logical_plan::storage_parameters* parameters) {
            auto left_getter = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
            auto right_getter = impl::create_value_getter(resource, function_registry, expr->right(), parameters);
            return [left_getter = std::move(left_getter),
                    right_getter = std::move(right_getter)](const vector::data_chunk_t& chunk_left,
                                                            const vector::data_chunk_t& chunk_right,
                                                            size_t index_left,
                                                            size_t index_right) {
                auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
                auto right_val = right_getter(chunk_left, chunk_right, index_left, index_right);
                // TODO: by SQL standard any comparison with NULL yields UNKNOWN, which is neither true or false
                if (left_val.is_null() || right_val.is_null()) {
                    return false;
                }
                return evaluate_comp<COMP>(left_val, right_val);
            };
        }

    } // anonymous namespace

    simple_predicate::simple_predicate(check_function_t func)
        : func_(std::move(func)) {}

    simple_predicate::simple_predicate(std::vector<predicate_ptr>&& nested, expressions::compare_type nested_type)
        : nested_(std::move(nested))
        , nested_type_(nested_type) {}

    bool simple_predicate::check_impl(const vector::data_chunk_t& chunk_left,
                                      const vector::data_chunk_t& chunk_right,
                                      size_t index_left,
                                      size_t index_right) {
        switch (nested_type_) {
            case expressions::compare_type::union_and:
                for (const auto& predicate : nested_) {
                    if (!predicate->check(chunk_left, chunk_right, index_left, index_right)) {
                        return false;
                    }
                }
                return true;
            case expressions::compare_type::union_or:
                for (const auto& predicate : nested_) {
                    if (predicate->check(chunk_left, chunk_right, index_left, index_right)) {
                        return true;
                    }
                }
                return false;
            case expressions::compare_type::union_not:
                return !nested_.front()->check(chunk_left, chunk_right, index_left, index_right);
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
                std::vector<predicate_ptr> nested;
                nested.reserve(expr->children().size());
                for (const auto& nested_expr : expr->children()) {
                    nested.emplace_back(create_predicate(resource,
                                                         function_registry,
                                                         nested_expr,
                                                         types_left,
                                                         types_right,
                                                         parameters));
                }
                return {new simple_predicate(std::move(nested), expr->type())};
            }
            case compare_type::eq:
                if (auto f = try_typed_comparator<std::equal_to<>>(expr, types_left, parameters))
                    return {new simple_predicate(std::move(*f))};
                return {new simple_predicate(
                    make_comparator<std::equal_to<>>(resource, function_registry, expr, parameters))};
            case compare_type::ne:
                if (auto f = try_typed_comparator<std::not_equal_to<>>(expr, types_left, parameters))
                    return {new simple_predicate(std::move(*f))};
                return {new simple_predicate(
                    make_comparator<std::not_equal_to<>>(resource, function_registry, expr, parameters))};
            case compare_type::gt:
                if (auto f = try_typed_comparator<std::greater<>>(expr, types_left, parameters))
                    return {new simple_predicate(std::move(*f))};
                return {new simple_predicate(
                    make_comparator<std::greater<>>(resource, function_registry, expr, parameters))};
            case compare_type::gte:
                if (auto f = try_typed_comparator<std::greater_equal<>>(expr, types_left, parameters))
                    return {new simple_predicate(std::move(*f))};
                return {new simple_predicate(
                    make_comparator<std::greater_equal<>>(resource, function_registry, expr, parameters))};
            case compare_type::lt:
                if (auto f = try_typed_comparator<std::less<>>(expr, types_left, parameters))
                    return {new simple_predicate(std::move(*f))};
                return {
                    new simple_predicate(make_comparator<std::less<>>(resource, function_registry, expr, parameters))};
            case compare_type::lte:
                if (auto f = try_typed_comparator<std::less_equal<>>(expr, types_left, parameters))
                    return {new simple_predicate(std::move(*f))};
                return {new simple_predicate(
                    make_comparator<std::less_equal<>>(resource, function_registry, expr, parameters))};
            case compare_type::regex:
                return {new simple_predicate(make_comparator<regex<>>(resource, function_registry, expr, parameters))};
            case compare_type::all_false:
                return {new simple_predicate(
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; })};
            case compare_type::is_null: {
                return {new simple_predicate(
                    [column_path = std::get<expressions::key_t>(expr->left()).path()](
                        const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t&,
                        size_t index_left,
                        size_t) { return !chunk_left.at(column_path)->validity().row_is_valid(index_left); })};
            }
            case compare_type::is_not_null: {
                return {new simple_predicate(
                    [column_path = std::get<expressions::key_t>(expr->left()).path()](
                        const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t&,
                        size_t index_left,
                        size_t) { return chunk_left.at(column_path)->validity().row_is_valid(index_left); })};
            }
            case compare_type::json_has_key: {
                // Postgres jsonb `?` semantics: missing column → false, else is_not_null.
                auto path = std::get<expressions::key_t>(expr->left()).path();
                if (path.empty()) {
                    return {new simple_predicate(
                        [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) {
                            return false;
                        })};
                }
                return {new simple_predicate(
                    [column_path = std::move(path)](const vector::data_chunk_t& chunk_left,
                                                    const vector::data_chunk_t&,
                                                    size_t index_left,
                                                    size_t) {
                        return chunk_left.at(column_path)->validity().row_is_valid(index_left);
                    })};
            }
            case compare_type::all_true:
            default:
                return {new simple_predicate(
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return true; })};
        }
    }

} // namespace components::operators::predicates

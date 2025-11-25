#include "simple_predicate.hpp"
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/types/operations_helper.hpp>
#include <fmt/format.h>
#include <regex>

namespace components::table::operators::predicates {

    namespace impl {

        size_t get_column_index(const expressions::key_t& key,
                                const std::pmr::vector<types::complex_logical_type>& types) {
            size_t column_index = -1;
            if (key.is_string()) {
                column_index = static_cast<size_t>(std::find_if(types.cbegin(),
                                                                types.cend(),
                                                                [&](const types::complex_logical_type& type) {
                                                                    return type.alias() == key.as_string();
                                                                }) -
                                                   types.cbegin());
            } else if (key.is_int()) {
                column_index = static_cast<size_t>(key.as_int());
            } else if (key.is_uint()) {
                column_index = key.as_uint();
            }
            if (column_index >= types.size()) {
                column_index = -1;
            }
            return column_index;
        }

        // simple check if types are comparable, otherwise we will return an exception
        template<typename, typename, typename = void>
        struct has_less_operator : std::false_type {};

        template<typename T, typename U>
        struct has_less_operator<T, U, std::void_t<decltype(std::declval<T>() < std::declval<U>())>>
            : std::true_type {};

        template<typename T = void>
        struct create_binary_comparator_t;
        template<typename T = void>
        struct create_unary_comparator_t;

        template<>
        struct create_unary_comparator_t<void> {
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<has_less_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&& comp,
                            size_t column_index,
                            expressions::side_t side,
                            const logical_plan::expr_value_t& value) const -> simple_predicate::check_function_t {
                return [comp, column_index, side, &value](const vector::data_chunk_t& chunk_left,
                                                          const vector::data_chunk_t& chunk_right,
                                                          size_t index_left,
                                                          size_t index_right) {
                    assert(column_index < chunk_left.column_count());
                    if (side == expressions::side_t::left) {
                        assert(column_index < chunk_left.column_count());
                        return comp(chunk_left.data.at(column_index).data<LeftType>()[index_left],
                                    value.value<RightType>());
                    } else {
                        assert(column_index < chunk_right.column_count());
                        return comp(chunk_right.data.at(column_index).data<LeftType>()[index_right],
                                    value.value<RightType>());
                    }
                };
            }
            // SFINAE unable to compare types
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<!has_less_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&&, size_t, expressions::side_t, const logical_plan::expr_value_t&) const
                -> simple_predicate::check_function_t {
                throw std::runtime_error("invalid expression in create_unary_comparator");
            }
        };

        template<>
        struct create_binary_comparator_t<void> {
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<has_less_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&& comp, size_t column_index_left, size_t column_index_right, bool one_sided) const
                -> simple_predicate::check_function_t {
                return [comp, column_index_left, column_index_right, one_sided](const vector::data_chunk_t& chunk_left,
                                                                                const vector::data_chunk_t& chunk_right,
                                                                                size_t index_left,
                                                                                size_t index_right) {
                    if (one_sided) {
                        assert(column_index_left < chunk_left.column_count());
                        assert(column_index_right < chunk_left.column_count());
                        return comp(chunk_left.data.at(column_index_left).data<LeftType>()[index_left],
                                    chunk_left.data.at(column_index_right).data<RightType>()[index_left]);
                    } else {
                        assert(column_index_left < chunk_left.column_count());
                        assert(column_index_right < chunk_right.column_count());
                        return comp(chunk_left.data.at(column_index_left).data<LeftType>()[index_left],
                                    chunk_right.data.at(column_index_right).data<RightType>()[index_right]);
                    }
                };
            }
            // SFINAE unable to compare types
            template<typename LeftType,
                     typename RightType,
                     typename... Args,
                     std::enable_if_t<!has_less_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(Args&&...) const -> simple_predicate::check_function_t {
                throw std::runtime_error("invalid expression in create_binary_comparator");
            }
        };

        // by this point compare_expression is unmodifiable, so we have to pass side explicitly
        template<typename COMP>
        simple_predicate::check_function_t
        create_unary_comparator(const expressions::compare_expression_ptr& expr,
                                const std::pmr::vector<types::complex_logical_type>& types,
                                const logical_plan::storage_parameters* parameters,
                                expressions::side_t side) {
            size_t column_index = get_column_index(expr->primary_key(), types);
            const auto& expr_val = parameters->parameters.at(expr->value());

            auto type_left = types.at(column_index).to_physical_type();
            auto type_right = expr_val.type().to_physical_type();

            return types::double_simple_physical_type_switch<create_unary_comparator_t>(type_left,
                                                                                        type_right,
                                                                                        COMP{},
                                                                                        column_index,
                                                                                        side,
                                                                                        expr_val);
        }

        simple_predicate::check_function_t
        create_unary_regex_comparator(const expressions::compare_expression_ptr& expr,
                                      const std::pmr::vector<types::complex_logical_type>& types,
                                      const logical_plan::storage_parameters* parameters,
                                      expressions::side_t side) {
            assert(side != expressions::side_t::undefined);
            size_t column_index = get_column_index(expr->primary_key(), types);
            auto expr_val = parameters->parameters.at(expr->value());

            return
                [column_index, val = expr_val.value<std::string_view>(), side](const vector::data_chunk_t& chunk_left,
                                                                               const vector::data_chunk_t& chunk_right,
                                                                               size_t index_left,
                                                                               size_t index_right) {
                    if (side == expressions::side_t::left) {
                        assert(column_index < chunk_left.column_count());
                        return std::regex_match(
                            chunk_left.data.at(column_index).data<std::string_view>()[index_left].data(),
                            std::regex(fmt::format(".*{}.*", val)));
                    } else {
                        assert(column_index < chunk_right.column_count());
                        return std::regex_match(
                            chunk_right.data.at(column_index).data<std::string_view>()[index_right].data(),
                            std::regex(fmt::format(".*{}.*", val)));
                    }
                };
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_binary_comparator(const expressions::compare_expression_ptr& expr,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right) {
            bool one_sided = false;
            size_t column_index_left = get_column_index(expr->primary_key(), types_left);
            size_t column_index_right = get_column_index(expr->secondary_key(), types_right);
            types::physical_type type_left = types_left.at(column_index_left).to_physical_type();
            types::physical_type type_right;
            if (column_index_right == -1) {
                // one-sided expr
                column_index_right = get_column_index(expr->secondary_key(), types_left);
                one_sided = true;
                type_right = types_left.at(column_index_right).to_physical_type();
            } else {
                type_right = types_right.at(column_index_right).to_physical_type();
            }

            return types::double_simple_physical_type_switch<create_binary_comparator_t>(type_left,
                                                                                         type_right,
                                                                                         COMP{},
                                                                                         column_index_left,
                                                                                         column_index_right,
                                                                                         one_sided);
        }

        simple_predicate::check_function_t
        create_binary_regex_comparator(const expressions::compare_expression_ptr& expr,
                                       const std::pmr::vector<types::complex_logical_type>& types_left,
                                       const std::pmr::vector<types::complex_logical_type>& types_right) {
            bool one_sided = false;
            size_t column_index_left = get_column_index(expr->primary_key(), types_left);
            size_t column_index_right = get_column_index(expr->secondary_key(), types_right);
            if (column_index_right == -1) {
                // one-sided expr
                column_index_right = get_column_index(expr->secondary_key(), types_left);
            }

            return [column_index_left, column_index_right, one_sided](const vector::data_chunk_t& chunk_left,
                                                                      const vector::data_chunk_t& chunk_right,
                                                                      size_t index_left,
                                                                      size_t index_right) {
                if (one_sided) {
                    return std::regex_match(
                        chunk_left.data.at(column_index_left).data<std::string_view>()[index_left].data(),
                        std::regex(fmt::format(
                            ".*{}.*",
                            chunk_left.data.at(column_index_right).data<std::string_view>()[index_left].data())));
                } else {
                    return std::regex_match(
                        chunk_left.data.at(column_index_left).data<std::string_view>()[index_left].data(),
                        std::regex(fmt::format(
                            ".*{}.*",
                            chunk_right.data.at(column_index_right).data<std::string_view>()[index_right].data())));
                }
            };
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_comparator(const expressions::compare_expression_ptr& expr,
                          const std::pmr::vector<types::complex_logical_type>& types_left,
                          const std::pmr::vector<types::complex_logical_type>& types_right,
                          const logical_plan::storage_parameters* parameters) {
            // TODO: use schema to determine expr side before this
            if (!expr->primary_key().is_null() && !expr->secondary_key().is_null()) {
                return create_binary_comparator<COMP>(expr, types_left, types_right);
            } else {
                if (expr->primary_key().side() == expressions::side_t::left) {
                    return create_unary_comparator<COMP>(expr, types_left, parameters, expressions::side_t::left);
                } else if (expr->primary_key().side() == expressions::side_t::right) {
                    return create_unary_comparator<COMP>(expr, types_right, parameters, expressions::side_t::right);
                } else {
                    auto it = std::find_if(types_left.begin(),
                                           types_left.end(),
                                           [&expr](const types::complex_logical_type& type) {
                                               return type.alias() == expr->primary_key().as_string();
                                           });
                    if (it != types_left.end()) {
                        return create_unary_comparator<COMP>(expr, types_left, parameters, expressions::side_t::left);
                    }
                    it = std::find_if(types_right.begin(),
                                      types_right.end(),
                                      [&expr](const types::complex_logical_type& type) {
                                          return type.alias() == expr->primary_key().as_string();
                                      });
                    if (it != types_right.end()) {
                        // undefined sided expressions store value on the left side by default
                        return create_unary_comparator<COMP>(expr, types_right, parameters, expressions::side_t::left);
                    }
                }
            }

            return [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; };
        }

        simple_predicate::check_function_t
        create_regex_comparator(const expressions::compare_expression_ptr& expr,
                                const std::pmr::vector<types::complex_logical_type>& types_left,
                                const std::pmr::vector<types::complex_logical_type>& types_right,
                                const logical_plan::storage_parameters* parameters) {
            // TODO: use schema to determine expr side before this
            if (!expr->primary_key().is_null() && !expr->secondary_key().is_null()) {
                return create_binary_regex_comparator(expr, types_left, types_right);
            } else {
                if (expr->primary_key().side() == expressions::side_t::left) {
                    return create_unary_regex_comparator(expr, types_left, parameters, expressions::side_t::left);
                } else if (expr->primary_key().side() == expressions::side_t::right) {
                    return create_unary_regex_comparator(expr, types_right, parameters, expressions::side_t::right);
                } else {
                    auto it = std::find_if(types_left.begin(),
                                           types_left.end(),
                                           [&expr](const types::complex_logical_type& type) {
                                               return type.alias() == expr->primary_key().as_string();
                                           });
                    if (it != types_left.end()) {
                        return create_unary_regex_comparator(expr, types_left, parameters, expressions::side_t::left);
                    }
                    it = std::find_if(types_right.begin(),
                                      types_right.end(),
                                      [&expr](const types::complex_logical_type& type) {
                                          return type.alias() == expr->primary_key().as_string();
                                      });
                    if (it != types_right.end()) {
                        // undefined sided expressions store value on the left side by default
                        return create_unary_regex_comparator(expr, types_right, parameters, expressions::side_t::left);
                    }
                }
            }

            return [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; };
        }

    } // namespace impl

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

    predicate_ptr create_simple_predicate(const expressions::compare_expression_ptr& expr,
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
                    nested.emplace_back(create_simple_predicate(
                        reinterpret_cast<const expressions::compare_expression_ptr&>(nested_expr),
                        types_left,
                        types_right,
                        parameters));
                }
                return {new simple_predicate(std::move(nested), expr->type())};
            }
            case compare_type::eq:
                return {new simple_predicate(
                    impl::create_comparator<std::equal_to<>>(expr, types_left, types_right, parameters))};
            case compare_type::ne:
                return {new simple_predicate(
                    impl::create_comparator<std::not_equal_to<>>(expr, types_left, types_right, parameters))};
            case compare_type::gt:
                return {new simple_predicate(
                    impl::create_comparator<std::greater<>>(expr, types_left, types_right, parameters))};
            case compare_type::gte:
                return {new simple_predicate(
                    impl::create_comparator<std::greater_equal<>>(expr, types_left, types_right, parameters))};
            case compare_type::lt:
                return {new simple_predicate(
                    impl::create_comparator<std::less<>>(expr, types_left, types_right, parameters))};
            case compare_type::lte:
                return {new simple_predicate(
                    impl::create_comparator<std::less_equal<>>(expr, types_left, types_right, parameters))};
            case compare_type::regex: {
                return {new simple_predicate(impl::create_regex_comparator(expr, types_left, types_right, parameters))};
            }
            case compare_type::all_true:
                return {new simple_predicate(
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return true; })};
            case compare_type::all_false:
                return {new simple_predicate(
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; })};
            default:
                break;
        }
        return {new simple_predicate(
            [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return true; })};
    }

} // namespace components::table::operators::predicates

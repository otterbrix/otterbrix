#include "simple_predicate.hpp"
#include <components/physical_plan/base/operators/operator.hpp>
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

        template<typename COMP, typename TYPE, typename VALUE>
        simple_predicate::check_function_t
        create_unary_comparator(size_t column_index, expressions::side_t side, VALUE&& val) {
            return [column_index, val, side](const vector::data_chunk_t& chunk_left,
                                             const vector::data_chunk_t& chunk_right,
                                             size_t index_left,
                                             size_t index_right) {
                assert(column_index < chunk_left.column_count());
                COMP comp{};
                if (side == expressions::side_t::left) {
                    assert(column_index < chunk_left.column_count());
                    return comp(chunk_left.data.at(column_index).data<TYPE>()[index_left], val);
                } else {
                    assert(column_index < chunk_right.column_count());
                    return comp(chunk_right.data.at(column_index).data<TYPE>()[index_right], val);
                }
            };
        }

        template<typename COMP, typename LEFT_TYPE, typename RIGHT_TYPE>
        simple_predicate::check_function_t
        create_binary_comparator(size_t column_index_left, size_t column_index_right, bool one_sided) {
            return [column_index_left, column_index_right, one_sided](const vector::data_chunk_t& chunk_left,
                                                                      const vector::data_chunk_t& chunk_right,
                                                                      size_t index_left,
                                                                      size_t index_right) {
                COMP comp{};
                if (one_sided) {
                    assert(column_index_left < chunk_left.column_count());
                    assert(column_index_right < chunk_left.column_count());
                    return comp(chunk_left.data.at(column_index_left).data<LEFT_TYPE>()[index_left],
                                chunk_left.data.at(column_index_right).data<RIGHT_TYPE>()[index_left]);
                } else {
                    assert(column_index_left < chunk_left.column_count());
                    assert(column_index_right < chunk_right.column_count());
                    return comp(chunk_left.data.at(column_index_left).data<LEFT_TYPE>()[index_left],
                                chunk_right.data.at(column_index_right).data<RIGHT_TYPE>()[index_right]);
                }
            };
        }

        // by this point compare_expression is unmodifiable, so we have to pass side explicitly
        template<typename COMP>
        simple_predicate::check_function_t
        create_unary_comparator(const expressions::compare_expression_ptr& expr,
                                const std::pmr::vector<types::complex_logical_type>& types,
                                const logical_plan::storage_parameters* parameters,
                                expressions::side_t side) {
            assert(side != expressions::side_t::undefined);
            size_t column_index =
                get_column_index(side == expressions::side_t::left ? expr->key_left() : expr->key_right(), types);
            const auto& expr_val = parameters->parameters.at(expr->value());

            switch (types.at(column_index).to_physical_type()) {
                case types::physical_type::BOOL:
                    return create_unary_comparator<COMP, bool>(column_index, side, expr_val.as_bool());
                case types::physical_type::UINT8:
                    return create_unary_comparator<COMP, uint8_t>(column_index, side, expr_val.as_unsigned());
                case types::physical_type::INT8:
                    return create_unary_comparator<COMP, int8_t>(column_index, side, expr_val.as_int());
                case types::physical_type::UINT16:
                    return create_unary_comparator<COMP, uint16_t>(column_index, side, expr_val.as_unsigned());
                case types::physical_type::INT16:
                    return create_unary_comparator<COMP, int16_t>(column_index, side, expr_val.as_int());
                case types::physical_type::UINT32:
                    return create_unary_comparator<COMP, uint32_t>(column_index, side, expr_val.as_unsigned());
                case types::physical_type::INT32:
                    return create_unary_comparator<COMP, int32_t>(column_index, side, expr_val.as_int());
                case types::physical_type::UINT64:
                    return create_unary_comparator<COMP, uint64_t>(column_index, side, expr_val.as_unsigned());
                case types::physical_type::INT64:
                    return create_unary_comparator<COMP, int64_t>(column_index, side, expr_val.as_int());
                // case types::physical_type::UINT128:
                //    return create_unary_comparator<COMP, types::uint128_t>(column_index, side, expr_val.as_int128());
                // case types::physical_type::INT128:
                //    return create_unary_comparator<COMP, types::int128_t>(column_index, side, expr_val.as_int128());
                case types::physical_type::FLOAT:
                    return create_unary_comparator<COMP, float>(column_index, side, expr_val.as_float());
                case types::physical_type::DOUBLE:
                    return create_unary_comparator<COMP, double>(column_index, side, expr_val.as_double());
                case types::physical_type::STRING:
                    return create_unary_comparator<COMP, std::string_view>(column_index, side, expr_val.as_string());
                default:
                    throw std::runtime_error("invalid expression in create_unary_comparator");
            }
        }

        simple_predicate::check_function_t
        create_unary_regex_comparator(const expressions::compare_expression_ptr& expr,
                                      const std::pmr::vector<types::complex_logical_type>& types,
                                      const logical_plan::storage_parameters* parameters,
                                      expressions::side_t side) {
            assert(side != expressions::side_t::undefined);
            size_t column_index =
                get_column_index(side == expressions::side_t::left ? expr->key_left() : expr->key_right(), types);
            auto expr_val = parameters->parameters.at(expr->value());

            return [column_index, val = expr_val.as_string(), side](const vector::data_chunk_t& chunk_left,
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

        template<typename COMP, typename LEFT_TYPE>
        simple_predicate::check_function_t create_binary_comparator_inner_switch(types::physical_type type_right,
                                                                                 size_t column_index_left,
                                                                                 size_t column_index_right,
                                                                                 bool one_sided) {
            switch (type_right) {
                case types::physical_type::BOOL:
                    return create_binary_comparator<COMP, LEFT_TYPE, bool>(column_index_left,
                                                                           column_index_right,
                                                                           one_sided);
                case types::physical_type::UINT8:
                    return create_binary_comparator<COMP, LEFT_TYPE, uint8_t>(column_index_left,
                                                                              column_index_right,
                                                                              one_sided);
                case types::physical_type::INT8:
                    return create_binary_comparator<COMP, LEFT_TYPE, int8_t>(column_index_left,
                                                                             column_index_right,
                                                                             one_sided);
                case types::physical_type::UINT16:
                    return create_binary_comparator<COMP, LEFT_TYPE, uint16_t>(column_index_left,
                                                                               column_index_right,
                                                                               one_sided);
                case types::physical_type::INT16:
                    return create_binary_comparator<COMP, LEFT_TYPE, int16_t>(column_index_left,
                                                                              column_index_right,
                                                                              one_sided);
                case types::physical_type::UINT32:
                    return create_binary_comparator<COMP, LEFT_TYPE, uint32_t>(column_index_left,
                                                                               column_index_right,
                                                                               one_sided);
                case types::physical_type::INT32:
                    return create_binary_comparator<COMP, LEFT_TYPE, int32_t>(column_index_left,
                                                                              column_index_right,
                                                                              one_sided);
                case types::physical_type::UINT64:
                    return create_binary_comparator<COMP, LEFT_TYPE, uint64_t>(column_index_left,
                                                                               column_index_right,
                                                                               one_sided);
                case types::physical_type::INT64:
                    return create_binary_comparator<COMP, LEFT_TYPE, int64_t>(column_index_left,
                                                                              column_index_right,
                                                                              one_sided);
                // case types::physical_type::UINT128:
                //    return create_binary_comparator<COMP, LEFT_TYPE, types::uint128_t>(column_index_left, column_index_right, one_sided);
                // case types::physical_type::INT128:
                //    return create_binary_comparator<COMP, LEFT_TYPE, types::int128_t>(column_index_left, column_index_right, one_sided);
                case types::physical_type::FLOAT:
                    return create_binary_comparator<COMP, LEFT_TYPE, float>(column_index_left,
                                                                            column_index_right,
                                                                            one_sided);
                case types::physical_type::DOUBLE:
                    return create_binary_comparator<COMP, LEFT_TYPE, double>(column_index_left,
                                                                             column_index_right,
                                                                             one_sided);
                default:
                    throw std::runtime_error("invalid expression in create_binary_comparator");
            }
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_binary_comparator(const expressions::compare_expression_ptr& expr,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right) {
            bool one_sided = false;
            size_t column_index_left = get_column_index(expr->key_left(), types_left);
            size_t column_index_right = get_column_index(expr->key_right(), types_right);
            types::physical_type type_right;
            if (column_index_right == -1) {
                // one-sided expr
                column_index_right = get_column_index(expr->key_right(), types_left);
                one_sided = true;
                type_right = types_left.at(column_index_right).to_physical_type();
                assert(types_left.at(column_index_left).to_physical_type() == type_right);
            } else {
                type_right = types_right.at(column_index_right).to_physical_type();
                assert(types_left.at(column_index_left).to_physical_type() == type_right);
            }

            switch (types_left.at(column_index_left).to_physical_type()) {
                case types::physical_type::BOOL:
                    return create_binary_comparator_inner_switch<COMP, bool>(type_right,
                                                                             column_index_left,
                                                                             column_index_right,
                                                                             one_sided);
                case types::physical_type::UINT8:
                    return create_binary_comparator_inner_switch<COMP, uint8_t>(type_right,
                                                                                column_index_left,
                                                                                column_index_right,
                                                                                one_sided);
                case types::physical_type::INT8:
                    return create_binary_comparator_inner_switch<COMP, int8_t>(type_right,
                                                                               column_index_left,
                                                                               column_index_right,
                                                                               one_sided);
                case types::physical_type::UINT16:
                    return create_binary_comparator_inner_switch<COMP, uint16_t>(type_right,
                                                                                 column_index_left,
                                                                                 column_index_right,
                                                                                 one_sided);
                case types::physical_type::INT16:
                    return create_binary_comparator_inner_switch<COMP, int16_t>(type_right,
                                                                                column_index_left,
                                                                                column_index_right,
                                                                                one_sided);
                case types::physical_type::UINT32:
                    return create_binary_comparator_inner_switch<COMP, uint32_t>(type_right,
                                                                                 column_index_left,
                                                                                 column_index_right,
                                                                                 one_sided);
                case types::physical_type::INT32:
                    return create_binary_comparator_inner_switch<COMP, int32_t>(type_right,
                                                                                column_index_left,
                                                                                column_index_right,
                                                                                one_sided);
                case types::physical_type::UINT64:
                    return create_binary_comparator_inner_switch<COMP, uint64_t>(type_right,
                                                                                 column_index_left,
                                                                                 column_index_right,
                                                                                 one_sided);
                case types::physical_type::INT64:
                    return create_binary_comparator_inner_switch<COMP, int64_t>(type_right,
                                                                                column_index_left,
                                                                                column_index_right,
                                                                                one_sided);
                // case types::physical_type::UINT128:
                //    return create_binary_comparator_inner_switch<COMP, types::uint128_t>(type_right, column_index_left, column_index_right, one_sided);
                // case types::physical_type::INT128:
                //    return create_binary_comparator_inner_switch<COMP, types::int128_t>(type_right, column_index_left, column_index_right, one_sided);
                case types::physical_type::FLOAT:
                    return create_binary_comparator_inner_switch<COMP, float>(type_right,
                                                                              column_index_left,
                                                                              column_index_right,
                                                                              one_sided);
                case types::physical_type::DOUBLE:
                    return create_binary_comparator_inner_switch<COMP, double>(type_right,
                                                                               column_index_left,
                                                                               column_index_right,
                                                                               one_sided);
                case types::physical_type::STRING:
                    return create_binary_comparator<COMP, std::string_view, std::string_view>(column_index_left,
                                                                                              column_index_right,
                                                                                              one_sided);
                default:
                    throw std::runtime_error("invalid expression in create_binary_comparator");
            }
        }

        simple_predicate::check_function_t
        create_binary_regex_comparator(const expressions::compare_expression_ptr& expr,
                                       const std::pmr::vector<types::complex_logical_type>& types_left,
                                       const std::pmr::vector<types::complex_logical_type>& types_right) {
            bool one_sided = false;
            size_t column_index_left = get_column_index(expr->key_left(), types_left);
            size_t column_index_right = get_column_index(expr->key_right(), types_right);
            if (column_index_right == -1) {
                // one-sided expr
                column_index_right = get_column_index(expr->key_right(), types_left);
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
            if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                return create_binary_comparator<COMP>(expr, types_left, types_right);
            } else {
                if (expr->side() == expressions::side_t::left) {
                    return create_unary_comparator<COMP>(expr, types_left, parameters, expressions::side_t::left);
                } else if (expr->side() == expressions::side_t::right) {
                    return create_unary_comparator<COMP>(expr, types_right, parameters, expressions::side_t::right);
                } else {
                    auto it = std::find_if(types_left.begin(),
                                           types_left.end(),
                                           [&expr](const types::complex_logical_type& type) {
                                               return type.alias() == expr->key_left().as_string();
                                           });
                    if (it != types_left.end()) {
                        return create_unary_comparator<COMP>(expr, types_left, parameters, expressions::side_t::left);
                    }
                    it = std::find_if(types_right.begin(),
                                      types_right.end(),
                                      [&expr](const types::complex_logical_type& type) {
                                          return type.alias() == expr->key_left().as_string();
                                      });
                    if (it != types_right.end()) {
                        return create_unary_comparator<COMP>(expr, types_right, parameters, expressions::side_t::right);
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
            if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                return create_binary_regex_comparator(expr, types_left, types_right);
            } else {
                if (expr->side() == expressions::side_t::left) {
                    return create_unary_regex_comparator(expr, types_left, parameters, expressions::side_t::left);
                } else if (expr->side() == expressions::side_t::right) {
                    return create_unary_regex_comparator(expr, types_right, parameters, expressions::side_t::right);
                } else {
                    auto it = std::find_if(types_left.begin(),
                                           types_left.end(),
                                           [&expr](const types::complex_logical_type& type) {
                                               return type.alias() == expr->key_left().as_string();
                                           });
                    if (it != types_left.end()) {
                        return create_unary_regex_comparator(expr, types_left, parameters, expressions::side_t::left);
                    }
                    it = std::find_if(types_right.begin(),
                                      types_right.end(),
                                      [&expr](const types::complex_logical_type& type) {
                                          return type.alias() == expr->key_left().as_string();
                                      });
                    if (it != types_right.end()) {
                        return create_unary_regex_comparator(expr, types_right, parameters, expressions::side_t::right);
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

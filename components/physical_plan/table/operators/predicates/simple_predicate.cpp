#include "simple_predicate.hpp"
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/types/operations_helper.hpp>
#include <fmt/format.h>
#include <regex>

namespace components::table::operators::predicates {

    namespace impl {

        std::pair<std::pmr::vector<size_t>, const types::complex_logical_type*>
        get_column_path(std::pmr::memory_resource* resource,
                        const expressions::key_t& key,
                        const std::pmr::vector<types::complex_logical_type>& types) {
            std::pmr::vector<size_t> res(resource);
            for (uint64_t i = 0; i < types.size(); i++) {
                if (types[i].alias() == key.storage().front()) {
                    res.emplace_back(i);
                    break;
                }
            }
            if (res.empty()) {
                assert(false && "data_chunk_t::column_index: no such column");
                return {{size_t(-1)}, nullptr};
            } else {
                const types::complex_logical_type* sub_column = &types[res.front()];
                for (auto it = std::next(key.storage().begin()); it != key.storage().end(); ++it) {
                    bool field_found = false;
                    for (uint64_t i = 0; i < sub_column->child_types().size(); i++) {
                        if (sub_column->child_types()[i].alias() == *it) {
                            res.emplace_back(i);
                            sub_column = &sub_column->child_types()[i];
                            field_found = true;
                            break;
                        }
                    }
                    if (!field_found) {
                        return {{size_t(-1)}, nullptr};
                    }
                }
                return {res, sub_column};
            }
        }

        template<typename T = void>
        struct create_binary_comparator_t;
        template<typename T = void>
        struct create_unary_comparator_t;

        template<>
        struct create_unary_comparator_t<void> {
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&& comp,
                            std::pmr::vector<size_t> column_path,
                            expressions::side_t side,
                            const logical_plan::expr_value_t& value) const -> simple_predicate::check_function_t {
                return [comp, column_path = std::move(column_path), side, &value](
                           const vector::data_chunk_t& chunk_left,
                           const vector::data_chunk_t& chunk_right,
                           size_t index_left,
                           size_t index_right) {
                    assert(column_path.front() < chunk_left.column_count());
                    if (side == expressions::side_t::left) {
                        assert(column_path.front() < chunk_left.column_count());
                        return comp(chunk_left.at(column_path)->data<LeftType>()[index_left], value.value<RightType>());
                    } else {
                        assert(column_path.front() < chunk_right.column_count());
                        return comp(chunk_right.at(column_path)->data<LeftType>()[index_right],
                                    value.value<RightType>());
                    }
                };
            }
            // SFINAE unable to compare types
            template<typename LeftType,
                     typename RightType,
                     typename... Args,
                     std::enable_if_t<!core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(Args&&...) const -> simple_predicate::check_function_t {
                throw std::runtime_error("invalid expression in create_unary_comparator");
            }
        };

        template<>
        struct create_binary_comparator_t<void> {
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&& comp,
                            std::pmr::vector<size_t> column_path_left,
                            std::pmr::vector<size_t> column_path_right,
                            bool one_sided) const -> simple_predicate::check_function_t {
                return [comp,
                        column_path_left = std::move(column_path_left),
                        column_path_right = std::move(column_path_right),
                        one_sided](const vector::data_chunk_t& chunk_left,
                                   const vector::data_chunk_t& chunk_right,
                                   size_t index_left,
                                   size_t index_right) {
                    if (one_sided) {
                        assert(column_path_left.front() < chunk_left.column_count());
                        assert(column_path_right.front() < chunk_left.column_count());
                        return comp(chunk_left.at(column_path_left)->data<LeftType>()[index_left],
                                    chunk_left.at(column_path_right)->data<RightType>()[index_left]);
                    } else {
                        assert(column_path_left.front() < chunk_left.column_count());
                        assert(column_path_right.front() < chunk_right.column_count());
                        return comp(chunk_left.at(column_path_left)->data<LeftType>()[index_left],
                                    chunk_right.at(column_path_right)->data<RightType>()[index_right]);
                    }
                };
            }
            // SFINAE unable to compare types
            template<typename LeftType,
                     typename RightType,
                     typename... Args,
                     std::enable_if_t<!core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(Args&&...) const -> simple_predicate::check_function_t {
                throw std::runtime_error("invalid expression in create_binary_comparator");
            }
        };

        // by this point compare_expression is unmodifiable, so we have to pass side explicitly
        template<typename COMP>
        simple_predicate::check_function_t
        create_unary_comparator(std::pmr::memory_resource* resource,
                                const expressions::compare_expression_ptr& expr,
                                const std::pmr::vector<types::complex_logical_type>& types,
                                const logical_plan::storage_parameters* parameters,
                                expressions::side_t side) {
            auto column_path = get_column_path(resource, expr->primary_key(), types);
            const auto& expr_val = parameters->parameters.at(expr->value());

            auto type_left = column_path.second->to_physical_type();
            auto type_right = expr_val.type().to_physical_type();

            return types::double_simple_physical_type_switch<create_unary_comparator_t>(type_left,
                                                                                        type_right,
                                                                                        COMP{},
                                                                                        std::move(column_path.first),
                                                                                        side,
                                                                                        expr_val);
        }

        simple_predicate::check_function_t
        create_unary_regex_comparator(std::pmr::memory_resource* resource,
                                      const expressions::compare_expression_ptr& expr,
                                      const std::pmr::vector<types::complex_logical_type>& types,
                                      const logical_plan::storage_parameters* parameters,
                                      expressions::side_t side) {
            assert(side != expressions::side_t::undefined);
            auto column_path = get_column_path(resource, expr->primary_key(), types);
            auto expr_val = parameters->parameters.at(expr->value());

            return
                [column_path, val = expr_val.value<std::string_view>(), side](const vector::data_chunk_t& chunk_left,
                                                                              const vector::data_chunk_t& chunk_right,
                                                                              size_t index_left,
                                                                              size_t index_right) {
                    if (side == expressions::side_t::left) {
                        assert(column_path.first.front() < chunk_left.column_count());
                        return std::regex_match(
                            chunk_left.at(column_path.first)->data<std::string_view>()[index_left].data(),
                            std::regex(fmt::format(".*{}.*", val)));
                    } else {
                        assert(column_path.first.front() < chunk_right.column_count());
                        return std::regex_match(
                            chunk_right.at(column_path.first)->data<std::string_view>()[index_right].data(),
                            std::regex(fmt::format(".*{}.*", val)));
                    }
                };
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_binary_comparator(std::pmr::memory_resource* resource,
                                 const expressions::compare_expression_ptr& expr,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right) {
            bool one_sided = false;
            auto column_path_left = get_column_path(resource, expr->primary_key(), types_left);
            auto column_path_right = get_column_path(resource, expr->secondary_key(), types_right);
            types::physical_type type_left = column_path_left.second->to_physical_type();
            types::physical_type type_right;
            if (column_path_right.first.front() == std::numeric_limits<size_t>::max()) {
                // one-sided expr
                column_path_right = get_column_path(resource, expr->secondary_key(), types_left);
                one_sided = true;
                type_right = column_path_right.second->to_physical_type();
            } else {
                type_right = column_path_right.second->to_physical_type();
            }

            return types::double_simple_physical_type_switch<create_binary_comparator_t>(
                type_left,
                type_right,
                COMP{},
                std::move(column_path_left.first),
                std::move(column_path_right.first),
                one_sided);
        }

        simple_predicate::check_function_t
        create_binary_regex_comparator(std::pmr::memory_resource* resource,
                                       const expressions::compare_expression_ptr& expr,
                                       const std::pmr::vector<types::complex_logical_type>& types_left,
                                       const std::pmr::vector<types::complex_logical_type>& types_right) {
            bool one_sided = false;
            auto column_path_left = get_column_path(resource, expr->primary_key(), types_left);
            auto column_path_right = get_column_path(resource, expr->secondary_key(), types_right);
            if (column_path_right.first.front() == std::numeric_limits<size_t>::max()) {
                // one-sided expr
                column_path_right = get_column_path(resource, expr->secondary_key(), types_left);
            }

            return [column_path_left, column_path_right, one_sided](const vector::data_chunk_t& chunk_left,
                                                                    const vector::data_chunk_t& chunk_right,
                                                                    size_t index_left,
                                                                    size_t index_right) {
                if (one_sided) {
                    return std::regex_match(
                        chunk_left.at(column_path_left.first)->data<std::string_view>()[index_left].data(),
                        std::regex(fmt::format(
                            ".*{}.*",
                            chunk_left.at(column_path_right.first)->data<std::string_view>()[index_left].data())));
                } else {
                    return std::regex_match(
                        chunk_left.at(column_path_left.first)->data<std::string_view>()[index_left].data(),
                        std::regex(fmt::format(
                            ".*{}.*",
                            chunk_right.at(column_path_right.first)->data<std::string_view>()[index_right].data())));
                }
            };
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_comparator(std::pmr::memory_resource* resource,
                          const expressions::compare_expression_ptr& expr,
                          const std::pmr::vector<types::complex_logical_type>& types_left,
                          const std::pmr::vector<types::complex_logical_type>& types_right,
                          const logical_plan::storage_parameters* parameters) {
            // TODO: use schema to determine expr side before this
            if (!expr->primary_key().is_null() && !expr->secondary_key().is_null()) {
                return create_binary_comparator<COMP>(resource, expr, types_left, types_right);
            } else {
                if (expr->primary_key().side() == expressions::side_t::left) {
                    return create_unary_comparator<COMP>(resource,
                                                         expr,
                                                         types_left,
                                                         parameters,
                                                         expressions::side_t::left);
                } else if (expr->primary_key().side() == expressions::side_t::right) {
                    return create_unary_comparator<COMP>(resource,
                                                         expr,
                                                         types_right,
                                                         parameters,
                                                         expressions::side_t::right);
                } else {
                    auto path = get_column_path(resource, expr->primary_key(), types_left);
                    if (path.first.front() != std::numeric_limits<size_t>::max()) {
                        return create_unary_comparator<COMP>(resource,
                                                             expr,
                                                             types_left,
                                                             parameters,
                                                             expressions::side_t::left);
                    }
                    path = get_column_path(resource, expr->primary_key(), types_right);
                    if (path.first.front() != std::numeric_limits<size_t>::max()) {
                        // undefined sided expressions store value on the left side by default
                        return create_unary_comparator<COMP>(resource,
                                                             expr,
                                                             types_right,
                                                             parameters,
                                                             expressions::side_t::left);
                    }
                }
            }

            return [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; };
        }

        simple_predicate::check_function_t
        create_regex_comparator(std::pmr::memory_resource* resource,
                                const expressions::compare_expression_ptr& expr,
                                const std::pmr::vector<types::complex_logical_type>& types_left,
                                const std::pmr::vector<types::complex_logical_type>& types_right,
                                const logical_plan::storage_parameters* parameters) {
            // TODO: use schema to determine expr side before this
            if (!expr->primary_key().is_null() && !expr->secondary_key().is_null()) {
                return create_binary_regex_comparator(resource, expr, types_left, types_right);
            } else {
                if (expr->primary_key().side() == expressions::side_t::left) {
                    return create_unary_regex_comparator(resource,
                                                         expr,
                                                         types_left,
                                                         parameters,
                                                         expressions::side_t::left);
                } else if (expr->primary_key().side() == expressions::side_t::right) {
                    return create_unary_regex_comparator(resource,
                                                         expr,
                                                         types_right,
                                                         parameters,
                                                         expressions::side_t::right);
                } else {
                    auto path = get_column_path(resource, expr->primary_key(), types_left);
                    if (path.first.front() != std::numeric_limits<size_t>::max()) {
                        return create_unary_regex_comparator(resource,
                                                             expr,
                                                             types_left,
                                                             parameters,
                                                             expressions::side_t::left);
                    }
                    path = get_column_path(resource, expr->primary_key(), types_right);
                    if (path.first.front() != std::numeric_limits<size_t>::max()) {
                        // undefined sided expressions store value on the left side by default
                        return create_unary_regex_comparator(resource,
                                                             expr,
                                                             types_right,
                                                             parameters,
                                                             expressions::side_t::left);
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

    predicate_ptr create_simple_predicate(std::pmr::memory_resource* resource,
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
                    nested.emplace_back(create_simple_predicate(
                        resource,
                        reinterpret_cast<const expressions::compare_expression_ptr&>(nested_expr),
                        types_left,
                        types_right,
                        parameters));
                }
                return {new simple_predicate(std::move(nested), expr->type())};
            }
            case compare_type::eq:
                return {new simple_predicate(
                    impl::create_comparator<std::equal_to<>>(resource, expr, types_left, types_right, parameters))};
            case compare_type::ne:
                return {new simple_predicate(
                    impl::create_comparator<std::not_equal_to<>>(resource, expr, types_left, types_right, parameters))};
            case compare_type::gt:
                return {new simple_predicate(
                    impl::create_comparator<std::greater<>>(resource, expr, types_left, types_right, parameters))};
            case compare_type::gte:
                return {new simple_predicate(impl::create_comparator<std::greater_equal<>>(resource,
                                                                                           expr,
                                                                                           types_left,
                                                                                           types_right,
                                                                                           parameters))};
            case compare_type::lt:
                return {new simple_predicate(
                    impl::create_comparator<std::less<>>(resource, expr, types_left, types_right, parameters))};
            case compare_type::lte:
                return {new simple_predicate(
                    impl::create_comparator<std::less_equal<>>(resource, expr, types_left, types_right, parameters))};
            case compare_type::regex: {
                return {new simple_predicate(
                    impl::create_regex_comparator(resource, expr, types_left, types_right, parameters))};
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

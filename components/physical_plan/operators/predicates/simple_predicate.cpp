#include "simple_predicate.hpp"
#include "utils.hpp"

#include <components/expressions/function_expression.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/operations_helper.hpp>
#include <regex>

namespace components::operators::predicates {

    namespace impl {

        const types::complex_logical_type& get_sub_type(const expressions::key_t& key,
                                                        const std::pmr::vector<types::complex_logical_type>& types) {
            const types::complex_logical_type* sub_type = &types[key.path().front()];
            for (auto it = std::next(key.path().begin()); it != key.path().end(); ++it) {
                if (sub_type->type() == types::logical_type::ARRAY || sub_type->type() == types::logical_type::LIST) {
                    sub_type = &sub_type->child_type();
                } else {
                    sub_type = &sub_type->child_types().at(*it);
                }
            }
            return *sub_type;
        }

        const types::complex_logical_type&
        get_sub_type(const expressions::key_t& key,
                     const std::pmr::vector<types::complex_logical_type>& types_left,
                     const std::pmr::vector<types::complex_logical_type>& types_right) {
            return key.side() == expressions::side_t::left ? get_sub_type(key, types_left)
                                                           : get_sub_type(key, types_right);
        }

        // Because regex is not a constexpr(?), we use it to dispatch function
        template<typename T = void>
        struct regex;

        template<>
        struct regex<void> {};

        template<typename COMP, typename T, typename U, std::enable_if_t<!std::is_same_v<COMP, regex<>>, bool> = true>
        bool evaluate_comp(T left, U right) {
            return COMP{}(left, right);
        }

        template<typename COMP, typename T, typename U, std::enable_if_t<std::is_same_v<COMP, regex<>>, bool> = true>
        bool evaluate_comp(T, U) {
            throw std::runtime_error("incorrect argument type for regex");
        }

        template<typename COMP, std::enable_if_t<std::is_same_v<COMP, regex<>>, bool> = true>
        bool evaluate_comp(std::string_view left, std::string_view right) {
            return std::regex_search(std::string(left), std::regex(std::string(right)));
        }

        template<typename COMP, std::enable_if_t<std::is_same_v<COMP, regex<>>, bool> = true>
        bool evaluate_comp(const types::logical_value_t& left, const types::logical_value_t& right) {
            return evaluate_comp<COMP>(left.value<std::string_view>(), right.value<std::string_view>());
        }

        template<typename T = void>
        struct create_simple_comparator_t;

        template<>
        struct create_simple_comparator_t<void> {
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&&, const expressions::key_t& key_left, const expressions::key_t& key_right) const
                -> simple_predicate::check_function_t {
                if (key_left.side() == expressions::side_t::left) {
                    if (key_right.side() == expressions::side_t::left) {
                        return [column_path_left = key_left.path(),
                                column_path_right = key_right.path()](const vector::data_chunk_t& chunk_left,
                                                                      const vector::data_chunk_t&,
                                                                      size_t index_left,
                                                                      size_t) {
                            return evaluate_comp<COMP>(chunk_left.at(column_path_left)->data<LeftType>()[index_left],
                                                       chunk_left.at(column_path_right)->data<RightType>()[index_left]);
                        };
                    } else {
                        assert(key_right.side() == expressions::side_t::right);
                        return [column_path_left = key_left.path(),
                                column_path_right = key_right.path()](const vector::data_chunk_t& chunk_left,
                                                                      const vector::data_chunk_t& chunk_right,
                                                                      size_t index_left,
                                                                      size_t index_right) {
                            return evaluate_comp<COMP>(
                                chunk_left.at(column_path_left)->data<LeftType>()[index_left],
                                chunk_right.at(column_path_right)->data<RightType>()[index_right]);
                        };
                    }
                } else {
                    assert(key_left.side() == expressions::side_t::right);
                    if (key_right.side() == expressions::side_t::left) {
                        return [column_path_left = key_left.path(),
                                column_path_right = key_right.path()](const vector::data_chunk_t& chunk_left,
                                                                      const vector::data_chunk_t& chunk_right,
                                                                      size_t index_left,
                                                                      size_t index_right) {
                            return evaluate_comp<COMP>(chunk_right.at(column_path_left)->data<LeftType>()[index_right],
                                                       chunk_left.at(column_path_right)->data<RightType>()[index_left]);
                        };
                    } else {
                        assert(key_right.side() == expressions::side_t::right);
                        return [column_path_left = key_left.path(),
                                column_path_right = key_right.path()](const vector::data_chunk_t&,
                                                                      const vector::data_chunk_t& chunk_right,
                                                                      size_t,
                                                                      size_t index_right) {
                            return evaluate_comp<COMP>(
                                chunk_right.at(column_path_left)->data<LeftType>()[index_right],
                                chunk_right.at(column_path_right)->data<RightType>()[index_right]);
                        };
                    }
                }
            }
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&&, const expressions::key_t& key_left, types::logical_value_t&& value) const
                -> simple_predicate::check_function_t {
                if (key_left.side() == expressions::side_t::left) {
                    return [column_path = key_left.path(),
                            value = std::move(value)](const vector::data_chunk_t& chunk_left,
                                                      const vector::data_chunk_t&,
                                                      size_t index_left,
                                                      size_t) {
                        return evaluate_comp<COMP>(chunk_left.at(column_path)->data<LeftType>()[index_left],
                                                   value.value<RightType>());
                    };
                } else {
                    return [column_path = key_left.path(),
                            value = std::move(value)](const vector::data_chunk_t&,
                                                      const vector::data_chunk_t& chunk_right,
                                                      size_t,
                                                      size_t index_right) {
                        return evaluate_comp<COMP>(chunk_right.at(column_path)->data<LeftType>()[index_right],
                                                   value.value<RightType>());
                    };
                }
            }
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&&, types::logical_value_t&& value, const expressions::key_t& key_right) const
                -> simple_predicate::check_function_t {
                if (key_right.side() == expressions::side_t::left) {
                    return [column_path = key_right.path(),
                            value = std::move(value)](const vector::data_chunk_t& chunk_left,
                                                      const vector::data_chunk_t&,
                                                      size_t index_left,
                                                      size_t) {
                        return evaluate_comp<COMP>(value.value<LeftType>(),
                                                   chunk_left.at(column_path)->data<RightType>()[index_left]);
                    };
                } else {
                    return [column_path = key_right.path(),
                            value = std::move(value)](const vector::data_chunk_t&,
                                                      const vector::data_chunk_t& chunk_right,
                                                      size_t,
                                                      size_t index_right) {
                        return evaluate_comp<COMP>(value.value<LeftType>(),
                                                   chunk_right.at(column_path)->data<RightType>()[index_right]);
                    };
                }
            }
            template<typename LeftType,
                     typename RightType,
                     typename COMP,
                     std::enable_if_t<core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(COMP&&, types::logical_value_t&& value_left, types::logical_value_t&& value_right) const
                -> simple_predicate::check_function_t {
                return [value_left = std::move(value_left.value<LeftType>()),
                        value_right = std::move(value_right.value<RightType>())](const vector::data_chunk_t&,
                                                                                 const vector::data_chunk_t&,
                                                                                 size_t,
                                                                                 size_t) {
                    return evaluate_comp<COMP>(value_left, value_right);
                };
            }
            // SFINAE unable to compare types
            template<typename LeftType,
                     typename RightType,
                     typename... Args,
                     std::enable_if_t<!core::has_equality_operator<LeftType, RightType>::value, bool> = true>
            auto operator()(Args&&...) const -> simple_predicate::check_function_t {
                throw std::runtime_error("invalid expression in create_simple_comparator");
            }
        };

        template<typename COMP>
        simple_predicate::check_function_t
        create_simple_comparator(const expressions::key_t& key_left,
                                 const expressions::key_t& key_right,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right,
                                 const logical_plan::storage_parameters*) {
            auto sub_type_left = get_sub_type(key_left, types_left, types_right);
            auto sub_type_right = get_sub_type(key_right, types_left, types_right);
            types::physical_type type_left = sub_type_left.to_physical_type();
            types::physical_type type_right = sub_type_right.to_physical_type();

            return types::double_simple_physical_type_switch<create_simple_comparator_t>(type_left,
                                                                                         type_right,
                                                                                         COMP{},
                                                                                         key_left,
                                                                                         key_right);
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_simple_comparator(const expressions::key_t& key_left,
                                 core::parameter_id_t id_right,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right,
                                 const logical_plan::storage_parameters* parameters) {
            auto sub_type_left = get_sub_type(key_left, types_left, types_right);
            auto value_right = parameters->parameters.at(id_right);
            types::physical_type type_left = sub_type_left.to_physical_type();
            types::physical_type type_right = value_right.type().to_physical_type();

            return types::double_simple_physical_type_switch<create_simple_comparator_t>(type_left,
                                                                                         type_right,
                                                                                         COMP{},
                                                                                         key_left,
                                                                                         std::move(value_right));
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_simple_comparator(core::parameter_id_t id_left,
                                 const expressions::key_t& key_right,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right,
                                 const logical_plan::storage_parameters* parameters) {
            auto value_left = parameters->parameters.at(id_left);
            auto sub_type_right = get_sub_type(key_right, types_left, types_right);
            types::physical_type type_left = value_left.type().to_physical_type();
            types::physical_type type_right = sub_type_right.to_physical_type();

            return types::double_simple_physical_type_switch<create_simple_comparator_t>(type_left,
                                                                                         type_right,
                                                                                         COMP{},
                                                                                         std::move(value_left),
                                                                                         key_right);
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_simple_comparator(core::parameter_id_t id_left,
                                 core::parameter_id_t id_right,
                                 const std::pmr::vector<types::complex_logical_type>&,
                                 const std::pmr::vector<types::complex_logical_type>&,
                                 const logical_plan::storage_parameters* parameters) {
            auto value_left = parameters->parameters.at(id_left);
            auto value_right = parameters->parameters.at(id_right);
            types::physical_type type_left = value_left.type().to_physical_type();
            types::physical_type type_right = value_right.type().to_physical_type();

            return types::double_simple_physical_type_switch<create_simple_comparator_t>(type_left,
                                                                                         type_right,
                                                                                         COMP{},
                                                                                         std::move(value_left),
                                                                                         std::move(value_right));
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_simple_comparator(const expressions::compare_expression_ptr& expr,
                                 const std::pmr::vector<types::complex_logical_type>& types_left,
                                 const std::pmr::vector<types::complex_logical_type>& types_right,
                                 const logical_plan::storage_parameters* parameters) {
            assert(!std::holds_alternative<expressions::expression_ptr>(expr->left()) &&
                   !std::holds_alternative<expressions::expression_ptr>(expr->right()));
            if (std::holds_alternative<expressions::key_t>(expr->left())) {
                if (std::holds_alternative<expressions::key_t>(expr->right())) {
                    return create_simple_comparator<COMP>(std::get<expressions::key_t>(expr->left()),
                                                          std::get<expressions::key_t>(expr->right()),
                                                          types_left,
                                                          types_right,
                                                          parameters);
                } else {
                    assert(std::holds_alternative<core::parameter_id_t>(expr->right()));
                    return create_simple_comparator<COMP>(std::get<expressions::key_t>(expr->left()),
                                                          std::get<core::parameter_id_t>(expr->right()),
                                                          types_left,
                                                          types_right,
                                                          parameters);
                }
            } else {
                assert(std::holds_alternative<core::parameter_id_t>(expr->left()));
                if (std::holds_alternative<expressions::key_t>(expr->right())) {
                    return create_simple_comparator<COMP>(std::get<core::parameter_id_t>(expr->left()),
                                                          std::get<expressions::key_t>(expr->right()),
                                                          types_left,
                                                          types_right,
                                                          parameters);
                } else {
                    assert(std::holds_alternative<core::parameter_id_t>(expr->right()));
                    return create_simple_comparator<COMP>(std::get<core::parameter_id_t>(expr->left()),
                                                          std::get<core::parameter_id_t>(expr->right()),
                                                          types_left,
                                                          types_right,
                                                          parameters);
                }
            }
        }

        template<typename COMP>
        simple_predicate::check_function_t
        create_comparator(std::pmr::memory_resource* resource,
                          const compute::function_registry_t* function_registry,
                          const expressions::compare_expression_ptr& expr,
                          const std::pmr::vector<types::complex_logical_type>& types_left,
                          const std::pmr::vector<types::complex_logical_type>& types_right,
                          const logical_plan::storage_parameters* parameters) {
            if (std::holds_alternative<expressions::expression_ptr>(expr->left()) ||
                std::holds_alternative<expressions::expression_ptr>(expr->right())) {
                return [left_getter = create_value_getter(resource, function_registry, expr->left(), parameters),
                        right_getter = create_value_getter(resource, function_registry, expr->right(), parameters)](
                           const vector::data_chunk_t& chunk_left,
                           const vector::data_chunk_t& chunk_right,
                           size_t index_left,
                           size_t index_right) {
                    return evaluate_comp<COMP>(
                        left_getter.operator()(chunk_left, chunk_right, index_left, index_right),
                        right_getter.operator()(chunk_left, chunk_right, index_left, index_right));
                };
            } else {
                return create_simple_comparator<COMP>(expr, types_left, types_right, parameters);
            }
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
                return {new simple_predicate(impl::create_comparator<std::equal_to<>>(resource,
                                                                                      function_registry,
                                                                                      expr,
                                                                                      types_left,
                                                                                      types_right,
                                                                                      parameters))};
            case compare_type::ne:
                return {new simple_predicate(impl::create_comparator<std::not_equal_to<>>(resource,
                                                                                          function_registry,
                                                                                          expr,
                                                                                          types_left,
                                                                                          types_right,
                                                                                          parameters))};
            case compare_type::gt:
                return {new simple_predicate(impl::create_comparator<std::greater<>>(resource,
                                                                                     function_registry,
                                                                                     expr,
                                                                                     types_left,
                                                                                     types_right,
                                                                                     parameters))};
            case compare_type::gte:
                return {new simple_predicate(impl::create_comparator<std::greater_equal<>>(resource,
                                                                                           function_registry,
                                                                                           expr,
                                                                                           types_left,
                                                                                           types_right,
                                                                                           parameters))};
            case compare_type::lt:
                return {new simple_predicate(impl::create_comparator<std::less<>>(resource,
                                                                                  function_registry,
                                                                                  expr,
                                                                                  types_left,
                                                                                  types_right,
                                                                                  parameters))};
            case compare_type::lte:
                return {new simple_predicate(impl::create_comparator<std::less_equal<>>(resource,
                                                                                        function_registry,
                                                                                        expr,
                                                                                        types_left,
                                                                                        types_right,
                                                                                        parameters))};
            case compare_type::regex: {
                return {new simple_predicate(impl::create_comparator<impl::regex<>>(resource,
                                                                                    function_registry,
                                                                                    expr,
                                                                                    types_left,
                                                                                    types_right,
                                                                                    parameters))};
            }
            case compare_type::all_false:
                return {new simple_predicate(
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return false; })};
            case compare_type::all_true:
            default:
                return {new simple_predicate(
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return true; })};
        }
    }

} // namespace components::operators::predicates

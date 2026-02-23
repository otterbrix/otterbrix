#include "simple_predicate_impl.hpp"

namespace components::operators::predicates {

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
                return {new simple_predicate(
                    create_eq_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
            case compare_type::ne:
                return {new simple_predicate(
                    create_ne_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
            case compare_type::gt:
                return {new simple_predicate(
                    create_gt_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
            case compare_type::gte:
                return {new simple_predicate(
                    create_gte_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
            case compare_type::lt:
                return {new simple_predicate(
                    create_lt_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
            case compare_type::lte:
                return {new simple_predicate(
                    create_lte_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
            case compare_type::regex: {
                return {new simple_predicate(
                    create_regex_simple_predicate(resource, function_registry, expr, types_left, types_right, parameters))};
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

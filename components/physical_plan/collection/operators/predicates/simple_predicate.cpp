#include "simple_predicate.hpp"
#include <components/physical_plan/base/operators/operator.hpp>
#include <fmt/format.h>
#include <regex>

namespace components::collection::operators::predicates {

    simple_predicate::simple_predicate(check_function_t func)
        : func_(std::move(func)) {}

    simple_predicate::simple_predicate(std::vector<predicate_ptr>&& nested, expressions::compare_type nested_type)
        : nested_(std::move(nested))
        , nested_type_(nested_type) {}

    bool simple_predicate::check_impl(const document::document_ptr& document_left,
                                      const document::document_ptr& document_right,
                                      const logical_plan::storage_parameters* parameters) {
        switch (nested_type_) {
            case expressions::compare_type::union_and:
                for (const auto& predicate : nested_) {
                    if (!predicate->check(document_left, document_right, parameters)) {
                        return false;
                    }
                }
                return true;
            case expressions::compare_type::union_or:
                for (const auto& predicate : nested_) {
                    if (predicate->check(document_left, document_right, parameters)) {
                        return true;
                    }
                }
                return false;
            case expressions::compare_type::union_not:
                return !nested_.front()->check(document_left, document_right, parameters);
            default:
                break;
        }
        return func_(document_left, document_right, parameters);
    }

    predicate_ptr create_simple_predicate(const expressions::compare_expression_ptr& expr) {
        using expressions::compare_type;

        switch (expr->type()) {
            case compare_type::union_and:
            case compare_type::union_or:
            case compare_type::union_not: {
                std::vector<predicate_ptr> nested;
                nested.reserve(expr->children().size());
                for (const auto& nested_expr : expr->children()) {
                    nested.emplace_back(create_simple_predicate(
                        reinterpret_cast<const expressions::compare_expression_ptr&>(nested_expr)));
                }
                return {new simple_predicate(std::move(nested), expr->type())};
            }
            case compare_type::eq:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        auto comp =
                            document_right
                                ? document_left->compare(expr->key_left().as_string(),
                                                         document_right->get_value(expr->key_right().as_string()))
                                : document_left->compare(expr->key_left().as_string(),
                                                         document_left->get_value(expr->key_right().as_string()));
                        return comp == types::compare_t::equals;
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() == it->second;
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->get_value(expr->key_right().as_string()).as_logical_value() ==
                               it->second;
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() == it->second;
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->get_value(expr->key_left().as_string()).as_logical_value() == it->second;
                    }
                    return false;
                })};
            case compare_type::ne:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        auto comp =
                            document_right
                                ? document_left->compare(expr->key_left().as_string(),
                                                         document_right->get_value(expr->key_right().as_string()))
                                : document_left->compare(expr->key_left().as_string(),
                                                         document_left->get_value(expr->key_right().as_string()));
                        return comp != types::compare_t::equals;
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() != it->second;
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->get_value(expr->key_right().as_string()).as_logical_value() !=
                               it->second;
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() != it->second;
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->get_value(expr->key_left().as_string()).as_logical_value() != it->second;
                    }
                    return false;
                })};
            case compare_type::gt:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        auto comp =
                            document_right
                                ? document_left->compare(expr->key_left().as_string(),
                                                         document_right->get_value(expr->key_right().as_string()))
                                : document_left->compare(expr->key_left().as_string(),
                                                         document_left->get_value(expr->key_right().as_string()));
                        return comp == types::compare_t::more;
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() > it->second;
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->get_value(expr->key_right().as_string()).as_logical_value() > it->second;
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() > it->second;
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->get_value(expr->key_left().as_string()).as_logical_value() > it->second;
                    }
                    return false;
                })};
            case compare_type::gte:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        auto comp =
                            document_right
                                ? document_left->compare(expr->key_left().as_string(),
                                                         document_right->get_value(expr->key_right().as_string()))
                                : document_left->compare(expr->key_left().as_string(),
                                                         document_left->get_value(expr->key_right().as_string()));
                        return comp == types::compare_t::equals || comp == types::compare_t::more;
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() >= it->second;
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->get_value(expr->key_right().as_string()).as_logical_value() >=
                               it->second;
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() >= it->second;
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->get_value(expr->key_left().as_string()).as_logical_value() >= it->second;
                    }
                    return false;
                })};
            case compare_type::lt:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        auto comp =
                            document_right
                                ? document_left->compare(expr->key_left().as_string(),
                                                         document_right->get_value(expr->key_right().as_string()))
                                : document_left->compare(expr->key_left().as_string(),
                                                         document_left->get_value(expr->key_right().as_string()));
                        return comp == types::compare_t::less;
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() < it->second;
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->get_value(expr->key_right().as_string()).as_logical_value() < it->second;
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() < it->second;
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->get_value(expr->key_left().as_string()).as_logical_value() < it->second;
                    }
                    return false;
                })};
            case compare_type::lte:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        auto comp =
                            document_right
                                ? document_left->compare(expr->key_left().as_string(),
                                                         document_right->get_value(expr->key_right().as_string()))
                                : document_left->compare(expr->key_left().as_string(),
                                                         document_left->get_value(expr->key_right().as_string()));
                        return comp == types::compare_t::equals || comp == types::compare_t::less;
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() <= it->second;
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->get_value(expr->key_right().as_string()).as_logical_value() <=
                               it->second;
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->get_value(expr->key_left().as_string()).as_logical_value() <= it->second;
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->get_value(expr->key_left().as_string()).as_logical_value() <= it->second;
                    }
                    return false;
                })};
            case compare_type::regex:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->key_left().is_null() && !expr->key_right().is_null()) {
                        return document_right
                                   ? document_left->type_by_key(expr->key_left().as_string()) ==
                                             types::logical_type::STRING_LITERAL &&
                                         std::regex_match(
                                             document_left->get_string(expr->key_left().as_string()).data(),
                                             std::regex(fmt::format(
                                                 ".*{}.*",
                                                 document_left->get_value(expr->key_right().as_string()).as_string())))
                                   : document_left->type_by_key(expr->key_left().as_string()) ==
                                             types::logical_type::STRING_LITERAL &&
                                         std::regex_match(
                                             document_left->get_string(expr->key_left().as_string()).data(),
                                             std::regex(
                                                 fmt::format(".*{}.*",
                                                             document_right->get_value(expr->key_right().as_string())
                                                                 .as_string())));
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->side() == expressions::side_t::left) {
                        return document_left->type_by_key(expr->key_left().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_left->get_string(expr->key_left().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    if (expr->side() == expressions::side_t::right) {
                        return document_right->type_by_key(expr->key_right().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_right->get_string(expr->key_right().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    if (document_left->is_exists(expr->key_left().as_string())) {
                        return document_left->type_by_key(expr->key_left().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_left->get_string(expr->key_left().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    if (document_right->is_exists(expr->key_left().as_string())) {
                        return document_right->type_by_key(expr->key_left().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_right->get_string(expr->key_left().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    return false;
                })};
            case compare_type::all_true:
                return {new simple_predicate([](const document::document_ptr&,
                                                const document::document_ptr&,
                                                const logical_plan::storage_parameters*) { return true; })};
            case compare_type::all_false:
                return {new simple_predicate([](const document::document_ptr&,
                                                const document::document_ptr&,
                                                const logical_plan::storage_parameters*) { return false; })};
            default:
                break;
        }
        return {new simple_predicate([](const document::document_ptr&,
                                        const document::document_ptr&,
                                        const logical_plan::storage_parameters*) { return true; })};
    }

} // namespace components::collection::operators::predicates

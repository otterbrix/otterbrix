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

    bool deduce_side(const document::document_ptr& document_left,
                     const document::document_ptr& document_right,
                     expressions::key_t& key) {
        if (key.side() == expressions::side_t::undefined) {
            if (document_left->is_exists(key.as_string())) {
                key.set_side(expressions::side_t::left);
                return true;
            } else if (document_right->is_exists(key.as_string())) {
                key.set_side(expressions::side_t::right);
                return true;
            }
            return false;
        }
        return true;
    }

    template<expressions::compare_type T>
    constexpr bool comparator_select(types::compare_t comp) {
        switch (T) {
            case expressions::compare_type::eq:
                return comp == types::compare_t::equals;
            case expressions::compare_type::ne:
                return comp != types::compare_t::equals;
            case expressions::compare_type::gt:
                return comp == types::compare_t::more;
            case expressions::compare_type::lt:
                return comp == types::compare_t::less;
            case expressions::compare_type::gte:
                return comp == types::compare_t::equals || comp == types::compare_t::more;
            case expressions::compare_type::lte:
                return comp == types::compare_t::equals || comp == types::compare_t::less;
            default:
                return false;
        }
    }

    std::optional<types::compare_t> compare_documents(const expressions::compare_expression_ptr& expr,
                                                      const document::document_ptr& document_left,
                                                      const document::document_ptr& document_right) {
        auto compare_documents_impl = [](const document::document_ptr& document_left,
                                         const document::document_ptr& document_right,
                                         const expressions::key_t& key_left,
                                         const expressions::key_t& key_right) {
            return document_left->compare(key_left.as_string(), document_right->get_value(key_right.as_string()));
        };

        // TODO: side definition and error handling is supposed to be done before this, using schema
        auto primary_key = expr->primary_key();
        auto secondary_key = expr->secondary_key();
        if (!deduce_side(document_left, document_right, primary_key)) {
            return std::nullopt;
        }
        if (!deduce_side(document_left, document_right, secondary_key)) {
            return std::nullopt;
        }

        if (primary_key.side() == expressions::side_t::left) {
            if (secondary_key.side() == expressions::side_t::left) {
                return compare_documents_impl(document_left, document_left, primary_key, secondary_key);
            } else {
                return compare_documents_impl(document_left, document_right, primary_key, secondary_key);
            }
        } else {
            if (secondary_key.side() == expressions::side_t::left) {
                return compare_documents_impl(document_right, document_left, primary_key, secondary_key);
            } else {
                return compare_documents_impl(document_right, document_right, primary_key, secondary_key);
            }
        }
    }

    bool compare_regex_documents(const expressions::compare_expression_ptr& expr,
                                 const document::document_ptr& document_left,
                                 const document::document_ptr& document_right) {
        auto compare_documents_impl = [](const document::document_ptr& document_left,
                                         const document::document_ptr& document_right,
                                         const expressions::key_t& key_left,
                                         const expressions::key_t& key_right) {
            return document_left->type_by_key(key_left.as_string()) == types::logical_type::STRING_LITERAL &&
                   std::regex_match(
                       document_left->get_string(key_left.as_string()).data(),
                       std::regex(fmt::format(".*{}.*", document_right->get_value(key_right.as_string()).as_string())));
        };

        // TODO: side definition and error handling is supposed to be done before this, using schema
        auto primary_key = expr->primary_key();
        auto secondary_key = expr->secondary_key();
        if (!deduce_side(document_left, document_right, primary_key)) {
            return false;
        }
        if (!deduce_side(document_left, document_right, secondary_key)) {
            return false;
        }

        if (primary_key.side() == expressions::side_t::left) {
            if (secondary_key.side() == expressions::side_t::left) {
                return compare_documents_impl(document_left, document_left, primary_key, secondary_key);
            } else {
                return compare_documents_impl(document_left, document_right, primary_key, secondary_key);
            }
        } else {
            if (secondary_key.side() == expressions::side_t::left) {
                return compare_documents_impl(document_right, document_left, primary_key, secondary_key);
            } else {
                return compare_documents_impl(document_right, document_right, primary_key, secondary_key);
            }
        }
    }

    std::optional<types::compare_t> get_comparison(const expressions::compare_expression_ptr& expr,
                                                   const document::document_ptr& document_left,
                                                   const document::document_ptr& document_right,
                                                   const logical_plan::storage_parameters* parameters) {
        if (!expr->primary_key().is_null() && !expr->secondary_key().is_null()) {
            return compare_documents(expr, document_left, document_right);
        }
        auto it = parameters->parameters.find(expr->value());
        if (it == parameters->parameters.end()) {
            return std::nullopt;
        }
        if (expr->primary_key().side() == expressions::side_t::left) {
            return document_left->get_value(expr->primary_key().as_string()).as_logical_value().compare(it->second);
        }
        if (expr->primary_key().side() == expressions::side_t::right) {
            return document_right->get_value(expr->primary_key().as_string()).as_logical_value().compare(it->second);
        }
        if (document_left->is_exists(expr->primary_key().as_string())) {
            return document_left->get_value(expr->primary_key().as_string()).as_logical_value().compare(it->second);
        }
        if (document_right->is_exists(expr->primary_key().as_string())) {
            return document_right->get_value(expr->primary_key().as_string()).as_logical_value().compare(it->second);
        }
        return std::nullopt;
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
                    auto comp = get_comparison(expr, document_left, document_right, parameters);
                    if (!comp.has_value()) {
                        return false;
                    } else {
                        return comp == types::compare_t::equals;
                    }
                })};
            case compare_type::ne:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    auto comp = get_comparison(expr, document_left, document_right, parameters);
                    if (!comp.has_value()) {
                        return false;
                    } else {
                        return comp != types::compare_t::equals;
                    }
                })};
            case compare_type::gt:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    auto comp = get_comparison(expr, document_left, document_right, parameters);
                    if (!comp.has_value()) {
                        return false;
                    } else {
                        return comp == types::compare_t::more;
                    }
                })};
            case compare_type::gte:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    auto comp = get_comparison(expr, document_left, document_right, parameters);
                    if (!comp.has_value()) {
                        return false;
                    } else {
                        return comp == types::compare_t::equals || comp == types::compare_t::more;
                    }
                })};
            case compare_type::lt:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    auto comp = get_comparison(expr, document_left, document_right, parameters);
                    if (!comp.has_value()) {
                        return false;
                    } else {
                        return comp == types::compare_t::less;
                    }
                })};
            case compare_type::lte:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    auto comp = get_comparison(expr, document_left, document_right, parameters);
                    if (!comp.has_value()) {
                        return false;
                    } else {
                        return comp == types::compare_t::equals || comp == types::compare_t::less;
                    }
                })};
            case compare_type::regex:
                return {new simple_predicate([&expr](const document::document_ptr& document_left,
                                                     const document::document_ptr& document_right,
                                                     const logical_plan::storage_parameters* parameters) {
                    if (!expr->primary_key().is_null() && !expr->secondary_key().is_null()) {
                        return compare_regex_documents(expr, document_left, document_right);
                    }
                    auto it = parameters->parameters.find(expr->value());
                    if (it == parameters->parameters.end()) {
                        return false;
                    }
                    if (expr->primary_key().side() == expressions::side_t::left) {
                        return document_left->type_by_key(expr->primary_key().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_left->get_string(expr->primary_key().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    if (expr->primary_key().side() == expressions::side_t::right) {
                        return document_right->type_by_key(expr->primary_key().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_right->get_string(expr->primary_key().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    if (document_left->is_exists(expr->primary_key().as_string())) {
                        return document_left->type_by_key(expr->primary_key().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_left->get_string(expr->primary_key().as_string()).data(),
                                   std::regex(fmt::format(".*{}.*", it->second.value<std::string_view>())));
                    }
                    if (document_right->is_exists(expr->primary_key().as_string())) {
                        return document_right->type_by_key(expr->primary_key().as_string()) ==
                                   types::logical_type::STRING_LITERAL &&
                               std::regex_match(
                                   document_right->get_string(expr->primary_key().as_string()).data(),
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

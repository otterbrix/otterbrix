#include "simple_predicate.hpp"
#include "utils.hpp"

#include <components/expressions/like_to_regex.hpp>
#include <core/regex/regex.hpp>

#include <optional>
#include <string>
#include <unordered_map>

namespace components::operators::predicates {

    namespace {

        template<typename COMP, typename T, typename U>
        core::result_wrapper_t<bool> evaluate_comp(std::pmr::memory_resource*, T left, U right) {
            return COMP{}(left, right);
        }

        // Exotic scalar regex: `col regexp <column-or-expression>` where the pattern is NOT a bound
        // parameter, so it varies per row and genuinely cannot be compiled once. Reads both operands via
        // the shared value_getter and compiles per row with RE2 — crash-safe: a bad pattern is a returned
        // core::error_t, never a thrown std::regex_error (std::regex had no non-throwing way to report it).
        // The common `col LIKE/ILIKE/regexp <literal>` shape does NOT come here — it routes to
        // regex_predicate (compile-once) below.
        inline simple_predicate::row_check_fn_t make_regex_comparator(std::pmr::memory_resource* resource,
                                                                      const compute::function_registry_t* function_registry,
                                                                      const expressions::compare_expression_ptr& expr,
                                                                      const logical_plan::storage_parameters* parameters) {
            auto left_getter = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
            auto right_getter = impl::create_value_getter(resource, function_registry, expr->right(), parameters);
            const bool icase = expr->regex_icase();
            return [resource, left_getter = std::move(left_getter), right_getter = std::move(right_getter), icase](
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
                if (left_val.value().is_null() || right_val.value().is_null()) {
                    return false;
                }
                const auto& subject_val = left_val.value();
                const auto& pattern_val = right_val.value();
                auto compiled = core::regex_t::compile(resource, pattern_val.value<std::string_view>(), icase);
                if (compiled.has_error()) {
                    return compiled.error();
                }
                return compiled.value().match(subject_val.value<std::string_view>());
            };
        }

        template<typename COMP>
        simple_predicate::row_check_fn_t make_comparator(std::pmr::memory_resource* resource,
                                                         const compute::function_registry_t* function_registry,
                                                         const expressions::compare_expression_ptr& expr,
                                                         const logical_plan::storage_parameters* parameters,
                                                         core::date::timezone_offset_t session_tz) {
            auto left_getter = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
            auto right_getter = impl::create_value_getter(resource, function_registry, expr->right(), parameters);
            return [resource, left_getter = std::move(left_getter), right_getter = std::move(right_getter), session_tz](
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
                // PostgreSQL rejects an IMPLICIT boolean <-> numeric comparison ("operator
                // does not exist: boolean = integer"). is_numeric(BOOLEAN) is true, so without
                // this guard cast_as would silently coerce the numeric via the int->bool cast
                // and compare — asymmetric and surprising. Reject when EXACTLY one side is
                // BOOLEAN and the other is a non-boolean numeric, so `bool_col = 1` errors.
                // Same-type bool=bool and numeric=numeric (neither trips l_bool != r_bool), and
                // any string / temporal / NULL pairing, are untouched.
                {
                    // Detect a boolean operand by PHYSICAL type (BOOL) so it fires whether the
                    // value's logical type is BOOL or BOOLEAN. The other side is numeric-non-bool
                    // when it is is_numeric but not itself physically BOOL.
                    const bool l_bool = left_val.value().type().to_physical_type() == types::physical_type::BOOL;
                    const bool r_bool = right_val.value().type().to_physical_type() == types::physical_type::BOOL;
                    const auto other = l_bool ? right_val.value().type().type() : left_val.value().type().type();
                    if (l_bool != r_bool && types::is_numeric(other)) {
                        return core::error_t{
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"operator does not exist: boolean = numeric type", resource}};
                    }
                }
                auto cast_right = right_val.value().cast_as(left_val.value().type(), session_tz);
                if (!cast_right.is_null()) {
                    return evaluate_comp<COMP>(resource, left_val.value(), cast_right);
                }
                auto cast_left = left_val.value().cast_as(right_val.value().type(), session_tz);
                if (!cast_left.is_null()) {
                    return evaluate_comp<COMP>(resource, cast_left, right_val.value());
                }
                return false;
            };
        }

        // Scalar `col LIKE/ILIKE/regexp <literal>`: the pattern is a bound parameter, compiled ONCE here
        // with RE2 — crash-safe (a bad pattern is stored as an error and returned at eval, never thrown).
        // The subject is read through the shared value_getter. NOT LIKE / NOT ILIKE arrive wrapped in a
        // union_not predicate, so this only performs a positive match. A NULL subject (or a NULL pattern)
        // yields false (SQL unknown). No per-comparator std::function: this is a real predicate subclass so
        // the move-only compiled regex can live as a member.
        class regex_predicate final : public predicate {
        public:
            regex_predicate(impl::value_getter subject, core::result_wrapper_t<core::regex_t> compiled)
                : subject_(std::move(subject))
                , error_(compiled.has_error() ? std::optional<core::error_t>(compiled.error()) : std::nullopt)
                , re_(compiled.has_error() ? std::nullopt : std::optional<core::regex_t>(std::move(compiled.value()))) {}

            // NULL pattern: `col LIKE NULL` is unknown -> matches nothing.
            explicit regex_predicate(impl::value_getter subject)
                : subject_(std::move(subject)) {}

        private:
            core::result_wrapper_t<bool> check_impl(const vector::data_chunk_t& chunk_left,
                                                    const vector::data_chunk_t& chunk_right,
                                                    size_t index_left,
                                                    size_t index_right) override {
                if (error_) {
                    return *error_;
                }
                if (!re_) {
                    return false; // NULL pattern
                }
                auto subject = subject_(chunk_left, chunk_right, index_left, index_right);
                if (subject.has_error()) {
                    return subject.convert_error<bool>();
                }
                if (subject.value().is_null()) {
                    return false;
                }
                const auto& subject_val = subject.value();
                return re_->match(subject_val.value<std::string_view>());
            }

            impl::value_getter subject_;
            std::optional<core::error_t> error_;
            std::optional<core::regex_t> re_;
        };

        // `col LIKE/ILIKE/regexp ANY|ALL (SELECT ...)`: the sub-query result array is the pattern set, read
        // per row (correlation-safe). Each distinct element pattern is compiled ONCE with RE2 into a cache
        // member — a move-only regex_t cache cannot live inside a std::function lambda, which is why this is
        // a class. regex_like converts a LIKE glob per element, regex_icase matches case-insensitively
        // (ILIKE), regex_negate inverts each element result for NOT LIKE before the any/all fold. Crash-safe.
        class regex_any_predicate final : public predicate {
        public:
            regex_any_predicate(std::pmr::memory_resource* resource,
                                impl::value_getter left_getter,
                                core::parameter_id_t param_id,
                                const logical_plan::storage_parameters* parameters,
                                bool re_like,
                                bool re_icase,
                                bool re_negate,
                                bool is_any,
                                core::date::timezone_offset_t session_tz)
                : resource_(resource)
                , left_getter_(std::move(left_getter))
                , param_id_(param_id)
                , parameters_(parameters)
                , re_like_(re_like)
                , re_icase_(re_icase)
                , re_negate_(re_negate)
                , is_any_(is_any)
                , session_tz_(session_tz)
                , cache_(resource) {}

        private:
            core::result_wrapper_t<bool> check_impl(const vector::data_chunk_t& chunk_left,
                                                    const vector::data_chunk_t& chunk_right,
                                                    size_t index_left,
                                                    size_t index_right) override {
                auto left_val = left_getter_(chunk_left, chunk_right, index_left, index_right);
                if (left_val.has_error()) {
                    return left_val.convert_error<bool>();
                }
                if (left_val.value().is_null()) {
                    return false;
                }
                const auto& arr_param = parameters_->parameters.at(param_id_);
                // Empty sub-query list: `x = ANY(empty)` is false, `x <> ALL(empty)` is true (loop-exhausted).
                if (arr_param.is_null()) {
                    return !is_any_;
                }
                const auto& subject_val = left_val.value();
                std::string_view subject = subject_val.value<std::string_view>();
                const auto& arr = arr_param.children();
                for (const auto& element : arr) {
                    if (element.is_null()) {
                        continue;
                    }
                    // Text pattern element: read the bytes inline, no per-row logical_value_t (Rule 1). A
                    // non-text element (exotic `col LIKE ANY(SELECT int_col ...)`) is coerced to the subject's
                    // string type so it stringifies; a failed coercion (null) is skipped.
                    std::optional<types::logical_value_t> coerced;
                    std::string_view pattern;
                    if (types::is_string(element.type().type())) {
                        pattern = element.value<std::string_view>();
                    } else {
                        coerced = element.cast_as(subject_val.type(), session_tz_);
                        if (coerced->is_null()) {
                            continue;
                        }
                        pattern = coerced->value<std::string_view>();
                    }
                    auto* re = compiled_for(pattern);
                    if (!re) {
                        return core::error_t{
                            core::error_code_t::comparison_failure,
                            std::pmr::string{"invalid regular expression in ANY/ALL pattern", resource_}};
                    }
                    bool matched = re->match(subject);
                    if (re_negate_) {
                        matched = !matched;
                    }
                    if (is_any_ && matched) {
                        return true;
                    }
                    if (!is_any_ && !matched) {
                        return false;
                    }
                }
                return !is_any_;
            }

            // Compile-once: look the element pattern up in the cache, compiling + inserting on a miss.
            // Returns nullptr for a pattern that does not compile (cached as an empty slot).
            core::regex_t* compiled_for(std::string_view pattern) {
                std::pmr::string key{pattern, resource_};
                if (auto it = cache_.find(key); it != cache_.end()) {
                    return it->second.has_value() ? &*it->second : nullptr;
                }
                const std::string converted =
                    re_like_ ? expressions::like_to_regex(std::string(pattern)) : std::string(pattern);
                auto compiled = core::regex_t::compile(resource_, converted, re_icase_);
                std::optional<core::regex_t> slot =
                    compiled.has_error() ? std::nullopt : std::optional<core::regex_t>(std::move(compiled.value()));
                auto [ins, _inserted] = cache_.emplace(std::move(key), std::move(slot));
                return ins->second.has_value() ? &*ins->second : nullptr;
            }

            std::pmr::memory_resource* resource_;
            impl::value_getter left_getter_;
            core::parameter_id_t param_id_;
            const logical_plan::storage_parameters* parameters_;
            bool re_like_;
            bool re_icase_;
            bool re_negate_;
            bool is_any_;
            core::date::timezone_offset_t session_tz_;
            std::pmr::unordered_map<std::pmr::string, std::optional<core::regex_t>> cache_;
        };

    } // anonymous namespace

    simple_predicate::simple_predicate(std::pmr::memory_resource* resource, row_check_fn_t func)
        : resource_(resource)
        , func_(std::move(func))
        , nested_(resource_) {}

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
                                          const logical_plan::storage_parameters* parameters,
                                          core::date::timezone_offset_t session_tz) {
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
                                                         parameters,
                                                         session_tz));
                }
                return {new simple_predicate(resource, std::move(nested), expr->type())};
            }
            case compare_type::eq:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::equal_to<>>(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::ne:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::not_equal_to<>>(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::gt:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::greater<>>(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::gte:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::greater_equal<>>(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::lt:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::less<>>(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::lte:
                return {new simple_predicate(
                    resource,
                    make_comparator<std::less_equal<>>(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::any:
            case compare_type::all: {
                // inner_op is guaranteed valid by the transformer (an unmapped ANY/ALL operator is rejected
                // there, not silently defaulted to `=` — finding 5). No invalid->eq remap here.
                auto inner_op = expr->inner_op();
                const bool is_any = expr->type() == compare_type::any;
                auto left_getter = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
                auto param_id = std::get<core::parameter_id_t>(expr->right());
                // LIKE/ILIKE/regexp ANY|ALL: a dedicated subclass compiles each element pattern once (RE2)
                // and is crash-safe; a move-only regex cache cannot live in the std::function lambda below.
                if (inner_op == compare_type::regex) {
                    return {new regex_any_predicate(resource,
                                                    std::move(left_getter),
                                                    param_id,
                                                    parameters,
                                                    expr->regex_like(),
                                                    expr->regex_icase(),
                                                    expr->regex_negate(),
                                                    is_any,
                                                    session_tz)};
                }
                return {new simple_predicate(
                    resource,
                    [resource,
                     left_getter = std::move(left_getter),
                     param_id,
                     parameters,
                     inner_op,
                     is_any,
                     session_tz](const vector::data_chunk_t& chunk_left,
                                 const vector::data_chunk_t& chunk_right,
                                 size_t index_left,
                                 size_t index_right) -> core::result_wrapper_t<bool> {
                        auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
                        if (left_val.has_error()) {
                            return left_val.convert_error<bool>();
                        }
                        if (left_val.value().is_null()) {
                            return false;
                        }
                        const auto& arr_param = parameters->parameters.at(param_id);
                        // Empty sub-query list: compact_to_array_value returns the NA-null
                        // sentinel for a zero-row `x [NOT] IN (SELECT ...)`. PostgreSQL:
                        // `x = ANY(empty)` is false (IN () matches nothing) and
                        // `x <> ALL(empty)` is true (NOT IN () matches everything) — exactly
                        // the loop-exhausted result, so short-circuit before dereferencing
                        // the (null) array's children.
                        if (arr_param.is_null()) {
                            return !is_any;
                        }
                        const auto& arr = arr_param.children();
                        for (const auto& element : arr) {
                            if (element.is_null()) {
                                continue;
                            }
                            auto rhs = element.cast_as(left_val.value().type(), session_tz);
                            if (rhs.is_null()) {
                                continue;
                            }
                            core::result_wrapper_t<bool> cmp{false};
                            switch (inner_op) {
                                case compare_type::eq:
                                    cmp = evaluate_comp<std::equal_to<>>(resource, left_val.value(), rhs);
                                    break;
                                case compare_type::ne:
                                    cmp = evaluate_comp<std::not_equal_to<>>(resource, left_val.value(), rhs);
                                    break;
                                case compare_type::gt:
                                    cmp = evaluate_comp<std::greater<>>(resource, left_val.value(), rhs);
                                    break;
                                case compare_type::lt:
                                    cmp = evaluate_comp<std::less<>>(resource, left_val.value(), rhs);
                                    break;
                                case compare_type::gte:
                                    cmp = evaluate_comp<std::greater_equal<>>(resource, left_val.value(), rhs);
                                    break;
                                case compare_type::lte:
                                    cmp = evaluate_comp<std::less_equal<>>(resource, left_val.value(), rhs);
                                    break;
                                default:
                                    break;
                            }
                            if (cmp.has_error()) {
                                return cmp;
                            }
                            if (is_any && cmp.value()) {
                                return true;
                            }
                            if (!is_any && !cmp.value()) {
                                return false;
                            }
                        }
                        return !is_any;
                    })};
            }
            case compare_type::regex: {
                // Scalar regex. Fast path: a bound-parameter pattern is compiled ONCE (regex_predicate);
                // a NULL pattern matches nothing. An exotic non-parameter pattern (`col regexp <column/expr>`)
                // varies per row and is handled by the general per-row comparator.
                if (expressions::is_parameter(expr->right())) {
                    if (auto it = parameters->parameters.find(expressions::as_parameter(expr->right()));
                        it != parameters->parameters.end()) {
                        auto subject = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
                        if (it->second.is_null()) {
                            return {new regex_predicate(std::move(subject))};
                        }
                        const auto& pattern_val = it->second;
                        auto compiled = core::regex_t::compile(resource,
                                                               pattern_val.value<std::string_view>(),
                                                               expr->regex_icase());
                        return {new regex_predicate(std::move(subject), std::move(compiled))};
                    }
                }
                return {new simple_predicate(resource,
                                             make_regex_comparator(resource, function_registry, expr, parameters))};
            }
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

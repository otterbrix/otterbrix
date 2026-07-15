#include "simple_predicate.hpp"
#include "utils.hpp"

#include <components/expressions/like_to_regex.hpp>
#include <components/table/column_state.hpp>
#include <core/regex/regex.hpp>

#include <optional>
#include <string>
#include <string_view>
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
        // core::error_t, never a throw.
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
                       size_t index_right) -> core::result_wrapper_t<types::tri_bool_t> {
                auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
                auto right_val = right_getter(chunk_left, chunk_right, index_left, index_right);
                if (left_val.has_error()) {
                    return left_val.convert_error<types::tri_bool_t>();
                }
                if (right_val.has_error()) {
                    return right_val.convert_error<types::tri_bool_t>();
                }
                // SQL 3VL: a comparison with a NULL operand is UNKNOWN (not FALSE) -- the two
                // diverge under NOT, so collapsing to FALSE here would let NOT resurrect the row.
                if (left_val.value().is_null() || right_val.value().is_null()) {
                    return types::tri_bool_t::unknown;
                }
                const auto& subject_val = left_val.value();
                const auto& pattern_val = right_val.value();
                // Operand type guard: a non-string operand must be a returned error —
                // value<std::string_view>() on a non-string value dereferences its payload
                // as a std::string*.
                if (!types::is_string(subject_val.type().type()) || !types::is_string(pattern_val.type().type())) {
                    return core::error_t{core::error_code_t::comparison_failure,
                                         std::pmr::string{"incorrect argument type for regex", resource}};
                }
                auto compiled = core::regex_t::compile(resource, pattern_val.value<std::string_view>(), icase);
                if (compiled.has_error()) {
                    return compiled.error();
                }
                return types::tri_of(compiled.value().match(subject_val.value<std::string_view>()));
            };
        }

        simple_predicate::row_check_fn_t make_comparator(std::pmr::memory_resource* resource,
                                                         const compute::function_registry_t* function_registry,
                                                         const expressions::compare_expression_ptr& expr,
                                                         const logical_plan::storage_parameters* parameters,
                                                         core::date::timezone_offset_t session_tz) {
            auto left_getter = impl::create_value_getter(resource, function_registry, expr->left(), parameters);
            auto right_getter = impl::create_value_getter(resource, function_registry, expr->right(), parameters);
            const auto op = expr->type();
            return [resource,
                    left_getter = std::move(left_getter),
                    right_getter = std::move(right_getter),
                    op,
                    session_tz](const vector::data_chunk_t& chunk_left,
                                const vector::data_chunk_t& chunk_right,
                                size_t index_left,
                                size_t index_right) -> core::result_wrapper_t<types::tri_bool_t> {
                auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
                auto right_val = right_getter(chunk_left, chunk_right, index_left, index_right);
                if (left_val.has_error()) {
                    return left_val.convert_error<types::tri_bool_t>();
                }
                if (right_val.has_error()) {
                    return right_val.convert_error<types::tri_bool_t>();
                }
                // PostgreSQL rejects an IMPLICIT boolean <-> numeric comparison ("operator
                // does not exist: boolean = integer"). is_numeric(BOOLEAN) is true, so without
                // this guard cast_as would silently coerce the numeric via the int->bool cast
                // and compare — asymmetric and surprising. Reject when EXACTLY one side is
                // BOOLEAN and the other is a non-boolean numeric, so `bool_col = 1` errors.
                // Same-type bool=bool and numeric=numeric (neither trips l_bool != r_bool), and
                // any string / temporal / NULL pairing, are untouched. NULL operands skip the
                // guard (gated to UNKNOWN just below).
                if (!left_val.value().is_null() && !right_val.value().is_null()) {
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
                // SQL 3VL: a comparison with a NULL operand is UNKNOWN (not FALSE) -- the two
                // diverge under NOT, so collapsing to FALSE here would let NOT resurrect the row.
                if (left_val.value().is_null() || right_val.value().is_null()) {
                    return types::tri_bool_t::unknown;
                }
                // THE canonical bidirectional-promotion + compare semantics live in ONE place —
                // table::compare_values_promoting — shared with the pushed col-vs-col disk filter
                // (row_group_t::check_predicate), so the two paths cannot diverge. NULL operands
                // never reach it (gated to UNKNOWN above; the disk filter gates on validity the
                // same way before calling it).
                auto cmp = table::compare_values_promoting(left_val.value(), right_val.value(), op, session_tz);
                if (cmp.has_error()) {
                    return cmp.convert_error<types::tri_bool_t>();
                }
                return types::tri_of(cmp.value());
            };
        }

        // Scalar `col LIKE/ILIKE/regexp <literal>`: the pattern is a bound parameter, compiled ONCE here
        // with RE2 — crash-safe (a bad pattern is stored as an error and returned at eval, never thrown).
        // The subject is read through the shared value_getter. NOT LIKE / NOT ILIKE arrive wrapped in a
        // union_not predicate, so this only performs a positive match. A NULL subject (or a NULL pattern)
        // yields UNKNOWN (SQL 3VL), which stays distinct from FALSE under that NOT. No per-comparator
        // std::function: this is a real predicate subclass so the move-only compiled regex can live as a
        // member.
        class regex_predicate final : public predicate {
        public:
            regex_predicate(std::pmr::memory_resource* resource,
                            impl::value_getter subject,
                            core::result_wrapper_t<core::regex_t> compiled)
                : resource_(resource)
                , subject_(std::move(subject))
                , error_(compiled.has_error() ? std::optional<core::error_t>(compiled.error()) : std::nullopt)
                , re_(compiled.has_error() ? std::nullopt : std::optional<core::regex_t>(std::move(compiled.value()))) {}

            // NULL pattern: `col LIKE NULL` is unknown -> matches nothing.
            regex_predicate(std::pmr::memory_resource* resource, impl::value_getter subject)
                : resource_(resource)
                , subject_(std::move(subject)) {}

        private:
            core::result_wrapper_t<types::tri_bool_t> check_impl(const vector::data_chunk_t& chunk_left,
                                                                 const vector::data_chunk_t& chunk_right,
                                                                 size_t index_left,
                                                                 size_t index_right) override {
                if (error_) {
                    return *error_;
                }
                if (!re_) {
                    return types::tri_bool_t::unknown; // NULL pattern: `x LIKE NULL` is UNKNOWN
                }
                auto subject = subject_(chunk_left, chunk_right, index_left, index_right);
                if (subject.has_error()) {
                    return subject.convert_error<types::tri_bool_t>();
                }
                if (subject.value().is_null()) {
                    return types::tri_bool_t::unknown; // NULL subject: UNKNOWN, not FALSE
                }
                const auto& subject_val = subject.value();
                // Non-string subject (`int_col LIKE 'p'` — the validator does not type-check regex):
                // a returned error — never read the payload as a std::string*.
                if (!types::is_string(subject_val.type().type())) {
                    return core::error_t{core::error_code_t::comparison_failure,
                                         std::pmr::string{"incorrect argument type for regex", resource_}};
                }
                return types::tri_of(re_->match(subject_val.value<std::string_view>()));
            }

            std::pmr::memory_resource* resource_;
            impl::value_getter subject_;
            std::optional<core::error_t> error_;
            std::optional<core::regex_t> re_;
        };

        // Transparent hash/equal so the pattern cache below is probed with the raw string_view —
        // the per-row/per-element hit path allocates nothing; only a miss materializes the key.
        struct pattern_hash_t {
            using is_transparent = void;
            size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
        };
        struct pattern_eq_t {
            using is_transparent = void;
            bool operator()(std::string_view lhs, std::string_view rhs) const noexcept { return lhs == rhs; }
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
            core::result_wrapper_t<types::tri_bool_t> check_impl(const vector::data_chunk_t& chunk_left,
                                                                 const vector::data_chunk_t& chunk_right,
                                                                 size_t index_left,
                                                                 size_t index_right) override {
                auto left_val = left_getter_(chunk_left, chunk_right, index_left, index_right);
                if (left_val.has_error()) {
                    return left_val.convert_error<types::tri_bool_t>();
                }
                const auto& arr_param = parameters_->parameters.at(param_id_);
                // Empty sub-query list: `x = ANY(empty)` is FALSE, `x <> ALL(empty)` is TRUE
                // (loop-exhausted) — a total answer even for a NULL x, so checked first.
                if (arr_param.is_null()) {
                    return is_any_ ? types::tri_bool_t::no : types::tri_bool_t::yes;
                }
                // A NULL subject makes every element comparison UNKNOWN, so the whole fold is UNKNOWN.
                if (left_val.value().is_null()) {
                    return types::tri_bool_t::unknown;
                }
                const auto& subject_val = left_val.value();
                // Non-string subject (`int_col LIKE ANY(...)` — the validator does not type-check
                // regex): a returned error, never a payload read as a std::string*.
                if (!types::is_string(subject_val.type().type())) {
                    return core::error_t{core::error_code_t::comparison_failure,
                                         std::pmr::string{"incorrect argument type for regex", resource_}};
                }
                std::string_view subject = subject_val.value<std::string_view>();
                const auto& arr = arr_param.children();
                // Three-valued (SQL NULL) membership, mirroring the non-regex ANY/ALL lambda below:
                // a NULL pattern element (or one whose cast yields NULL) contributes an UNKNOWN
                // comparison, never a match. Remember that one was seen so the ALL exhausted-loop
                // result can drop the row.
                bool has_null_element = false;
                for (const auto& element : arr) {
                    if (element.is_null()) {
                        has_null_element = true;
                        continue;
                    }
                    // Text pattern element: read the bytes inline, no per-row logical_value_t. A
                    // non-text element (exotic `col LIKE ANY(SELECT int_col ...)`) is coerced to the subject's
                    // string type so it stringifies; a failed coercion (null) is skipped.
                    std::optional<types::logical_value_t> coerced;
                    std::string_view pattern;
                    if (types::is_string(element.type().type())) {
                        pattern = element.value<std::string_view>();
                    } else {
                        auto casted = element.cast_as(subject_val.type(), session_tz_);
                        if (casted.has_error()) {
                            return casted.convert_error<types::tri_bool_t>();
                        }
                        if (casted.value().is_null()) {
                            has_null_element = true;
                            continue;
                        }
                        coerced = std::move(casted.value());
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
                        return types::tri_bool_t::yes;
                    }
                    if (!is_any_ && !matched) {
                        return types::tri_bool_t::no;
                    }
                }
                // Loop exhausted with no decisive element. A NULL pattern element made a
                // comparison UNKNOWN, so the fold is UNKNOWN for ANY and ALL alike — the row is
                // dropped either way, and NOT cannot resurrect it.
                if (has_null_element) {
                    return types::tri_bool_t::unknown;
                }
                return is_any_ ? types::tri_bool_t::no : types::tri_bool_t::yes;
            }

            // Compile-once: look the element pattern up in the cache (heterogeneous string_view
            // probe — a hit allocates nothing), compiling + inserting on a miss.
            // Returns nullptr for a pattern that does not compile (cached as an empty slot).
            core::regex_t* compiled_for(std::string_view pattern) {
                if (auto it = cache_.find(pattern); it != cache_.end()) {
                    return it->second.has_value() ? &*it->second : nullptr;
                }
                const std::string converted =
                    re_like_ ? expressions::like_to_regex(std::string(pattern)) : std::string(pattern);
                auto compiled = core::regex_t::compile(resource_, converted, re_icase_);
                std::optional<core::regex_t> slot =
                    compiled.has_error() ? std::nullopt : std::optional<core::regex_t>(std::move(compiled.value()));
                auto [ins, _inserted] = cache_.emplace(std::pmr::string{pattern, resource_}, std::move(slot));
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
            std::pmr::unordered_map<std::pmr::string, std::optional<core::regex_t>, pattern_hash_t, pattern_eq_t>
                cache_;
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

    core::result_wrapper_t<std::vector<types::tri_bool_t>>
    simple_predicate::batch_check_impl(const vector::data_chunk_t& left,
                                       const vector::data_chunk_t& right,
                                       const vector::indexing_vector_t& left_indices,
                                       const vector::indexing_vector_t& right_indices,
                                       uint64_t count) {
        using types::tri_bool_t;
        switch (nested_type_) {
            case expressions::compare_type::union_and: {
                // 3VL AND: identity is TRUE; FALSE dominates, else UNKNOWN if any child is UNKNOWN.
                std::vector<tri_bool_t> result(count, tri_bool_t::yes);
                for (const auto& child : nested_) {
                    auto child_res = child->batch_check(left, right, left_indices, right_indices, count);
                    if (child_res.has_error()) {
                        return child_res;
                    }
                    for (uint64_t k = 0; k < count; ++k) {
                        result[k] = types::tri_and(result[k], child_res.value()[k]);
                    }
                }
                return result;
            }
            case expressions::compare_type::union_or: {
                // 3VL OR: identity is FALSE; TRUE dominates, else UNKNOWN if any child is UNKNOWN.
                std::vector<tri_bool_t> result(count, tri_bool_t::no);
                for (const auto& child : nested_) {
                    auto child_res = child->batch_check(left, right, left_indices, right_indices, count);
                    if (child_res.has_error()) {
                        return child_res;
                    }
                    for (uint64_t k = 0; k < count; ++k) {
                        result[k] = types::tri_or(result[k], child_res.value()[k]);
                    }
                }
                return result;
            }
            case expressions::compare_type::union_not: {
                // 3VL NOT: TRUE<->FALSE, UNKNOWN unchanged (so NOT does not resurrect a NULL row).
                auto result = nested_.front()->batch_check(left, right, left_indices, right_indices, count);
                if (result.has_error()) {
                    return result;
                }
                for (auto& v : result.value()) {
                    v = types::tri_not(v);
                }
                return result;
            }
            default:
                // fallback to row-by-row via func_
                std::vector<tri_bool_t> result(count);
                for (uint64_t k = 0; k < count; ++k) {
                    if (auto res = func_(left, right, left_indices.get_index(k), right_indices.get_index(k));
                        res.has_error()) {
                        return res.convert_error<std::vector<tri_bool_t>>();
                    } else {
                        result[k] = res.value();
                    }
                }
                return result;
        }
    }

    core::result_wrapper_t<types::tri_bool_t> simple_predicate::check_impl(const vector::data_chunk_t& chunk_left,
                                                                           const vector::data_chunk_t& chunk_right,
                                                                           size_t index_left,
                                                                           size_t index_right) {
        using types::tri_bool_t;
        switch (nested_type_) {
            case expressions::compare_type::union_and: {
                // 3VL AND: FALSE dominates (short-circuit); a later FALSE can still override an
                // accumulated UNKNOWN, so only FALSE ends the fold early.
                tri_bool_t acc = tri_bool_t::yes;
                for (const auto& predicate : nested_) {
                    auto res = predicate->check(chunk_left, chunk_right, index_left, index_right);
                    if (res.has_error()) {
                        return res;
                    }
                    acc = types::tri_and(acc, res.value());
                    if (acc == tri_bool_t::no) {
                        return acc;
                    }
                }
                return acc;
            }
            case expressions::compare_type::union_or: {
                // 3VL OR: TRUE dominates (short-circuit); only TRUE ends the fold early.
                tri_bool_t acc = tri_bool_t::no;
                for (const auto& predicate : nested_) {
                    auto res = predicate->check(chunk_left, chunk_right, index_left, index_right);
                    if (res.has_error()) {
                        return res;
                    }
                    acc = types::tri_or(acc, res.value());
                    if (acc == tri_bool_t::yes) {
                        return acc;
                    }
                }
                return acc;
            }
            case expressions::compare_type::union_not: {
                auto res = nested_.front()->check(chunk_left, chunk_right, index_left, index_right);
                if (!res.has_error()) {
                    res.value() = types::tri_not(res.value());
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
            case compare_type::ne:
            case compare_type::gt:
            case compare_type::gte:
            case compare_type::lt:
            case compare_type::lte:
                // One comparator for all six operators: the op rides expr->type() into the shared
                // table::compare_values_promoting helper (single source with the pushed disk filter).
                return {new simple_predicate(
                    resource,
                    make_comparator(resource, function_registry, expr, parameters, session_tz))};
            case compare_type::any:
            case compare_type::all: {
                // inner_op is guaranteed valid by the transformer (an unmapped ANY/ALL operator is
                // rejected there, not silently defaulted to `=`).
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
                                 size_t index_right) -> core::result_wrapper_t<types::tri_bool_t> {
                        auto left_val = left_getter(chunk_left, chunk_right, index_left, index_right);
                        if (left_val.has_error()) {
                            return left_val.convert_error<types::tri_bool_t>();
                        }
                        const auto& arr_param = parameters->parameters.at(param_id);
                        // Empty sub-query list: compact_to_array_value returns the NA-null
                        // sentinel for a zero-row `x [NOT] IN (SELECT ...)`. PostgreSQL:
                        // `x = ANY(empty)` is FALSE (IN () matches nothing) and
                        // `x <> ALL(empty)` is TRUE (NOT IN () matches everything) — exactly
                        // the loop-exhausted result, so short-circuit before dereferencing
                        // the (null) array's children. This precedes the NULL-left check
                        // below: over an empty set the answer is total regardless of x, so
                        // even a NULL x yields `NULL NOT IN (empty)` = TRUE / `NULL IN (empty)` = FALSE.
                        if (arr_param.is_null()) {
                            return is_any ? types::tri_bool_t::no : types::tri_bool_t::yes;
                        }
                        // A NULL left operand makes every element comparison UNKNOWN, so the whole
                        // ANY / ALL is UNKNOWN.
                        if (left_val.value().is_null()) {
                            return types::tri_bool_t::unknown;
                        }
                        const auto& arr = arr_param.children();
                        // Three-valued (SQL NULL) membership: a NULL element (or one whose cast to the left
                        // type yields NULL) contributes an UNKNOWN comparison, never a match. Remember that
                        // one was seen so the ALL / NOT-IN exhausted-loop result below can drop the row.
                        bool has_null_element = false;
                        for (const auto& element : arr) {
                            if (element.is_null()) {
                                has_null_element = true;
                                continue;
                            }
                            auto rhs_rw = element.cast_as(left_val.value().type(), session_tz);
                            if (rhs_rw.has_error()) {
                                return rhs_rw.convert_error<types::tri_bool_t>();
                            }
                            if (rhs_rw.value().is_null()) {
                                has_null_element = true;
                                continue;
                            }
                            const auto& rhs = rhs_rw.value();
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
                                return cmp.convert_error<types::tri_bool_t>();
                            }
                            if (is_any && cmp.value()) {
                                return types::tri_bool_t::yes;
                            }
                            if (!is_any && !cmp.value()) {
                                return types::tri_bool_t::no;
                            }
                        }
                        // Loop exhausted with no decisive element. A NULL element (or one whose
                        // cast yielded NULL) made a comparison UNKNOWN, so the fold is UNKNOWN for
                        // ANY and ALL alike: `x = ANY(S with a NULL)` on a miss is UNKNOWN, not
                        // FALSE, and `x <> ALL(S with a NULL)` (NOT IN) is UNKNOWN, not TRUE --
                        // either way the row is dropped, and NOT cannot resurrect it.
                        if (has_null_element) {
                            return types::tri_bool_t::unknown;
                        }
                        return is_any ? types::tri_bool_t::no : types::tri_bool_t::yes;
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
                            return {new regex_predicate(resource, std::move(subject))};
                        }
                        const auto& pattern_val = it->second;
                        // Non-string pattern parameter: stored as the predicate's error (returned at
                        // eval through the same channel as a failed compile) — never read as a string.
                        if (!types::is_string(pattern_val.type().type())) {
                            return {new regex_predicate(
                                resource,
                                std::move(subject),
                                core::error_t{core::error_code_t::comparison_failure,
                                              std::pmr::string{"incorrect argument type for regex", resource}})};
                        }
                        auto compiled = core::regex_t::compile(resource,
                                                               pattern_val.value<std::string_view>(),
                                                               expr->regex_icase());
                        return {new regex_predicate(resource, std::move(subject), std::move(compiled))};
                    }
                }
                return {new simple_predicate(resource,
                                             make_regex_comparator(resource, function_registry, expr, parameters))};
            }
            case compare_type::all_false:
                return {new simple_predicate(resource,
                                             [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t)
                                                 -> core::result_wrapper_t<types::tri_bool_t> {
                                                 return types::tri_bool_t::no;
                                             })};
            case compare_type::is_null: {
                // IS NULL / IS NOT NULL are themselves total predicates (never UNKNOWN): they ask
                // about validity directly and answer a definite TRUE / FALSE.
                return {new simple_predicate(
                    resource,
                    [column_path = std::get<expressions::key_t>(expr->left()).path()](
                        const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t&,
                        size_t index_left,
                        size_t) -> core::result_wrapper_t<types::tri_bool_t> {
                        return types::tri_of(!chunk_left.at(column_path)->validity().row_is_valid(index_left));
                    })};
            }
            case compare_type::is_not_null: {
                return {new simple_predicate(
                    resource,
                    [column_path = std::get<expressions::key_t>(expr->left()).path()](
                        const vector::data_chunk_t& chunk_left,
                        const vector::data_chunk_t&,
                        size_t index_left,
                        size_t) -> core::result_wrapper_t<types::tri_bool_t> {
                        return types::tri_of(chunk_left.at(column_path)->validity().row_is_valid(index_left));
                    })};
            }
            case compare_type::all_true:
            default:
                return {new simple_predicate(resource,
                                             [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t)
                                                 -> core::result_wrapper_t<types::tri_bool_t> {
                                                 return types::tri_bool_t::yes;
                                             })};
        }
    }

} // namespace components::operators::predicates

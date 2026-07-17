#include "full_scan.hpp"

#include <components/expressions/clone_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/like_to_regex.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <core/regex/regex.hpp>
#include <services/disk/manager_disk.hpp>

#include <optional>

namespace components::operators {

    namespace {
        namespace expr = expressions;

        // Collect the column key paths and bound-parameter ids the expression references (key ->
        // path; parameter -> id; nested scalar/function -> recurse). The paths tell
        // row_group_t::check_predicate which columns to materialize into the per-row chunk; the ids
        // bound the parameter snapshot shipped with an expression_filter_t.
        void collect_refs(const expr::param_storage& p,
                          std::pmr::vector<std::pmr::vector<size_t>>& paths,
                          std::pmr::vector<core::parameter_id_t>& param_ids) {
            if (expr::is_key(p)) {
                const auto& path = expr::as_key(p).path();
                // Uses-allocator construction: the outer pmr::vector propagates its resource inward.
                paths.emplace_back(path.begin(), path.end());
                return;
            }
            if (expr::is_parameter(p)) {
                param_ids.push_back(expr::as_parameter(p));
                return;
            }
            const auto& e = expr::as_expr(p);
            if (e->group() == expr::expression_group::scalar) {
                const auto& s = reinterpret_cast<const expr::scalar_expression_ptr&>(e);
                for (const auto& sub : s->params()) {
                    collect_refs(sub, paths, param_ids);
                }
            } else {
                const auto& f = reinterpret_cast<const expr::function_expression_ptr&>(e);
                for (const auto& sub : f->args()) {
                    collect_refs(sub, paths, param_ids);
                }
            }
        }
    } // namespace

    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters,
                        core::date::timezone_offset_t session_tz) {
        if (!expression || expression->type() == expressions::compare_type::all_true) {
            return std::unique_ptr<table::table_filter_t>{};
        }
        if (expression->type() == expressions::compare_type::all_false) {
            // Callers short-circuit all_false before filter construction
            // (source_next / pushed_reduce_scan). If one ever does not, fail
            // with an error instead of the previous Release-erased assert
            // falling through into the switch below.
            return core::error_t{core::error_code_t::physical_plan_error,
                                 std::pmr::string{"all_false predicate reached filter construction", resource}};
        }
        switch (expression->type()) {
            case expressions::compare_type::union_and: {
                auto filter = std::make_unique<table::conjunction_and_filter_t>();
                for (const auto& child : expression->children()) {
                    auto child_result =
                        transform_predicate(resource,
                                            reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters,
                                            session_tz);
                    if (child_result.has_error()) {
                        return child_result;
                    }
                    if (child_result.value()) {
                        filter->child_filters.emplace_back(std::move(child_result.value()));
                    }
                }
                if (filter->child_filters.size() < 2) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"incomplete AND filter — expression construction error", resource}};
                }
                return std::unique_ptr<table::table_filter_t>(std::move(filter));
            }
            case expressions::compare_type::union_or: {
                auto filter = std::make_unique<table::conjunction_or_filter_t>();
                for (const auto& child : expression->children()) {
                    auto child_result =
                        transform_predicate(resource,
                                            reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters,
                                            session_tz);
                    if (child_result.has_error()) {
                        return child_result;
                    }
                    if (child_result.value()) {
                        filter->child_filters.emplace_back(std::move(child_result.value()));
                    }
                }
                if (filter->child_filters.size() < 2) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"incomplete OR filter — expression construction error", resource}};
                }
                return std::unique_ptr<table::table_filter_t>(std::move(filter));
            }
            case expressions::compare_type::union_not: {
                auto filter = std::make_unique<table::conjunction_not_filter_t>();
                filter->child_filters.reserve(expression->children().size());
                for (const auto& child : expression->children()) {
                    auto child_result =
                        transform_predicate(resource,
                                            reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters,
                                            session_tz);
                    if (child_result.has_error()) {
                        return child_result;
                    }
                    if (child_result.value()) {
                        filter->child_filters.emplace_back(std::move(child_result.value()));
                    }
                }
                if (filter->child_filters.empty()) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"empty NOT filter — expression construction error", resource}};
                }
                return std::unique_ptr<table::table_filter_t>(std::move(filter));
            }
            case expressions::compare_type::any:
            case expressions::compare_type::all: {
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                auto param_id = std::get<core::parameter_id_t>(expression->right());
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                if (parameters->parameters.find(param_id) == parameters->parameters.end()) {
                    return core::error_t{
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"parameter not found in expression to filter conversion", resource}};
                }
                auto inner_op = expression->inner_op();
                if (inner_op == expressions::compare_type::invalid) {
                    // An unmapped ANY/ALL operator must surface as an error, never silently become `=`
                    // (finding 5). The transformer already rejects these; this is defence in depth.
                    return core::error_t{
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"unsupported operator in ANY/ALL subquery comparison", resource}};
                }
                // For a subscript path (v[i]) the comparison is against the element
                // type, not the ARRAY/LIST column type; type_from_path resolves it.
                const auto& col_type = types::complex_logical_type::type_from_path(types, path);
                const auto& arr_param = parameters->parameters.at(param_id);
                const bool is_any = expression->type() == expressions::compare_type::any;
                const bool is_regex = inner_op == expressions::compare_type::regex;
                const bool re_like = is_regex && expression->regex_like();
                const bool re_icase = is_regex && expression->regex_icase();
                const bool re_negate = is_regex && expression->regex_negate();

                // Build the per-element leaf filters (regex_filter for LIKE/ILIKE, constant_filter otherwise).
                // Empty / 0-row sub-query: the array param is the NA-null sentinel; .children() is invalid, so
                // leave the leaf list empty (an empty OR matches nothing, an empty AND matches everything).
                //
                // Three-valued (SQL NULL) membership: a NULL element of S contributes an UNKNOWN comparison,
                // never a match, so it yields no leaf. Track whether S carried one — the ALL / NOT-IN branch
                // below must honour it (`x <> ALL(S)` with a NULL in S is UNKNOWN for every non-matching row).
                std::pmr::vector<std::unique_ptr<table::table_filter_t>> leaves(resource);
                bool has_null_element = false;
                if (!arr_param.is_null()) {
                    // Regex over a non-string subject column would reinterpret the column payload as
                    // string bytes inside the scan; PostgreSQL rejects it as a type error. Mirrors
                    // the in-memory regex predicates' operand guard.
                    if (is_regex && !types::is_string(col_type.type())) {
                        return core::error_t{core::error_code_t::comparison_failure,
                                             std::pmr::string{"incorrect argument type for regex", resource}};
                    }
                    const auto& arr = arr_param.children();
                    leaves.reserve(arr.size());
                    for (const auto& val : arr) {
                        if (is_regex) {
                            // A NULL pattern element contributes an UNKNOWN comparison, never a match
                            // (three-valued) — and must never be dereferenced as a string.
                            if (val.is_null()) {
                                has_null_element = true;
                                continue;
                            }
                            // Non-text element (exotic `col LIKE ANY(SELECT int_col ...)`): coerce to
                            // the subject's string type so it stringifies, mirroring the in-memory
                            // regex_any_predicate; a coercion that yields NULL is an UNKNOWN comparison.
                            std::optional<types::logical_value_t> coerced_el;
                            std::string_view raw;
                            if (types::is_string(val.type().type())) {
                                raw = val.value<std::string_view>();
                            } else {
                                auto casted = val.cast_as(col_type, session_tz);
                                if (casted.has_error()) {
                                    return casted.convert_error<std::unique_ptr<table::table_filter_t>>();
                                }
                                if (casted.value().is_null()) {
                                    has_null_element = true;
                                    continue;
                                }
                                coerced_el = std::move(casted.value());
                                raw = coerced_el->value<std::string_view>();
                            }
                            std::pmr::string pat{resource};
                            if (re_like) {
                                const std::string converted = expressions::like_to_regex(std::string(raw));
                                pat.assign(converted.begin(), converted.end());
                            } else {
                                pat.assign(raw.begin(), raw.end());
                            }
                            // Compile ONCE at plan time: an element pattern RE2 rejects must surface as
                            // an error here — matching the in-memory path — never a filter that silently
                            // drops every row (regex_filter_t::matches relies on this validation).
                            auto compiled = core::regex_t::compile(resource, pat, re_icase);
                            if (compiled.has_error()) {
                                return compiled.convert_error<std::unique_ptr<table::table_filter_t>>();
                            }
                            leaves.emplace_back(
                                std::make_unique<table::regex_filter_t>(std::move(pat), re_icase, indices));
                        } else {
                            // A NULL element is NA-typed; cast_as() from NA is unhandled and throws
                            // (operations_helper: simple_physical_type_switch default). Guard it out here —
                            // it produces no leaf and marks the set NULL-bearing. A non-null element whose
                            // cast to the column type yields NULL is likewise an UNKNOWN comparison.
                            if (val.is_null()) {
                                has_null_element = true;
                                continue;
                            }
                            if (val.type() == col_type) {
                                leaves.emplace_back(
                                    std::make_unique<table::constant_filter_t>(inner_op, val, indices));
                                continue;
                            }
                            auto coerced = val.cast_as(col_type, session_tz);
                            if (coerced.has_error()) {
                                return coerced.convert_error<std::unique_ptr<table::table_filter_t>>();
                            }
                            if (coerced.value().is_null()) {
                                has_null_element = true;
                                continue;
                            }
                            leaves.emplace_back(
                                std::make_unique<table::constant_filter_t>(inner_op, coerced.value(), indices));
                        }
                    }
                }
                const bool empty = leaves.empty();

                // x op ALL(S) (NOT IN is `x <> ALL(S)`) when S carries a NULL: no outer row can be TRUE — a
                // non-null element that violates makes the row FALSE, otherwise the NULL makes it UNKNOWN;
                // either way the row is dropped, so the whole predicate collapses to a constant FALSE (an
                // empty OR matches nothing). This is the classic `x NOT IN (SELECT ... with a NULL)` → no
                // rows, and it holds for the regex family too: `x [NOT] LIKE ALL(S with a NULL)` is at best
                // UNKNOWN (`x LIKE NULL` is UNKNOWN), so both LIKE ALL and NOT LIKE ALL drop every row.
                // ANY / IN is unaffected: a NULL element only adds UNKNOWN, and for `x op ANY(S)`
                // UNKNOWN and FALSE both drop the row, so the OR of the non-null leaves already agrees.
                if (!is_any && has_null_element) {
                    return std::unique_ptr<table::table_filter_t>(std::make_unique<table::conjunction_or_filter_t>());
                }

                // NOT LIKE ANY = OR_p not(match); NOT LIKE ALL = not(any match) = conjunction_not over all
                // (conjunction_not is true iff no child matches). A negated match must EXCLUDE a NULL subject
                // (SQL: NULL NOT LIKE p = NULL); the disk regex reads a NULL as empty and would otherwise pass,
                // so guard non-empty negations with is_not_null. (Empty stays unguarded: an empty OR is false,
                // an empty conjunction_not is vacuously true — matching PostgreSQL over an empty pattern set.)
                if (re_negate) {
                    std::unique_ptr<table::table_filter_t> neg;
                    if (is_any) {
                        auto or_f = std::make_unique<table::conjunction_or_filter_t>();
                        for (auto& leaf : leaves) {
                            auto not_f = std::make_unique<table::conjunction_not_filter_t>();
                            not_f->child_filters.emplace_back(std::move(leaf));
                            or_f->child_filters.emplace_back(std::move(not_f));
                        }
                        neg = std::move(or_f);
                    } else {
                        auto not_f = std::make_unique<table::conjunction_not_filter_t>();
                        for (auto& leaf : leaves) {
                            not_f->child_filters.emplace_back(std::move(leaf));
                        }
                        neg = std::move(not_f);
                    }
                    if (empty) {
                        return neg;
                    }
                    auto guard = std::make_unique<table::conjunction_and_filter_t>();
                    guard->child_filters.emplace_back(
                        std::make_unique<table::is_null_filter_t>(expressions::compare_type::is_not_null, indices));
                    guard->child_filters.emplace_back(std::move(neg));
                    return std::unique_ptr<table::table_filter_t>(std::move(guard));
                }

                // Positive: OR (any) / AND (all) of the per-element match filters.
                auto outer = is_any ? std::unique_ptr<table::conjunction_filter_t>(
                                          std::make_unique<table::conjunction_or_filter_t>())
                                    : std::unique_ptr<table::conjunction_filter_t>(
                                          std::make_unique<table::conjunction_and_filter_t>());
                for (auto& leaf : leaves) {
                    outer->child_filters.emplace_back(std::move(leaf));
                }
                return outer;
            }
            case expressions::compare_type::invalid:
                return core::error_t{
                    core::error_code_t::physical_plan_error,
                    std::pmr::string{"unsupported compare_type in expression to filter conversion", resource}};
            case expressions::compare_type::is_null:
            case expressions::compare_type::is_not_null: {
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                return std::unique_ptr<table::table_filter_t>(
                    std::make_unique<table::is_null_filter_t>(expression->type(), std::move(indices)));
            }
            default: {
                // Shape (B): one operand is a FUNCTION/ARITHMETIC expression over column(s) and the
                // other a bound parameter — e.g. WHERE substring(s,1,3)='abc', WHERE x+1>5. Not
                // representable as a constant_filter_t, so ship an expression_filter_t: a deep clone
                // of the compare, the referenced column paths, and a snapshot of the referenced
                // parameter values. The clone (canonical expressions::clone_expression) is REQUIRED:
                // the filter is MOVED to (and destroyed on) the disk agent's thread, and sharing the
                // operator's compare_expression_ptr would mutate the expression's thread-UNSAFE
                // intrusive refcount from two threads (cf. pushed_aggregate_spec.hpp's
                // no-expression_ptr-across-mailbox rule) — the clone keeps ownership single-threaded,
                // the filter its sole owner end to end. The per-row evaluator is built AGENT-SIDE
                // (its value_getter closures capture the agent's resource + function registry, which
                // cannot cross the mailbox), mirroring what operator_match does for the in-memory path.
                if (expressions::is_expr(expression->left()) || expressions::is_expr(expression->right())) {
                    std::pmr::vector<std::pmr::vector<size_t>> column_paths{resource};
                    std::pmr::vector<core::parameter_id_t> param_ids{resource};
                    collect_refs(expression->left(), column_paths, param_ids);
                    collect_refs(expression->right(), column_paths, param_ids);
                    // Snapshot ONLY the parameters the expression references: every bound parameter is
                    // a param_storage alternative (compare operand / scalar param / function arg) that
                    // collect_refs enumerates, so the referenced-id set is complete — an unrelated
                    // query parameter (e.g. a large sub-query array) never ships with the filter.
                    std::pmr::unordered_map<core::parameter_id_t, types::logical_value_t> param_snapshot{resource};
                    for (const auto& id : param_ids) {
                        if (auto pit = parameters->parameters.find(id); pit != parameters->parameters.end()) {
                            param_snapshot.emplace(id, pit->second);
                        }
                    }
                    auto clone = expressions::clone_expression(resource, expression);
                    return std::unique_ptr<table::table_filter_t>(std::make_unique<table::expression_filter_t>(
                        std::move(reinterpret_cast<expressions::compare_expression_ptr&>(clone)),
                        std::move(column_paths),
                        std::move(param_snapshot),
                        session_tz));
                }
                // A disk table_filter_t is `column OP constant` only: the LEFT operand must be a key
                // and the RIGHT a bound parameter (the param_storage alternative that is neither a key
                // nor a nested expression). A column-vs-column comparison (right is a key_t) or any
                // other shape is not representable here — return a clean error instead of std::get
                // throwing bad_variant_access (create_plan_match routes such predicates to
                // operator_match; this is a defensive guard for any other caller, and with the asserts
                // erased in Release it stops a bad_variant_access — Rule 2: no exceptions).
                // Column-vs-column `a.x OP a.y`: both operands are columns -> a column_column_filter_t that
                // fetches both values per row and compares (is_pure_compare only accepts a plain comparison).
                if (expressions::is_key(expression->left()) && expressions::is_key(expression->right())) {
                    const auto& lp = expressions::as_key(expression->left()).path();
                    const auto& rp = expressions::as_key(expression->right()).path();
                    // PostgreSQL rejects an implicit boolean <-> numeric comparison ("operator does not
                    // exist: boolean = integer"). Mirror the in-memory make_comparator guard so col-vs-col
                    // pushdown does not silently coerce and compare a bool column against a numeric column.
                    const auto& lt = types::complex_logical_type::type_from_path(types, lp);
                    const auto& rt = types::complex_logical_type::type_from_path(types, rp);
                    const bool l_bool = lt.to_physical_type() == types::physical_type::BOOL;
                    const bool r_bool = rt.to_physical_type() == types::physical_type::BOOL;
                    const auto other = l_bool ? rt.type() : lt.type();
                    if (l_bool != r_bool && types::is_numeric(other)) {
                        return core::error_t{
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"operator does not exist: boolean = numeric type", resource}};
                    }
                    std::pmr::vector<uint64_t> li(lp.begin(), lp.end(), lp.get_allocator().resource());
                    std::pmr::vector<uint64_t> ri(rp.begin(), rp.end(), rp.get_allocator().resource());
                    return std::unique_ptr<table::table_filter_t>(
                        std::make_unique<table::column_column_filter_t>(expression->type(),
                                                                        std::move(li),
                                                                        std::move(ri),
                                                                        session_tz));
                }
                if (!expressions::is_key(expression->left()) || !expressions::is_parameter(expression->right())) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"unexpected operand shape in expression to filter conversion", resource}};
                }
                const auto& path = expressions::as_key(expression->left()).path();
                auto id = expressions::as_parameter(expression->right());
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                auto it = parameters->parameters.find(id);
                if (it == parameters->parameters.end()) {
                    return core::error_t{
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"parameter not found in expression to filter conversion", resource}};
                }
                // LIKE / ILIKE / regexp: `it->second` is the like_to_regex-converted pattern (a regex, not an
                // orderable constant). Build a regex_filter_t that holds it as a plain pmr::string (NOT a
                // logical_value_t — Rule 1) and matches with RE2, case-insensitively for ILIKE. Checked before
                // the ENUM / cast logic below (which only makes sense for orderable constants).
                if (expression->type() == expressions::compare_type::regex) {
                    const auto& pat = it->second;
                    // NULL pattern (`col LIKE NULL` nested under AND/OR reaches filter construction —
                    // the top-level short-circuit in source_next only sees non-union compares):
                    // UNKNOWN matches nothing — an empty OR; never dereference the NULL's payload.
                    if (pat.is_null()) {
                        return std::unique_ptr<table::table_filter_t>(
                            std::make_unique<table::conjunction_or_filter_t>());
                    }
                    // Operand type guard (mirrors the in-memory regex predicates): a non-string subject
                    // column would be reinterpreted as string bytes inside the scan, a non-string
                    // pattern parameter as a std::string* here. PostgreSQL rejects both as type errors.
                    const auto& subj_type = types::complex_logical_type::type_from_path(types, path);
                    if (!types::is_string(subj_type.type()) || !types::is_string(pat.type().type())) {
                        return core::error_t{core::error_code_t::comparison_failure,
                                             std::pmr::string{"incorrect argument type for regex", resource}};
                    }
                    // Compile ONCE at plan time: a pattern RE2 rejects (raw regexp with a backreference,
                    // lookahead, ...) must surface as an error on the scan error channel — identical to
                    // the in-memory regex_predicate — never a filter that silently drops every row
                    // (regex_filter_t::matches relies on this validation).
                    auto compiled =
                        core::regex_t::compile(resource, pat.value<std::string_view>(), expression->regex_icase());
                    if (compiled.has_error()) {
                        return compiled.convert_error<std::unique_ptr<table::table_filter_t>>();
                    }
                    return std::unique_ptr<table::table_filter_t>(
                        std::make_unique<table::regex_filter_t>(std::pmr::string{pat.value<std::string_view>(), resource},
                                                                expression->regex_icase(),
                                                                std::move(indices)));
                }
                // Coerce STRING parameter to ENUM ordinal when the target column is an ENUM:
                // compare semantics see int32 storage on both sides, so the literal must be
                // resolved to its ordinal up-front (else the filter matches 0 rows).
                // For a subscript path (v[i]) this resolves to the element type, so the
                // constant is coerced to what the per-element compare actually sees.
                const auto& col_type = types::complex_logical_type::type_from_path(types, path);
                const auto& param_value = it->second;
                if (col_type.type() == types::logical_type::ENUM &&
                    param_value.type().type() == types::logical_type::STRING_LITERAL) {
                    auto key = param_value.value<std::string_view>();
                    auto coerced = types::logical_value_t::create_enum(resource, col_type, key);
                    if (coerced.type().type() == types::logical_type::NA) {
                        return core::error_t{core::error_code_t::invalid_parameter,
                                             std::pmr::string{std::string{"enum value '"} + std::string{key} +
                                                                  "' not found in ENUM column",
                                                              resource}};
                    }
                    // Storage holds the ordinal as int32 (ENUM physical_type=INT32).
                    // constant_filter_t's compare path doesn't auto-coerce ENUM<->INT32,
                    // so wrap the ordinal as a plain INT32 logical_value_t.
                    types::logical_value_t ordinal_val{resource, coerced.value<int32_t>()};
                    return std::unique_ptr<table::table_filter_t>(
                        std::make_unique<table::constant_filter_t>(expression->type(),
                                                                   std::move(ordinal_val),
                                                                   std::move(indices)));
                }
                if (!param_value.is_null() && param_value.type() != col_type) {
                    auto coerced = param_value.cast_as(col_type, session_tz);
                    if (coerced.has_error()) {
                        return coerced.convert_error<std::unique_ptr<table::table_filter_t>>();
                    }
                    if (!coerced.value().is_null()) {
                        return std::unique_ptr<table::table_filter_t>(
                            std::make_unique<table::constant_filter_t>(expression->type(),
                                                                       std::move(coerced.value()),
                                                                       std::move(indices)));
                    }
                }
                return std::unique_ptr<table::table_filter_t>(
                    std::make_unique<table::constant_filter_t>(expression->type(), it->second, std::move(indices)));
            }
        }
    }

    full_scan::full_scan(std::pmr::memory_resource* resource,
                         log_t log,
                         components::catalog::oid_t table_oid,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit,
                         std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log, operator_type::full_scan)
        , table_oid_(table_oid)
        , expression_(expression)
        , limit_(limit)
        , projected_cols_(std::move(projected_cols)) {}

    vector::data_chunk_t full_scan::make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types) {
        std::pmr::vector<types::complex_logical_type> projected_types(resource_);
        if (projected_cols_.empty()) {
            projected_types = types;
        } else {
            projected_types.reserve(projected_cols_.size());
            for (auto idx : projected_cols_) {
                if (idx < types.size()) {
                    projected_types.push_back(types[idx]);
                }
            }
        }
        return vector::data_chunk_t{resource_, projected_types, 0};
    }

    // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
    // FIRST call: one-time setup (short-circuits, build the filter, the storage_types await for the
    //   empty-guard schema), then OPEN the cursor (storage_fetch_next_batch, cursor_id==0, passing
    //   the filter + offset+limit head cap) and return its first batch.
    // SUBSEQUENT calls: ADVANCE the SAME cursor (cursor_id_!=0, no filter) and return one batch.
    // Each call does at most ONE cross-actor fetch await; the N awaits are sequential across calls
    // in this nested operator coroutine (driven by execute_pipeline), so the single-slot awaited
    // continuation is republished+cleared between awaits — no lost-wakeup. Peak scan memory = one
    // batch (zero pins survive a round-trip; the agent re-seeks a transient scan state from a
    // stored position).
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    full_scan::source_next(pipeline::context_t* ctx) {
        if (drained_) {
            co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
        }

        // No-table sentinel (no-FROM SELECT): emit ONE synthetic single-row batch
        // carrying one placeholder column (not the 0-column drain sentinel), then drain.
        // operator_select_t projects its constant/arithmetic columns over this one row to
        // produce the single constants row (the placeholder is ignored), matching the
        // legacy virtual-row path. No disk round-trip.
        if (table_oid_ == components::catalog::INVALID_OID) {
            drained_ = true;
            std::pmr::vector<types::complex_logical_type> types(resource_);
            types.emplace_back(types::logical_type::BOOLEAN);
            vector::data_chunk_t row{resource_, types, 1};
            row.set_cardinality(1);
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(row));
        }

        if (!opened_) {
            opened_ = true;

            // Short-circuit: all_false → empty result, immediately drained.
            if (expression_ && expression_->type() == expressions::compare_type::all_false) {
                drained_ = true;
                co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
            }

            // Short-circuit: null parameter in a scalar comparison — SQL NULL semantics.
            // col OP NULL → always false → empty. col OP ALL(empty) is vacuously true → scan all.
            bool null_param_skip_filter = false;
            if (expression_ && !expression_->is_union() && expression_->type() != expressions::compare_type::is_null &&
                expression_->type() != expressions::compare_type::is_not_null &&
                std::holds_alternative<core::parameter_id_t>(expression_->right())) {
                auto pid = std::get<core::parameter_id_t>(expression_->right());
                auto it = ctx->parameters.parameters.find(pid);
                if (it != ctx->parameters.parameters.end() && it->second.is_null()) {
                    if (expression_->type() != expressions::compare_type::all) {
                        drained_ = true;
                        co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
                    }
                    null_param_skip_filter = true;
                }
            }

            // Get types to build the filter (await 1). Cached for the no-data empty-guard below.
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            guard_types_ = co_await std::move(tf);

            std::unique_ptr<table::table_filter_t> filter;
            if (!null_param_skip_filter) {
                auto filter_result =
                    transform_predicate(resource_, expression_, guard_types_, &ctx->parameters, ctx->session_tz);
                if (filter_result.has_error()) {
                    set_error(filter_result.error());
                    mark_failed();
                    co_return core::result_wrapper_t<vector::data_chunk_t>(filter_result.error());
                }
                filter = std::move(filter_result.value());
            }

            // OPEN the cursor: the read-cap (offset+limit head cap) is pushed down as the agent's
            // post-filter matched-row COUNT cap. SELECT OFFSET is applied by operator_limit above,
            // so every scan receives offset()==0 and head_cap() == limit here.
            const int64_t scan_limit = limit_.head_cap();

            auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_fetch_next_batch,
                                             ctx->session,
                                             table_oid_,
                                             cursor_id_, // 0 == OPEN
                                             std::move(filter),
                                             scan_limit,
                                             projected_cols_,
                                             ctx->txn);
            auto fetch_result = co_await std::move(sf);
            if (fetch_result.has_error()) {
                set_error(fetch_result.error());
                mark_failed();
                co_return fetch_result.convert_error<vector::data_chunk_t>();
            }
            auto reply = std::move(fetch_result.value());
            cursor_id_ = reply.cursor_id;
            co_return co_await emit_or_skip(ctx, std::move(reply.batch));
        }

        // ADVANCE: read one more batch from the open cursor (filter dropped — the agent owns it).
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_fetch_next_batch,
                                         ctx->session,
                                         table_oid_,
                                         cursor_id_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         int64_t{-1},
                                         projected_cols_,
                                         ctx->txn);
        auto fetch_result = co_await std::move(sf);
        if (fetch_result.has_error()) {
            set_error(fetch_result.error());
            mark_failed();
            co_return fetch_result.convert_error<vector::data_chunk_t>();
        }
        auto reply = std::move(fetch_result.value());
        co_return co_await emit_or_skip(ctx, std::move(reply.batch));
    }

    // Apply the drained empty-guard to one fetched batch. (OFFSET is applied by operator_limit
    // above; every scan receives offset()==0, so there is no per-batch skip / re-fetch.)
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    full_scan::emit_or_skip(pipeline::context_t* /*ctx*/, std::unique_ptr<vector::data_chunk_t> batch) {
        const uint64_t sz = batch ? batch->size() : 0;

        // Drained: the agent replied a cardinality-0 batch (and erased its cursor).
        if (sz == 0) {
            drained_ = true;
            // Emit ONE schema'd 0-row guard the first time the source produces nothing, so a
            // scalar aggregate emits COUNT=0 and an OUTER join NULL-pads.
            if (!emitted_any_) {
                emitted_any_ = true;
                co_return make_drain_chunk(guard_types_);
            }
            co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
        }

        emitted_any_ = true;
        co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(*batch));
    }

} // namespace components::operators

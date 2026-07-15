#include "operator_check_constraint.hpp"

#include "constraint_util.hpp"
#include "predicates/simple_predicate.hpp"
#include <components/cursor/cursor.hpp>
#include <components/types/logical_value.hpp>

#include <array>
#include <charconv>
#include <fast_float/fast_float.h>
#include <string>
#include <string_view>
#include <vector>

namespace components::operators {

    namespace {

        const vector::vector_t* find_col(const vector::data_chunk_t& chunk, std::string_view name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                if (chunk.data[c].type().alias() == name)
                    return &chunk.data[c];
            }
            return nullptr;
        }

        // The decoded DEFAULT value for `col`, or nullptr when the column has none.
        const types::logical_value_t*
        find_default(const std::vector<std::pair<std::string, types::logical_value_t>>* defaults,
                     std::string_view col) {
            if (defaults == nullptr) {
                return nullptr;
            }
            for (const auto& [name, value] : *defaults) {
                if (name == col) {
                    return &value;
                }
            }
            return nullptr;
        }

        // Parse a literal constant string into a logical_value_t without a type hint.
        types::logical_value_t parse_const(std::pmr::memory_resource* r, std::string_view s) {
            if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'')
                return types::logical_value_t(r, std::string(s.substr(1, s.size() - 2)));
            if (s.find('.') != std::string_view::npos) {
                double v{};
                auto [ptr, ec] = fast_float::from_chars(s.data(), s.data() + s.size(), v);
                if (ec == std::errc{})
                    return types::logical_value_t(r, v);
            }
            bool neg = !s.empty() && s[0] == '-';
            auto str = neg ? s.substr(1) : s;
            uint64_t u{};
            auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), u);
            int64_t v{};
            if (ec == std::errc{})
                v = neg ? -static_cast<int64_t>(u) : static_cast<int64_t>(u);
            return types::logical_value_t(r, v);
        }

        std::string_view trim(std::string_view s) {
            while (!s.empty() && s.front() == ' ') s.remove_prefix(1);
            while (!s.empty() && s.back() == ' ') s.remove_suffix(1);
            return s;
        }

        std::string_view strip_outer(std::string_view s) {
            s = trim(s);
            if (s.size() < 2 || s.front() != '(' || s.back() != ')')
                return s;
            int depth = 0;
            for (size_t i = 0; i < s.size(); ++i) {
                if (s[i] == '(')
                    ++depth;
                else if (s[i] == ')') {
                    --depth;
                    if (depth == 0 && i == s.size() - 1)
                        return s.substr(1, s.size() - 2);
                    if (depth == 0)
                        return s;
                }
            }
            return s;
        }

        // Forward declaration for recursion. `defaults` = the table's decoded DEFAULT
        // values: a column ABSENT from the write-set stores its default (filled
        // agent-side at storage_append), so the compiled predicates evaluate an
        // absent column AS that default; absent with no (non-NULL) default means the
        // stored value is NULL.
        // strict_absent: TRUE when the write-set is name-addressed (absence by alias
        // means the statement omitted the column, so it stores DEFAULT-or-NULL);
        // FALSE keeps the legacy absent-column pass-through.
        predicates::predicate_ptr
        build_check_predicate(std::pmr::memory_resource* r,
                              std::string_view expr,
                              const std::vector<std::pair<std::string, types::logical_value_t>>* defaults,
                              bool strict_absent);

        predicates::predicate_ptr
        build_check_predicate(std::pmr::memory_resource* r,
                              std::string_view expr,
                              const std::vector<std::pair<std::string, types::logical_value_t>>* defaults,
                              bool strict_absent) {
            using CT = expressions::compare_type;
            expr = trim(expr);

            if (expr.empty())
                return {new predicates::simple_predicate(
                    r,
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t)
                        -> core::result_wrapper_t<types::tri_bool_t> { return types::tri_bool_t::yes; })};

            // NOT (...)
            if (expr.size() > 5 && expr.substr(0, 5) == "NOT (") {
                std::pmr::vector<predicates::predicate_ptr> nested(r);
                nested.push_back(build_check_predicate(r, strip_outer(expr.substr(4)), defaults, strict_absent));
                return {new predicates::simple_predicate(r, std::move(nested), CT::union_not)};
            }

            // Paren-led: find matching ')' then check for AND/OR after.
            if (expr.front() == '(') {
                int depth = 0;
                size_t close = std::string_view::npos;
                for (size_t i = 0; i < expr.size(); ++i) {
                    if (expr[i] == '(')
                        ++depth;
                    else if (expr[i] == ')') {
                        --depth;
                        if (depth == 0) {
                            close = i;
                            break;
                        }
                    }
                }
                if (close != std::string_view::npos) {
                    std::string_view after = trim(expr.substr(close + 1));
                    if (after.size() >= 4 && after.substr(0, 4) == "AND ") {
                        std::pmr::vector<predicates::predicate_ptr> nested(r);
                        nested.push_back(build_check_predicate(r, expr.substr(1, close - 1), defaults, strict_absent));
                        nested.push_back(build_check_predicate(r, strip_outer(after.substr(4)), defaults, strict_absent));
                        return {new predicates::simple_predicate(r, std::move(nested), CT::union_and)};
                    }
                    if (after.size() >= 3 && after.substr(0, 3) == "OR ") {
                        std::pmr::vector<predicates::predicate_ptr> nested(r);
                        nested.push_back(build_check_predicate(r, expr.substr(1, close - 1), defaults, strict_absent));
                        nested.push_back(build_check_predicate(r, strip_outer(after.substr(3)), defaults, strict_absent));
                        return {new predicates::simple_predicate(r, std::move(nested), CT::union_or)};
                    }
                    if (close == expr.size() - 1)
                        return build_check_predicate(r, expr.substr(1, close - 1), defaults, strict_absent);
                }
            }

            // IS NOT NULL / IS NULL
            constexpr std::string_view kIsNotNull = " IS NOT NULL";
            constexpr std::string_view kIsNull = " IS NULL";
            if (expr.size() > kIsNotNull.size() && expr.substr(expr.size() - kIsNotNull.size()) == kIsNotNull) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNotNull.size())));
                // A column absent from the INSERT write-set stores the table DEFAULT
                // when one exists (filled agent-side at storage_append) — the STORED
                // row is then non-NULL and IS NOT NULL must PASS. Absent with no
                // (non-NULL) default really stores NULL, so it must FAIL — otherwise
                // `INSERT (a) VALUES (..)` would silently bypass `CHECK (b IS NOT
                // NULL)` for the omitted column b. Resolved once at compile time.
                const auto* def = find_default(defaults, col);
                const bool absent_is_valid = !strict_absent || (def != nullptr && !def->is_null());
                return {new predicates::simple_predicate(
                    r,
                    [col, absent_is_valid](const vector::data_chunk_t& chunk,
                                           const vector::data_chunk_t&,
                                           size_t idx,
                                           size_t) -> core::result_wrapper_t<types::tri_bool_t> {
                        const auto* v = find_col(chunk, col);
                        // IS NOT NULL is a total predicate: a definite TRUE / FALSE, never UNKNOWN.
                        return types::tri_of(v ? v->validity().row_is_valid(idx) : absent_is_valid);
                    })};
            }
            if (expr.size() > kIsNull.size() && expr.substr(expr.size() - kIsNull.size()) == kIsNull) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNull.size())));
                // Mirror of IS NOT NULL: an absent column stores its (non-NULL)
                // DEFAULT when one exists, so IS NULL fails; with no default the
                // stored value IS NULL.
                const auto* def = find_default(defaults, col);
                const bool absent_is_null = !strict_absent || def == nullptr || def->is_null();
                return {new predicates::simple_predicate(
                    r,
                    [col, absent_is_null](const vector::data_chunk_t& chunk,
                                          const vector::data_chunk_t&,
                                          size_t idx,
                                          size_t) -> core::result_wrapper_t<types::tri_bool_t> {
                        const auto* v = find_col(chunk, col);
                        return types::tri_of(v ? !v->validity().row_is_valid(idx) : absent_is_null);
                    })};
            }

            // Binary comparison: try operators longest-first to avoid ambiguous matches.
            constexpr std::array<std::string_view, 6> kOps{">=", "<=", "<>", ">", "<", "="};
            for (auto op : kOps) {
                std::string needle;
                needle.reserve(op.size() + 2);
                needle += ' ';
                needle += op;
                needle += ' ';
                auto pos = expr.find(needle);
                if (pos == std::string_view::npos)
                    continue;

                auto lhs = trim(expr.substr(0, pos));
                auto rhs = trim(expr.substr(pos + needle.size()));

                auto is_const = [](std::string_view s) {
                    return !s.empty() && (s.front() == '\'' || (s.front() >= '0' && s.front() <= '9') ||
                                          s.front() == '-' || s.front() == '.');
                };
                bool col_is_rhs = is_const(lhs);

                auto col_name = std::string(col_is_rhs ? rhs : lhs);
                auto const_val = parse_const(r, col_is_rhs ? lhs : rhs);
                auto op_str = std::string(op);
                // Absent-column policy, resolved once at compile time: with a non-NULL
                // DEFAULT the stored row carries that value — evaluate the comparison
                // against it; without one, keep the legacy pass (untyped/unknown shape).
                const auto* def = find_default(defaults, col_name);
                const bool has_def = strict_absent && def != nullptr && !def->is_null();
                auto def_val = has_def ? *def : const_val;

                return {new predicates::simple_predicate(
                    r,
                    [col_name, const_val, col_is_rhs, op_str, has_def, def_val](const vector::data_chunk_t& chunk,
                                                                                const vector::data_chunk_t&,
                                                                                size_t idx,
                                                                                size_t)
                        -> core::result_wrapper_t<types::tri_bool_t> {
                        // Raw three-valued result of the comparison: a NULL operand is UNKNOWN (not
                        // pre-folded to "passes"), so that a NOT wrapping it stays UNKNOWN rather
                        // than flipping to a violation. The consumer applies permits() -- a CHECK is
                        // violated only by a definitely-FALSE (tri_bool_t::no) result.
                        auto compare_tri = [&op_str](const types::logical_value_t& lhs,
                                                     const types::logical_value_t& rhs) -> types::tri_bool_t {
                            const auto c = lhs.compare_sql(rhs);
                            if (!c)
                                return types::tri_bool_t::unknown;
                            using Cmp = types::compare_t;
                            if (op_str == ">")
                                return types::tri_of(*c == Cmp::more);
                            if (op_str == "<")
                                return types::tri_of(*c == Cmp::less);
                            if (op_str == ">=")
                                return types::tri_of(*c == Cmp::more || *c == Cmp::equals);
                            if (op_str == "<=")
                                return types::tri_of(*c == Cmp::less || *c == Cmp::equals);
                            if (op_str == "=")
                                return types::tri_of(*c == Cmp::equals);
                            if (op_str == "<>")
                                return types::tri_of(*c != Cmp::equals);
                            return types::tri_bool_t::yes; // unreachable operator: do not reject
                        };
                        const auto* vec = find_col(chunk, col_name);
                        if (!vec) {
                            // Absent column with no default stores NULL -> UNKNOWN (permits passes).
                            if (!has_def)
                                return types::tri_bool_t::unknown;
                            return col_is_rhs ? compare_tri(const_val, def_val) : compare_tri(def_val, const_val);
                        }
                        // vec->value reports NA for a NULL row, so compare_sql yields UNKNOWN.
                        auto col_val = vec->value(idx);
                        return col_is_rhs ? compare_tri(const_val, col_val) : compare_tri(col_val, const_val);
                    })};
            }

            // Unrecognised expression — pass.
            return {new predicates::simple_predicate(
                r,
                [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t)
                    -> core::result_wrapper_t<types::tri_bool_t> { return types::tri_bool_t::yes; })};
        }

    } // anonymous namespace

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::vector<std::string> not_null_columns,
        std::vector<std::pair<std::string, std::string>> check_exprs,
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs,
        std::vector<std::pair<std::string, types::logical_value_t>> column_defaults,
        bool write_set_named)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns))
        , column_defaults_(std::move(column_defaults))
        , write_set_named_(write_set_named)
        , array_size_reqs_(std::move(array_size_reqs)) {
        check_predicates_.reserve(check_exprs.size());
        for (auto& [name, expr_str] : check_exprs) {
            check_predicates_.emplace_back(
                std::move(name),
                build_check_predicate(resource, expr_str, &column_defaults_, write_set_named_));
        }
    }

    actor_zeta::unique_future<void> operator_check_constraint_t::await_async_and_resume(pipeline::context_t* /*ctx*/) {
        // SYNCHRONOUS validation routed through the async-finalize drive so it runs
        // AFTER the DML child's await (which snapshots the written rows into
        // constraint_input()). No cross-actor await — completes immediately.
        validate_();
        if (has_error()) {
            co_return;
        }
        mark_executed();
        co_return;
    }

    void operator_check_constraint_t::validate_() {
        if (!left_)
            return;

        // The DML's constraint_input() snapshot of the just-written rows. Constraint
        // ops STACK above one DML (this check sits OUTERMOST, above the per-FK
        // fk_check ops), so the immediate left_ is often an (empty) fk_check — walk
        // DOWN the left_ spine to the DML's snapshot (single canonical source, R6;
        // see constraint_util.hpp). An empty snapshot means an empty write-set.
        operator_data_ptr data_src = constraint_detail::resolve_constraint_source(left_);

        // check_constraint is the plan ROOT, so output_ becomes the result cursor:
        // surface the DML child's final result (RETURNING / affected-count chunk).
        output_ = left_->output();

        if (!data_src || data_src->size() == 0)
            return;

        // Constraints are validated independently on every chunk of the input.
        for (const auto& chunk : data_src->chunks()) {
            if (chunk.size() == 0) {
                continue;
            }

            // NOT NULL checks. A column ABSENT from the write-set stores the table
            // DEFAULT when one exists (filled agent-side); with no non-NULL default
            // the stored value IS NULL — a violation (e.g. an INSERT omitting a
            // PRIMARY KEY column, which pg_attribute never marks attnotnull).
            for (const auto& col_name : not_null_columns_) {
                bool found = false;
                for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                    if (chunk.data[col].type().alias() != col_name)
                        continue;
                    found = true;
                    for (uint64_t row = 0; row < chunk.size(); ++row) {
                        if (!chunk.data[col].validity().row_is_valid(row)) {
                            set_error(core::error_t{
                                core::error_code_t::other_error,
                                std::pmr::string{"NOT NULL constraint violated for column: " + col_name, resource_}});
                            return;
                        }
                    }
                    break;
                }
                if (!found && write_set_named_) {
                    const auto* def = find_default(&column_defaults_, col_name);
                    if (def == nullptr || def->is_null()) {
                        set_error(core::error_t{
                            core::error_code_t::other_error,
                            std::pmr::string{"NOT NULL constraint violated for column: " + col_name, resource_}});
                        return;
                    }
                }
            }

            // Fixed-ARRAY length checks. A NOT NULL fixed ARRAY column with no DEFAULT cannot
            // pad a value shorter than its declared size (there is nothing to fill the missing
            // slots with), so such a value must be rejected here rather than silently dropped
            // at the append. Validated per column: a single short element fails the operation.
            for (const auto& [col_name, required_size] : array_size_reqs_) {
                for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                    if (chunk.data[col].type().alias() != col_name)
                        continue;
                    for (uint64_t row = 0; row < chunk.size(); ++row) {
                        if (!chunk.data[col].validity().row_is_valid(row))
                            continue; // NULL handled by the NOT NULL check above
                        if (chunk.data[col].value(row).children().size() < required_size) {
                            set_error(core::error_t{
                                core::error_code_t::other_error,
                                std::pmr::string{"value for NOT NULL array column '" + col_name + "' has fewer than " +
                                                     std::to_string(required_size) +
                                                     " elements and the column has no default to pad from",
                                                 resource_}});
                            return;
                        }
                    }
                    break;
                }
            }

            // CHECK expression evaluation.
            for (const auto& [name, pred] : check_predicates_) {
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    auto check_result = pred->check(chunk, row);
                    // A CHECK is violated only by a definitely-FALSE result; UNKNOWN (a NULL operand,
                    // even under NOT) permits the row -- SQL rejects only definitely-FALSE checks.
                    if (check_result.has_error() || !types::permits(check_result.value())) {
                        set_error(
                            core::error_t{core::error_code_t::other_error,
                                          std::pmr::string{"CHECK constraint \"" + name + "\" violated", resource_}});
                        return;
                    }
                }
            }
        }
    }

} // namespace components::operators
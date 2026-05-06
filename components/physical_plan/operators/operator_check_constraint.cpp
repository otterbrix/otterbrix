#include "operator_check_constraint.hpp"

#include <components/cursor/cursor.hpp>
#include <components/types/logical_value.hpp>

#include <array>
#include <charconv>
#include <string>
#include <string_view>

namespace components::operators {

    namespace {

        // Attempt to get a column vector by name from the chunk.
        const vector::vector_t* find_col(const vector::data_chunk_t& chunk, std::string_view name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                if (chunk.data[c].type().alias() == name) return &chunk.data[c];
            }
            return nullptr;
        }

        // Create a logical_value matching the column's type from the constant string.
        // Falls back to parsing as int64.
        types::logical_value_t parse_const(std::pmr::memory_resource* r,
                                            std::string_view           s,
                                            const types::complex_logical_type& col_type) {
            using LT = types::logical_type;
            LT lt = col_type.type();
            // String constant: 'text'
            if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'') {
                return types::logical_value_t(r, std::string(s.substr(1, s.size() - 2)));
            }
            // Numeric: try double first (has '.'), then int64
            if (lt == LT::DOUBLE || lt == LT::FLOAT ||
                s.find('.') != std::string_view::npos) {
                double v{};
                auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
                if (ec == std::errc{}) return types::logical_value_t(r, v);
            }
            int64_t v{};
            // Handle negative numbers (from_chars doesn't parse '-' for unsigned)
            bool neg = !s.empty() && s[0] == '-';
            auto str = neg ? s.substr(1) : s;
            uint64_t u{};
            auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), u);
            if (ec == std::errc{}) v = neg ? -static_cast<int64_t>(u) : static_cast<int64_t>(u);
            return types::logical_value_t(r, v);
        }

        // Forward declaration.
        bool eval_check(std::pmr::memory_resource* r,
                         std::string_view           expr,
                         const vector::data_chunk_t& chunk,
                         uint64_t                   row);

        // Strip leading/trailing whitespace.
        std::string_view trim(std::string_view s) {
            while (!s.empty() && s.front() == ' ') s.remove_prefix(1);
            while (!s.empty() && s.back()  == ' ') s.remove_suffix(1);
            return s;
        }

        // Strip one layer of outer parens if the whole string is wrapped.
        std::string_view strip_outer(std::string_view s) {
            s = trim(s);
            if (s.size() < 2 || s.front() != '(' || s.back() != ')') return s;
            int depth = 0;
            for (size_t i = 0; i < s.size(); ++i) {
                if (s[i] == '(') ++depth;
                else if (s[i] == ')') {
                    --depth;
                    if (depth == 0 && i == s.size() - 1) return s.substr(1, s.size() - 2);
                    if (depth == 0) return s; // ) closes before the end → not outer parens
                }
            }
            return s;
        }

        bool eval_check(std::pmr::memory_resource* r,
                         std::string_view           expr,
                         const vector::data_chunk_t& chunk,
                         uint64_t                   row) {
            expr = trim(expr);
            if (expr.empty()) return true;

            // NOT (...)
            if (expr.size() > 5 && expr.substr(0, 5) == "NOT (") {
                auto inner = strip_outer(expr.substr(4)); // keep parens for strip_outer
                return !eval_check(r, inner, chunk, row);
            }

            // If starts with '(' find the matching ')' and check for AND/OR after it.
            if (expr.front() == '(') {
                int depth = 0;
                size_t close = std::string_view::npos;
                for (size_t i = 0; i < expr.size(); ++i) {
                    if (expr[i] == '(') ++depth;
                    else if (expr[i] == ')') {
                        --depth;
                        if (depth == 0) { close = i; break; }
                    }
                }
                if (close != std::string_view::npos) {
                    std::string_view after = trim(expr.substr(close + 1));
                    if (after.substr(0, 4) == "AND ") {
                        auto left  = expr.substr(1, close - 1);
                        auto right = strip_outer(after.substr(4));
                        return eval_check(r, left, chunk, row) && eval_check(r, right, chunk, row);
                    }
                    if (after.substr(0, 3) == "OR ") {
                        auto left  = expr.substr(1, close - 1);
                        auto right = strip_outer(after.substr(3));
                        return eval_check(r, left, chunk, row) || eval_check(r, right, chunk, row);
                    }
                    if (close == expr.size() - 1) {
                        // Entire expression is parenthesised
                        return eval_check(r, expr.substr(1, close - 1), chunk, row);
                    }
                }
            }

            // IS NOT NULL / IS NULL suffix tests.
            constexpr std::string_view kIsNotNull = " IS NOT NULL";
            constexpr std::string_view kIsNull    = " IS NULL";
            if (expr.size() > kIsNotNull.size() &&
                expr.substr(expr.size() - kIsNotNull.size()) == kIsNotNull) {
                auto col = trim(expr.substr(0, expr.size() - kIsNotNull.size()));
                const auto* v = find_col(chunk, col);
                return v ? v->validity().row_is_valid(row) : true;
            }
            if (expr.size() > kIsNull.size() &&
                expr.substr(expr.size() - kIsNull.size()) == kIsNull) {
                auto col = trim(expr.substr(0, expr.size() - kIsNull.size()));
                const auto* v = find_col(chunk, col);
                return v ? !v->validity().row_is_valid(row) : true;
            }

            // Binary comparison: col op const  or  const op col.
            // Try multi-char operators first to avoid prefix ambiguity.
            constexpr std::array<std::string_view, 6> kOps{">=", "<=", "<>", ">", "<", "="};
            for (auto op : kOps) {
                // Look for " op " pattern.
                std::string needle;
                needle.reserve(op.size() + 2);
                needle += ' ';
                needle += op;
                needle += ' ';
                auto pos = expr.find(needle);
                if (pos == std::string_view::npos) continue;

                auto lhs = trim(expr.substr(0, pos));
                auto rhs = trim(expr.substr(pos + needle.size()));

                // Identify which side is the column.
                const vector::vector_t* col_vec = find_col(chunk, lhs);
                bool col_is_rhs = false;
                if (!col_vec) {
                    col_vec = find_col(chunk, rhs);
                    col_is_rhs = true;
                }
                if (!col_vec) return true; // unknown column → pass

                if (!col_vec->validity().row_is_valid(row)) return false; // NULL fails any comparison

                auto col_val   = col_vec->value(row);
                auto const_str = col_is_rhs ? lhs : rhs;
                auto const_val = parse_const(r, const_str, col_val.type());

                // cmp = col_val compared to const_val (or reversed if col is on rhs).
                types::compare_t cmp = col_is_rhs ? const_val.compare(col_val)
                                                   : col_val.compare(const_val);
                using CT = types::compare_t;
                if (op == ">")  return cmp == CT::more;
                if (op == "<")  return cmp == CT::less;
                if (op == ">=") return cmp == CT::more || cmp == CT::equals;
                if (op == "<=") return cmp == CT::less || cmp == CT::equals;
                if (op == "=")  return cmp == CT::equals;
                if (op == "<>") return cmp != CT::equals;
            }

            return true; // unrecognised expression → pass
        }

    } // anonymous namespace

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource*                        resource,
        log_t                                              log,
        std::vector<std::string>                           not_null_columns,
        std::vector<std::pair<std::string, std::string>>   check_exprs)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns))
        , check_exprs_(std::move(check_exprs)) {}

    void operator_check_constraint_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_) return;

        // After intercept_dml_io_, the DML operator's output is replaced with a
        // zero-column result chunk. The original typed data lives in the DML
        // operator's child (the raw data source), whose output is never replaced.
        // Prefer the child's output if the direct output has lost its columns.
        operator_data_ptr data_src = left_->output();
        if (!data_src || data_src->data_chunk().column_count() == 0) {
            if (left_->left() && left_->left()->output()) {
                data_src = left_->left()->output();
            }
        }

        output_ = left_->output(); // propagate the DML result chunk

        if (!data_src || data_src->data_chunk().size() == 0) return;
        const auto& chunk = data_src->data_chunk();

        // NOT NULL checks.
        for (const auto& col_name : not_null_columns_) {
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                if (chunk.data[col].type().alias() != col_name) continue;
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (!chunk.data[col].validity().row_is_valid(row)) {
                        set_error("NOT NULL constraint violated for column: " + col_name);
                        return;
                    }
                }
                break;
            }
        }

        // CHECK expression evaluation.
        for (const auto& [name, expr] : check_exprs_) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                if (!eval_check(resource_, expr, chunk, row)) {
                    set_error("CHECK constraint \"" + name + "\" violated: " + expr);
                    return;
                }
            }
        }
    }

} // namespace components::operators
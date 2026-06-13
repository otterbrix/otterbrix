#include "operator_check_constraint.hpp"

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

        // Forward declaration for recursion.
        predicates::predicate_ptr build_check_predicate(std::pmr::memory_resource* r, std::string_view expr);

        predicates::predicate_ptr build_check_predicate(std::pmr::memory_resource* r, std::string_view expr) {
            using CT = expressions::compare_type;
            expr = trim(expr);

            if (expr.empty())
                return {new predicates::simple_predicate(
                    r,
                    [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return true; })};

            // NOT (...)
            if (expr.size() > 5 && expr.substr(0, 5) == "NOT (") {
                std::pmr::vector<predicates::predicate_ptr> nested(r);
                nested.push_back(build_check_predicate(r, strip_outer(expr.substr(4))));
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
                        nested.push_back(build_check_predicate(r, expr.substr(1, close - 1)));
                        nested.push_back(build_check_predicate(r, strip_outer(after.substr(4))));
                        return {new predicates::simple_predicate(r, std::move(nested), CT::union_and)};
                    }
                    if (after.size() >= 3 && after.substr(0, 3) == "OR ") {
                        std::pmr::vector<predicates::predicate_ptr> nested(r);
                        nested.push_back(build_check_predicate(r, expr.substr(1, close - 1)));
                        nested.push_back(build_check_predicate(r, strip_outer(after.substr(3))));
                        return {new predicates::simple_predicate(r, std::move(nested), CT::union_or)};
                    }
                    if (close == expr.size() - 1)
                        return build_check_predicate(r, expr.substr(1, close - 1));
                }
            }

            // IS NOT NULL / IS NULL
            constexpr std::string_view kIsNotNull = " IS NOT NULL";
            constexpr std::string_view kIsNull = " IS NULL";
            if (expr.size() > kIsNotNull.size() && expr.substr(expr.size() - kIsNotNull.size()) == kIsNotNull) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNotNull.size())));
                return {new predicates::simple_predicate(
                    r,
                    [col](const vector::data_chunk_t& chunk, const vector::data_chunk_t&, size_t idx, size_t) -> bool {
                        const auto* v = find_col(chunk, col);
                        return v ? v->validity().row_is_valid(idx) : true;
                    })};
            }
            if (expr.size() > kIsNull.size() && expr.substr(expr.size() - kIsNull.size()) == kIsNull) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNull.size())));
                return {new predicates::simple_predicate(
                    r,
                    [col](const vector::data_chunk_t& chunk, const vector::data_chunk_t&, size_t idx, size_t) -> bool {
                        const auto* v = find_col(chunk, col);
                        return v ? !v->validity().row_is_valid(idx) : true;
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

                return {new predicates::simple_predicate(
                    r,
                    [col_name, const_val, col_is_rhs, op_str](const vector::data_chunk_t& chunk,
                                                              const vector::data_chunk_t&,
                                                              size_t idx,
                                                              size_t) -> bool {
                        const auto* vec = find_col(chunk, col_name);
                        if (!vec)
                            return true;
                        if (!vec->validity().row_is_valid(idx))
                            return false;
                        auto col_val = vec->value(idx);
                        using Cmp = types::compare_t;
                        auto cmp = col_is_rhs ? const_val.compare(col_val) : col_val.compare(const_val);
                        if (op_str == ">")
                            return cmp == Cmp::more;
                        if (op_str == "<")
                            return cmp == Cmp::less;
                        if (op_str == ">=")
                            return cmp == Cmp::more || cmp == Cmp::equals;
                        if (op_str == "<=")
                            return cmp == Cmp::less || cmp == Cmp::equals;
                        if (op_str == "=")
                            return cmp == Cmp::equals;
                        if (op_str == "<>")
                            return cmp != Cmp::equals;
                        return true;
                    })};
            }

            // Unrecognised expression — pass.
            return {new predicates::simple_predicate(
                r,
                [](const vector::data_chunk_t&, const vector::data_chunk_t&, size_t, size_t) { return true; })};
        }

    } // anonymous namespace

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::vector<std::string> not_null_columns,
        std::vector<std::pair<std::string, std::string>> check_exprs)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns)) {
        check_predicates_.reserve(check_exprs.size());
        for (auto& [name, expr_str] : check_exprs) {
            check_predicates_.emplace_back(std::move(name), build_check_predicate(resource, expr_str));
        }
    }

    void operator_check_constraint_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_)
            return;

        operator_data_ptr data_src = left_->output();
        if (!data_src || data_src->data_chunk().column_count() == 0) {
            if (left_->left() && left_->left()->output()) {
                data_src = left_->left()->output();
            }
        }

        output_ = left_->output();

        if (!data_src || data_src->data_chunk().size() == 0)
            return;
        const auto& chunk = data_src->data_chunk();

        // NOT NULL checks.
        for (const auto& col_name : not_null_columns_) {
            for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                if (chunk.data[col].type().alias() != col_name)
                    continue;
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
        }

        // CHECK expression evaluation.
        for (const auto& [name, pred] : check_predicates_) {
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                auto check_result = pred->check(chunk, row);
                if (check_result.has_error() || !check_result.value()) {
                    set_error(core::error_t{core::error_code_t::other_error,
                                            std::pmr::string{"CHECK constraint \"" + name + "\" violated", resource_}});
                    return;
                }
            }
        }
    }

} // namespace components::operators
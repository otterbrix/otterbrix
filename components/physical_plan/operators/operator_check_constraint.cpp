#include "operator_check_constraint.hpp"

#include "constraint_util.hpp"
#include <components/cursor/cursor.hpp>
#include <components/types/logical_value.hpp>

#include <array>
#include <charconv>
#include <fast_float/fast_float.h>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace components::operators {

    namespace {

        std::optional<size_t> find_col_index(const vector::data_chunk_t& chunk, std::string_view name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                if (chunk.data[c].type().alias() == name) {
                    return c;
                }
            }
            return std::nullopt;
        }

        // NOTHING HERE BUILDS A CONSTANT PREDICATE. A CHECK over a name the table does not
        // have is a constraint enforced by nothing, so those arms refuse instead of
        // compiling to the constant TRUE.

        expressions::param_storage column_operand(std::pmr::memory_resource* r, size_t ordinal) {
            expressions::key_t key(r);
            key.set_path(std::pmr::vector<size_t>{{ordinal}, r});
            return expressions::param_storage{key};
        }

        expressions::param_storage constant_operand(const types::logical_value_t& value,
                                                    const types::complex_logical_type& target,
                                                    core::date::timezone_offset_t session_tz,
                                                    types::parameter_map_t* params,
                                                    uint64_t* next_id,
                                                    bool* convertible) {
            types::logical_value_t bound = value;
            if (value.type() != target) {
                auto converted = value.cast_as(target, session_tz);
                if (converted.has_error()) {
                    *convertible = false;
                    return expressions::param_storage{core::parameter_id_t{0}};
                }
                bound = std::move(converted.value());
            }
            const core::parameter_id_t id{static_cast<uint16_t>((*next_id)++)};
            params->insert_or_assign(id, bound);
            return expressions::param_storage{id};
        }

        // NO DEFAULTS ARE CONSULTED HERE. Absent columns do not reach this operator — the
        // insert expands them — and deciding an absent column's fate from the plan's own
        // copy of its DEFAULT is precisely how a CHECK comes to admit a row whose stored
        // value it never saw.

        // Parse a literal constant string into a logical_value_t without a type hint.
        // The WHOLE text must be one literal: a partial parse answering with what it
        // managed to read turns `a > 1 + 1` into `a > 1` and `lo <= hi` into `lo <= 0` —
        // a different constraint than the declared one, enforced silently. Nothing
        // consumable => nullopt, and the caller refuses.
        std::optional<types::logical_value_t> parse_const(std::pmr::memory_resource* r, std::string_view s) {
            if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'')
                return types::logical_value_t(r, std::string(s.substr(1, s.size() - 2)));
            if (s.find('.') != std::string_view::npos) {
                double v{};
                auto [ptr, ec] = fast_float::from_chars(s.data(), s.data() + s.size(), v);
                if (ec == std::errc{} && ptr == s.data() + s.size())
                    return types::logical_value_t(r, v);
                return std::nullopt;
            }
            bool neg = !s.empty() && s[0] == '-';
            auto str = neg ? s.substr(1) : s;
            if (str.empty())
                return std::nullopt;
            uint64_t u{};
            auto [ptr, ec] = std::from_chars(str.data(), str.data() + str.size(), u);
            if (ec != std::errc{} || ptr != str.data() + str.size())
                return std::nullopt;
            return types::logical_value_t(r, neg ? -static_cast<int64_t>(u) : static_cast<int64_t>(u));
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

        // Find `needle` in `expr` at TOP level: not inside a single-quoted string
        // constant and not nested in parentheses. Scanning without that distinction is
        // how `name = 'a > b'` came to be read as a comparison on " > ": the operator
        // INSIDE the literal was taken for the predicate's own, the left side
        // ("name = 'a") matched no column, and the whole CHECK compiled to TRUE.
        size_t find_top_level(std::string_view expr, std::string_view needle) {
            int depth = 0;
            bool in_quotes = false;
            for (size_t i = 0; i < expr.size(); ++i) {
                const char c = expr[i];
                if (c == '\'') {
                    in_quotes = !in_quotes;
                    continue;
                }
                if (in_quotes) {
                    continue;
                }
                if (c == '(') {
                    ++depth;
                    continue;
                }
                if (c == ')') {
                    if (depth > 0) {
                        --depth;
                    }
                    continue;
                }
                if (depth == 0 && expr.compare(i, needle.size(), needle) == 0) {
                    return i;
                }
            }
            return std::string_view::npos;
        }

        struct check_build_context {
            const vector::data_chunk_t* chunk;
            core::date::timezone_offset_t session_tz;
            types::parameter_map_t* params;
            uint64_t next_id;
            // Names the constraint in every refusal below.
            std::string_view constraint_name;
        };

        // A CHECK whose text this recogniser cannot read is a constraint that would be
        // enforced by NOTHING, so it is refused rather than compiled to the constant TRUE.
        // The declaration path (deparse_check_expr) refuses these shapes too; this is the
        // last line of defence for a text that reached the catalog by another route.
        core::error_t unevaluable(std::pmr::memory_resource* r,
                                  const check_build_context& ctx,
                                  std::string_view expr,
                                  const std::string& why) {
            std::string what = "CHECK constraint \"";
            what.append(ctx.constraint_name);
            what += "\" cannot be evaluated: ";
            what += why;
            what += " in \"";
            what.append(expr);
            what += "\"";
            return core::error_t{core::error_code_t::invalid_constraint, std::pmr::string{std::move(what), r}};
        }

        core::result_wrapper_t<expressions::expression_ptr>
        build_check_expression(std::pmr::memory_resource* r, std::string_view expr, check_build_context& ctx);

        core::result_wrapper_t<expressions::expression_ptr>
        build_check_expression(std::pmr::memory_resource* r, std::string_view expr, check_build_context& ctx) {
            using CT = expressions::compare_type;
            expr = trim(expr);

            if (expr.empty())
                return unevaluable(r, ctx, expr, "the expression is empty");

            // NOT (...)
            if (expr.size() > 5 && expr.substr(0, 5) == "NOT (") {
                auto inner = build_check_expression(r, strip_outer(expr.substr(4)), ctx);
                if (inner.has_error()) {
                    return inner;
                }
                auto combined = expressions::make_compare_union_expression(r, CT::union_not);
                combined->append_child(std::move(inner.value()));
                return expressions::expression_ptr{std::move(combined)};
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
                    const bool is_and = after.size() >= 4 && after.substr(0, 4) == "AND ";
                    const bool is_or = after.size() >= 3 && after.substr(0, 3) == "OR ";
                    if (is_and || is_or) {
                        auto left = build_check_expression(r, expr.substr(1, close - 1), ctx);
                        if (left.has_error()) {
                            return left;
                        }
                        auto right = build_check_expression(r, strip_outer(after.substr(is_and ? 4 : 3)), ctx);
                        if (right.has_error()) {
                            return right;
                        }
                        auto combined =
                            expressions::make_compare_union_expression(r, is_and ? CT::union_and : CT::union_or);
                        combined->append_child(std::move(left.value()));
                        combined->append_child(std::move(right.value()));
                        return expressions::expression_ptr{std::move(combined)};
                    }
                    if (close == expr.size() - 1)
                        return build_check_expression(r, expr.substr(1, close - 1), ctx);
                }
            }

            // IS NOT NULL / IS NULL. Matched at TOP level so a string constant that ENDS
            // in " IS NULL" is not mistaken for the test itself.
            constexpr std::string_view kIsNotNull = " IS NOT NULL";
            constexpr std::string_view kIsNull = " IS NULL";
            if (expr.size() > kIsNotNull.size() &&
                find_top_level(expr, kIsNotNull) == expr.size() - kIsNotNull.size()) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNotNull.size())));
                // Every table column is IN the row by the time this runs: an INSERT that omitted one had it
                // expanded (to its DEFAULT, or NULL) before the append, and CHECK is refused on dynamic-schema
                // tables at DDL — so a name that is still not here names NO column of this table. Compiling it to
                // the constant TRUE is this operator's SUCCESS path: the declared constraint would judge nothing,
                // silently. The declaration path writes the mentioned names onto the node and the DDL guard refuses
                // the typo; this is the last line of defence for a constraint that reached the catalog otherwise.
                auto ordinal = find_col_index(*ctx.chunk, col);
                if (!ordinal.has_value()) {
                    return unevaluable(r, ctx, expr, "the column \"" + col + "\" does not exist in the written row");
                }
                // IS NOT NULL is not an operator of its own — it is a negated is_null, which is
                // exactly how the graph builder wants to see it.
                auto negated = expressions::make_compare_union_expression(r, CT::union_not);
                negated->append_child(expressions::make_compare_expression(r,
                                                                           CT::is_null,
                                                                           column_operand(r, *ordinal),
                                                                           expressions::param_storage{}));
                return expressions::expression_ptr{std::move(negated)};
            }
            if (expr.size() > kIsNull.size() && find_top_level(expr, kIsNull) == expr.size() - kIsNull.size()) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNull.size())));
                // Mirror of IS NOT NULL: the column is in the row, so read it — and a
                // name that is not refuses for the same reason as that arm.
                auto ordinal = find_col_index(*ctx.chunk, col);
                if (!ordinal.has_value()) {
                    return unevaluable(r, ctx, expr, "the column \"" + col + "\" does not exist in the written row");
                }
                return expressions::expression_ptr{expressions::make_compare_expression(r,
                                                                                        CT::is_null,
                                                                                        column_operand(r, *ordinal),
                                                                                        expressions::param_storage{})};
            }

            // Binary comparison: try operators longest-first to avoid ambiguous matches.
            constexpr std::array<std::string_view, 6> kOps{">=", "<=", "<>", ">", "<", "="};
            for (auto op : kOps) {
                std::string needle;
                needle.reserve(op.size() + 2);
                needle += ' ';
                needle += op;
                needle += ' ';
                auto pos = find_top_level(expr, needle);
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
                const auto const_text = col_is_rhs ? lhs : rhs;
                auto const_val = parse_const(r, const_text);
                if (!const_val.has_value()) {
                    // Not a literal at all: another column (`lo <= hi`), or arithmetic
                    // (`a > 1 + 1`). Read as the number this parser can scavenge from the
                    // text — 0 and 1 respectively — the row would be judged against a bound
                    // nobody wrote.
                    return unevaluable(r, ctx, expr, std::string{"\""} + std::string{const_text} +
                                                         "\" is not a constant");
                }

                auto ordinal = find_col_index(*ctx.chunk, col_name);
                if (!ordinal.has_value()) {
                    // Not a column of this table at all (see the IS NOT NULL arm): a CHECK
                    // over it compares nothing, and compiling it to TRUE would enforce the
                    // declared constraint with nothing.
                    return unevaluable(r, ctx, expr,
                                       "the column \"" + col_name + "\" does not exist in the written row");
                }

                const auto compare_type_of = [&op]() {
                    if (op == ">")
                        return CT::gt;
                    if (op == "<")
                        return CT::lt;
                    if (op == ">=")
                        return CT::gte;
                    if (op == "<=")
                        return CT::lte;
                    if (op == "=")
                        return CT::eq;
                    return CT::ne;
                };

                bool convertible = true;
                auto literal = constant_operand(*const_val,
                                                ctx.chunk->data[*ordinal].type(),
                                                ctx.session_tz,
                                                ctx.params,
                                                &ctx.next_id,
                                                &convertible);
                if (!convertible) {
                    // The literal does not fit the column's type, so there is no comparison
                    // to make. Passing the row is the same silence as the unrecognised case:
                    // the constraint the user declared would judge nothing.
                    return unevaluable(r,
                                       ctx,
                                       expr,
                                       std::string{"\""} + std::string{const_text} +
                                           "\" does not convert to the type of column \"" + col_name + "\"");
                }
                auto column = column_operand(r, *ordinal);
                return expressions::expression_ptr{
                    col_is_rhs ? expressions::make_compare_expression(r, compare_type_of(), literal, column)
                               : expressions::make_compare_expression(r, compare_type_of(), column, literal)};
            }

            // Unrecognised expression. Compiling it to the constant TRUE is how a declared
            // constraint comes to be enforced by nothing at all.
            return unevaluable(r, ctx, expr, "the expression is not a comparison this engine can evaluate");
        }

    } // anonymous namespace

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::vector<std::string> not_null_columns,
        std::vector<std::pair<std::string, std::string>> check_exprs,
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns))
        , array_size_reqs_(std::move(array_size_reqs))
        , check_exprs_(std::move(check_exprs)) {}

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

        // Compile every check once
        std::pmr::vector<compiled_check_t> checks(resource_);
        const vector::data_chunk_t* schema_chunk = nullptr;
        for (const auto& chunk : data_src->chunks()) {
            if (chunk.size() > 0) {
                schema_chunk = &chunk;
                break;
            }
        }
        const components::graph_execution_context graph_context{};
        if (schema_chunk != nullptr && !check_exprs_.empty()) {
            check_params_.clear();
            check_build_context build_ctx{schema_chunk,
                                          graph_context.timezone_offset,
                                          &check_params_,
                                          0,
                                          std::string_view{}};
            checks.reserve(check_exprs_.size());
            for (const auto& entry : check_exprs_) {
                compiled_check_t compiled;
                build_ctx.constraint_name = entry.first;
                auto tree_r = build_check_expression(resource_, entry.second, build_ctx);
                if (tree_r.has_error()) {
                    // A constraint the engine cannot read is not a constraint that permits
                    // everything: it fails the statement that would have been judged by it.
                    set_error(tree_r.error());
                    return;
                }
                auto tree = std::move(tree_r.value());
                compiled.condition = expressions::classify_condition(tree);
                if (compiled.condition == expressions::condition_kind::computed) {
                    auto types = schema_chunk->types();
                    auto built = expressions::build_condition_graph(resource_, check_params_, tree.get(), types);
                    if (built.has_error()) {
                        set_error(built.error());
                        return;
                    }
                    compiled.graph = std::move(built.value());
                }
                checks.push_back(std::move(compiled));
            }
        }

        // Constraints are validated independently on every chunk of the input.
        for (const auto& chunk : data_src->chunks()) {
            if (chunk.size() == 0) {
                continue;
            }

            // NOT NULL checks over the MATERIALISED row. An INSERT that omitted the column had it expanded
            // before the append — to its DEFAULT, or to NULL when there is none — so the validity bit answers
            // the question directly, for the value that was actually stored. (An INSERT omitting a PRIMARY KEY
            // column, which pg_attribute never marks attnotnull, therefore fails here on the NULL written.)
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
                if (!found) {
                    // Not a column of this write-set at all (a dynamic-schema table, or a
                    // path that hands storage ready-made rows). Nothing materialised to judge.
                    continue;
                }
            }

            // Fixed-ARRAY element checks. A value shorter than the column's declared size is reconciled to it by
            // padding NULL (casts::array_cast), and this validates the rows the DML has ALREADY written — so the
            // pad has happened and the short value is no longer short. What survives it is a NULL element, which
            // a NOT NULL column cannot hold, so that is what is tested. The length is still compared for a
            // write-set that reaches here unreconciled. Validated per column: one bad element fails the operation.
            for (const auto& [col_name, required_size] : array_size_reqs_) {
                for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                    if (chunk.data[col].type().alias() != col_name)
                        continue;
                    for (uint64_t row = 0; row < chunk.size(); ++row) {
                        if (!chunk.data[col].validity().row_is_valid(row))
                            continue; // NULL handled by the NOT NULL check above
                        const auto elements = chunk.data[col].value(row).children();
                        bool has_null_element = elements.size() < required_size;
                        for (const auto& element : elements) {
                            if (element.is_null()) {
                                has_null_element = true;
                                break;
                            }
                        }
                        if (has_null_element) {
                            set_error(
                                core::error_t{core::error_code_t::other_error,
                                              std::pmr::string{"NOT NULL array column '" + col_name + "' requires " +
                                                                   std::to_string(required_size) + " non-null elements",
                                                               resource_}});
                            return;
                        }
                    }
                    break;
                }
            }

            // CHECK expression evaluation
            for (size_t index = 0; index < checks.size(); ++index) {
                const auto& compiled = checks[index];
                if (compiled.condition == expressions::condition_kind::always) {
                    continue;
                }
                const vector::vector_t* decisions = nullptr;
                std::optional<vector::data_chunk_t> produced;
                if (compiled.graph) {
                    auto decided = expressions::run_graph(compiled.graph.get(), check_params_, chunk, graph_context);
                    if (decided.has_error()) {
                        set_error(decided.error());
                        return;
                    }
                    produced = std::move(decided.value());
                    decisions = &produced->data.front();
                }
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    // A CHECK is violated only by a definitely-FALSE result. UNKNOWN — a NULL
                    // operand, even under NOT — permits the row: SQL rejects only definitely-FALSE
                    // checks, unlike WHERE, which drops everything that is not definitely TRUE.
                    const bool passed =
                        decisions != nullptr && (decisions->is_null(row) || decisions->get_value<bool>(row));
                    if (!passed) {
                        set_error(core::error_t{
                            core::error_code_t::other_error,
                            std::pmr::string{"CHECK constraint \"" + check_exprs_[index].first + "\" violated",
                                             resource_}});
                        return;
                    }
                }
            }
        }
    }

} // namespace components::operators
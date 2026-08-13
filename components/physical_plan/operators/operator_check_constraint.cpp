#include "operator_check_constraint.hpp"

#include "constraint_util.hpp"
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

        std::optional<size_t> find_col_index(const vector::data_chunk_t& chunk, std::string_view name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                if (chunk.data[c].type().alias() == name) {
                    return c;
                }
            }
            return std::nullopt;
        }

        expressions::compare_expression_ptr constant_leaf(std::pmr::memory_resource* r, bool value) {
            using CT = expressions::compare_type;
            return expressions::make_compare_expression(r, value ? CT::all_true : CT::all_false);
        }

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

        struct check_build_context {
            const std::vector<std::pair<std::string, types::logical_value_t>>* defaults;
            bool strict_absent;
            const vector::data_chunk_t* chunk;
            core::date::timezone_offset_t session_tz;
            types::parameter_map_t* params;
            uint64_t next_id;
        };

        expressions::expression_ptr
        build_check_expression(std::pmr::memory_resource* r, std::string_view expr, check_build_context& ctx);

        expressions::expression_ptr
        build_check_expression(std::pmr::memory_resource* r, std::string_view expr, check_build_context& ctx) {
            using CT = expressions::compare_type;
            expr = trim(expr);

            if (expr.empty())
                return constant_leaf(r, true);

            // NOT (...)
            if (expr.size() > 5 && expr.substr(0, 5) == "NOT (") {
                auto combined = expressions::make_compare_union_expression(r, CT::union_not);
                combined->append_child(build_check_expression(r, strip_outer(expr.substr(4)), ctx));
                return combined;
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
                        auto combined = expressions::make_compare_union_expression(r, CT::union_and);
                        combined->append_child(build_check_expression(r, expr.substr(1, close - 1), ctx));
                        combined->append_child(build_check_expression(r, strip_outer(after.substr(4)), ctx));
                        return combined;
                    }
                    if (after.size() >= 3 && after.substr(0, 3) == "OR ") {
                        auto combined = expressions::make_compare_union_expression(r, CT::union_or);
                        combined->append_child(build_check_expression(r, expr.substr(1, close - 1), ctx));
                        combined->append_child(build_check_expression(r, strip_outer(after.substr(3)), ctx));
                        return combined;
                    }
                    if (close == expr.size() - 1)
                        return build_check_expression(r, expr.substr(1, close - 1), ctx);
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
                const auto* def = find_default(ctx.defaults, col);
                const bool absent_is_valid = !ctx.strict_absent || (def != nullptr && !def->is_null());
                auto ordinal = find_col_index(*ctx.chunk, col);
                if (!ordinal.has_value()) {
                    return constant_leaf(r, absent_is_valid);
                }
                // IS NOT NULL is not an operator of its own — it is a negated is_null, which is
                // exactly how the graph builder wants to see it.
                auto negated = expressions::make_compare_union_expression(r, CT::union_not);
                negated->append_child(expressions::make_compare_expression(r,
                                                                           CT::is_null,
                                                                           column_operand(r, *ordinal),
                                                                           expressions::param_storage{}));
                return negated;
            }
            if (expr.size() > kIsNull.size() && expr.substr(expr.size() - kIsNull.size()) == kIsNull) {
                auto col = std::string(trim(expr.substr(0, expr.size() - kIsNull.size())));
                // Mirror of IS NOT NULL: an absent column stores its (non-NULL)
                // DEFAULT when one exists, so IS NULL fails; with no default the
                // stored value IS NULL.
                const auto* def = find_default(ctx.defaults, col);
                const bool absent_is_null = !ctx.strict_absent || def == nullptr || def->is_null();
                auto ordinal = find_col_index(*ctx.chunk, col);
                if (!ordinal.has_value()) {
                    return constant_leaf(r, absent_is_null);
                }
                return expressions::make_compare_expression(r,
                                                            CT::is_null,
                                                            column_operand(r, *ordinal),
                                                            expressions::param_storage{});
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

                auto apply = [&op](types::compare_t cmp) -> bool {
                    using Cmp = types::compare_t;
                    if (op == ">")
                        return cmp == Cmp::more;
                    if (op == "<")
                        return cmp == Cmp::less;
                    if (op == ">=")
                        return cmp == Cmp::more || cmp == Cmp::equals;
                    if (op == "<=")
                        return cmp == Cmp::less || cmp == Cmp::equals;
                    if (op == "=")
                        return cmp == Cmp::equals;
                    if (op == "<>")
                        return cmp != Cmp::equals;
                    return true;
                };

                auto ordinal = find_col_index(*ctx.chunk, col_name);
                if (!ordinal.has_value()) {
                    // Absent-column policy, resolved here: with a non-NULL DEFAULT the stored row
                    // carries that value, so the comparison is decided against it once, for every
                    // row; without one, keep the legacy pass (untyped/unknown shape).
                    const auto* def = find_default(ctx.defaults, col_name);
                    if (!ctx.strict_absent || def == nullptr || def->is_null()) {
                        return constant_leaf(r, true);
                    }
                    return constant_leaf(r, apply(col_is_rhs ? const_val.compare(*def) : def->compare(const_val)));
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
                auto literal = constant_operand(const_val,
                                                ctx.chunk->data[*ordinal].type(),
                                                ctx.session_tz,
                                                ctx.params,
                                                &ctx.next_id,
                                                &convertible);
                if (!convertible) {
                    // The literal does not fit the column's type, so the CHECK can say nothing
                    // about the value — the same answer the unrecognised-expression case gives.
                    return constant_leaf(r, true);
                }
                auto column = column_operand(r, *ordinal);
                return col_is_rhs ? expressions::make_compare_expression(r, compare_type_of(), literal, column)
                                  : expressions::make_compare_expression(r, compare_type_of(), column, literal);
            }

            // Unrecognised expression — pass.
            return constant_leaf(r, true);
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
            check_build_context build_ctx{&column_defaults_,
                                          write_set_named_,
                                          schema_chunk,
                                          graph_context.timezone_offset,
                                          &check_params_,
                                          0};
            checks.reserve(check_exprs_.size());
            for (const auto& entry : check_exprs_) {
                compiled_check_t compiled;
                auto tree = build_check_expression(resource_, entry.second, build_ctx);
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

            // Fixed-ARRAY element checks. A value shorter than the column's declared size is
            // reconciled to it by padding NULL (casts::array_cast), and this validates the rows
            // the DML has ALREADY written — so the pad has happened and the short value is no
            // longer short. What survives it is a NULL element, which is what a NOT NULL column
            // cannot hold, so that is what is tested. The length is still compared for a write-set
            // that reaches here unreconciled. Validated per column: one bad element fails the
            // operation.
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
                        decisions == nullptr || decisions->is_null(row) || decisions->get_value<bool>(row);
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
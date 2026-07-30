#include "operator_check_constraint.hpp"

#include "constraint_util.hpp"
#include <components/expressions/bound/bound_expression.hpp>
#include <components/expressions/bound/expression_executor.hpp>

#include <optional>
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

        // ------------------------------------------------------------------ CHECK binding
        //
        // A CHECK is stored as SQL TEXT and parsed by hand here. What the text walk produces is a
        // BOUND EXPRESSION TREE, bound ONCE against the write-set the statement actually carries.
        //
        // WHY THE WRITE-SET AND NOT THE DECLARED SCHEMA. The check validates the SUBMITTED row, but
        // a column omitted from an INSERT column list is filled with the table DEFAULT agent-side at
        // storage_append -- so what gets STORED is not what was submitted, and a column the check
        // names may not be in front of it at all. Absence has THREE outcomes, and each one is a
        // different LEAF, chosen here rather than branched on per row:
        //
        //   present                          -> a reference at the ordinal it actually occupies
        //   absent, non-NULL DEFAULT, named  -> a constant holding that default (what gets stored)
        //   absent otherwise                 -> a NULL constant, so the comparison is UNKNOWN and
        //                                       permits() lets the row through
        //
        // WHY ON THE FIRST CHUNK AND NOT IN THE CONSTRUCTOR. Both the presence answer and the
        // ORDINAL come from the write-set, which does not exist until the first batch. Binding
        // earlier would mean assuming the chunk's column order matches the statement's column list;
        // that assumption is unproven, and reading the wrong column is a mistake a constraint
        // operator makes silently. One statement carries one write-set shape, so binding on the
        // first chunk is still binding ONCE.
        // The DECLARED type of `col`, or nullptr when the plan did not carry one.
        const types::complex_logical_type*
        find_column_type(const std::vector<std::pair<std::string, types::complex_logical_type>>* column_types,
                         std::string_view col) {
            if (column_types == nullptr) {
                return nullptr;
            }
            for (const auto& [name, type] : *column_types) {
                if (name == col) {
                    return &type;
                }
            }
            return nullptr;
        }

        expressions::bound_expression_ptr constant_bool(std::pmr::memory_resource* r, bool v) {
            return expressions::bound_expression_ptr{
                expressions::make_bound_constant(r, types::logical_value_t{r, v})};
        }

        // First name match, exactly as find_col does -- the write-set may legally carry duplicate
        // column names and the boxed path took the first, so this must too.
        // has_name() stays in front of the comparison: it is not a guard against a crashing
        // accessor (name() is total), it is the question "is this column named at all". A
        // malformed constraint can hand us an empty `name` -- " = 5" trims to no left operand --
        // and an UNNAMED column must not answer to it.
        std::optional<uint32_t> find_col_index(const vector::data_chunk_t& chunk, std::string_view name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                const auto& column = chunk.data[c];
                if (column.has_name() && column.name() == name) {
                    return static_cast<uint32_t>(c);
                }
            }
            return std::nullopt;
        }

        bool compare_type_of(std::string_view op, expressions::compare_type& out) {
            using CT = expressions::compare_type;
            if (op == ">") { out = CT::gt; return true; }
            if (op == "<") { out = CT::lt; return true; }
            if (op == ">=") { out = CT::gte; return true; }
            if (op == "<=") { out = CT::lte; return true; }
            if (op == "=") { out = CT::eq; return true; }
            if (op == "<>") { out = CT::ne; return true; }
            return false;
        }

        struct check_bind_t {
            std::pmr::memory_resource* r;
            const vector::data_chunk_t* chunk;
            const std::vector<std::pair<std::string, types::logical_value_t>>* defaults;
            const std::vector<std::pair<std::string, types::complex_logical_type>>* column_types;
            bool strict_absent;
        };

        expressions::bound_expression_ptr bind_check_expression(const check_bind_t& b, std::string_view expr);

        expressions::bound_expression_ptr
        bind_union(const check_bind_t& b, expressions::compare_type op,
                   std::pmr::vector<expressions::bound_expression_ptr> children) {
            auto bound = expressions::make_bound_conjunction(b.r, op, std::move(children));
            // A conjunction this layer cannot form is treated the way an unrecognised expression is:
            // it passes. The operator's standing rule is that what it cannot decide, it permits, so a
            // binding failure can never newly REJECT a row that is valid today.
            return bound.has_error() ? constant_bool(b.r, true) : std::move(bound.value());
        }

        expressions::bound_expression_ptr bind_check_expression(const check_bind_t& b, std::string_view expr) {
            using CT = expressions::compare_type;
            expr = trim(expr);

            if (expr.empty()) {
                return constant_bool(b.r, true);
            }

            // NOT (...)
            if (expr.size() > 5 && expr.substr(0, 5) == "NOT (") {
                std::pmr::vector<expressions::bound_expression_ptr> nested{b.r};
                nested.push_back(bind_check_expression(b, strip_outer(expr.substr(4))));
                return bind_union(b, CT::union_not, std::move(nested));
            }

            // Paren-led: find the matching ')' then look for AND/OR after it.
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
                        std::pmr::vector<expressions::bound_expression_ptr> nested{b.r};
                        nested.push_back(bind_check_expression(b, expr.substr(1, close - 1)));
                        nested.push_back(bind_check_expression(b, strip_outer(after.substr(4))));
                        return bind_union(b, CT::union_and, std::move(nested));
                    }
                    if (after.size() >= 3 && after.substr(0, 3) == "OR ") {
                        std::pmr::vector<expressions::bound_expression_ptr> nested{b.r};
                        nested.push_back(bind_check_expression(b, expr.substr(1, close - 1)));
                        nested.push_back(bind_check_expression(b, strip_outer(after.substr(3))));
                        return bind_union(b, CT::union_or, std::move(nested));
                    }
                    if (close == expr.size() - 1)
                        return bind_check_expression(b, expr.substr(1, close - 1));
                }
            }

            // IS NOT NULL / IS NULL. Both are TOTAL predicates -- a definite TRUE or FALSE, never
            // UNKNOWN -- so when the column is absent the answer is a CONSTANT, decided here from
            // the default policy rather than asked per row.
            constexpr std::string_view kIsNotNull = " IS NOT NULL";
            constexpr std::string_view kIsNull = " IS NULL";
            for (const auto& [suffix, want_null] :
                 {std::pair<std::string_view, bool>{kIsNotNull, false}, std::pair<std::string_view, bool>{kIsNull, true}}) {
                if (expr.size() <= suffix.size() || expr.substr(expr.size() - suffix.size()) != suffix) {
                    continue;
                }
                const auto col = std::string(trim(expr.substr(0, expr.size() - suffix.size())));
                if (auto index = find_col_index(*b.chunk, col)) {
                    auto reference = expressions::bound_expression_ptr{
                        expressions::make_bound_reference(b.r, b.chunk->data[*index].type(), *index,
                                                          expressions::side_t::left)};
                    auto bound = expressions::make_bound_null_test(b.r, want_null ? CT::is_null : CT::is_not_null,
                                                                    std::move(reference));
                    return bound.has_error() ? constant_bool(b.r, true) : std::move(bound.value());
                }
                // Absent. A column omitted from a NAME-addressed write-set stores its DEFAULT when it
                // has a non-NULL one, and NULL otherwise; a positional write-set proves nothing by a
                // name miss, so it keeps the pass-through.
                const auto* def = find_default(b.defaults, col);
                const bool stored_is_null = !b.strict_absent ? want_null : (def == nullptr || def->is_null());
                return constant_bool(b.r, want_null ? stored_is_null : !stored_is_null);
            }

            // Binary comparison, operators longest-first so ">=" is not read as ">".
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
                auto is_const = [](std::string_view t) {
                    return !t.empty() && (t.front() == '\'' || (t.front() >= '0' && t.front() <= '9') ||
                                          t.front() == '-' || t.front() == '.');
                };
                const bool col_is_rhs = is_const(lhs);
                const auto col_name = std::string(col_is_rhs ? rhs : lhs);
                auto const_val = parse_const(b.r, col_is_rhs ? lhs : rhs);
                // The literal is typed from the column's DECLARED type, once. parse_const can only
                // answer STRING_LITERAL / DOUBLE / BIGINT, so over any other column type it produces
                // a constant of the wrong type -- which used to abort with assertions on and, worse,
                // silently accept violating rows without them.
                if (const auto* declared = find_column_type(b.column_types, col_name)) {
                    if (const_val.type() != *declared) {
                        auto typed = const_val.cast_as(*declared, core::date::timezone_offset_t{});
                        if (!typed.has_error() && !typed.value().is_null()) {
                            const_val = std::move(typed.value());
                        }
                    }
                }

                expressions::compare_type cmp{};
                if (!compare_type_of(op, cmp)) {
                    return constant_bool(b.r, true); // unreachable operator: do not reject
                }

                // THE THREE ABSENCE OUTCOMES, as three different leaves.
                expressions::bound_expression_ptr column_leaf;
                if (auto index = find_col_index(*b.chunk, col_name)) {
                    column_leaf = expressions::bound_expression_ptr{
                        expressions::make_bound_reference(b.r, b.chunk->data[*index].type(), *index,
                                                          expressions::side_t::left)};
                } else {
                    const auto* def = find_default(b.defaults, col_name);
                    if (b.strict_absent && def != nullptr && !def->is_null()) {
                        column_leaf = expressions::bound_expression_ptr{expressions::make_bound_constant(b.r, *def)};
                    } else {
                        // Stores NULL (or a positional write-set proves nothing): a NULL of the
                        // literal's own type, so the comparison propagates it to UNKNOWN and
                        // permits() lets the row through -- what the boxed path answered directly.
                        //
                        // It must be a real NULL and not a constant TRUE, even though a lone
                        // UNKNOWN and a lone TRUE both permit: under `NOT (x > 5)` they diverge.
                        // tri_not(UNKNOWN) is UNKNOWN and still permits; tri_not(TRUE) is FALSE and
                        // rejects the row.
                        column_leaf = expressions::bound_expression_ptr{
                            expressions::make_bound_null_constant(b.r, const_val.type())};
                    }
                }

                auto literal = expressions::bound_expression_ptr{expressions::make_bound_constant(b.r, const_val)};
                auto bound = col_is_rhs
                                 ? expressions::make_bound_comparison(b.r, cmp, std::move(literal), std::move(column_leaf))
                                 : expressions::make_bound_comparison(b.r, cmp, std::move(column_leaf), std::move(literal));
                return bound.has_error() ? constant_bool(b.r, true) : std::move(bound.value());
            }

            // Unrecognised expression — pass.
            return constant_bool(b.r, true);
        }

    } // anonymous namespace

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::vector<std::string> not_null_columns,
        std::vector<std::pair<std::string, std::string>> check_exprs,
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs,
        std::vector<std::pair<std::string, types::logical_value_t>> column_defaults,
        bool write_set_named,
        std::vector<std::pair<std::string, types::complex_logical_type>> column_types)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns))
        , column_defaults_(std::move(column_defaults))
        , write_set_named_(write_set_named)
        // The CHECK TEXT is kept; the tree is bound on the first write-set batch, because both the
        // presence answer and the column ordinals come from the write-set and it does not exist yet.
        , check_exprs_(std::move(check_exprs))
        , check_executors_(resource)
        , array_size_reqs_(std::move(array_size_reqs))
        , column_types_(std::move(column_types)) {}

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

            // M3-B2/B3: the write-set's column names come from the chunk's schema record.
            // Both loops below used to read complex_logical_type::alias() straight off the
            // column type, which asserts on its extension and dereferences null in release
            // (types.cpp:334-337) — and a write-set column built with no name has no
            // extension at all. The schema record answers an empty name instead, so the
            // match is total over every chunk shape. One read covers both loops: nothing
            // here mutates `chunk` (it is const).
            const auto& schema = chunk.schema();

            // NOT NULL checks. A column ABSENT from the write-set stores the table
            // DEFAULT when one exists (filled agent-side); with no non-NULL default
            // the stored value IS NULL — a violation (e.g. an INSERT omitting a
            // PRIMARY KEY column, which pg_attribute never marks attnotnull).
            for (const auto& col_name : not_null_columns_) {
                bool found = false;
                for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                    if (std::string_view{schema[col].name} != std::string_view{col_name})
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
                bool found = false;
                for (uint64_t col = 0; col < chunk.column_count(); ++col) {
                    if (std::string_view{schema[col].name} != std::string_view{col_name})
                        continue;
                    found = true;
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
                // Not found by alias — two different situations, only one of them benign.
                // A NAMED write-set addresses columns by the statement's own column list,
                // so absence PROVES the statement omitted the column: the stored row takes
                // the table DEFAULT, and every column listed here is NOT NULL with no
                // DEFAULT (see enrich_insert_sync), so the NOT NULL loop above has already
                // rejected that row — nothing left to size-check.
                // An UNNAMED write-set (INSERT without a column list) aliases arbitrarily,
                // so a miss proves nothing: the column may well be present under another
                // alias, carrying a value shorter than the declared size. Dropping the
                // requirement there accepts a row this operator exists to reject, so refuse
                // the write instead.
                if (!found && !write_set_named_) {
                    set_error(core::error_t{
                        core::error_code_t::other_error,
                        std::pmr::string{"NOT NULL array column '" + col_name +
                                             "' is not addressable in the write-set: the INSERT carries no column "
                                             "list, so its required size of " +
                                             std::to_string(required_size) + " elements cannot be verified",
                                         resource_}});
                    return;
                }
            }

            // CHECK expression evaluation.
            //
            // Bound ONCE, against the first write-set batch: presence and ordinals both come from
            // the write-set, and one statement carries one write-set shape.
            if (!check_bound_ && !check_exprs_.empty()) {
                const check_bind_t binding{resource_, &chunk, &column_defaults_, &column_types_, write_set_named_};
                check_executors_.reserve(check_exprs_.size());
                for (const auto& [name, expr_str] : check_exprs_) {
                    auto root = bind_check_expression(binding, expr_str);
                    auto executor = expressions::expression_executor_t::create(resource_, std::move(root));
                    if (executor.has_error()) {
                        // A tree that cannot be executed permits, exactly as one that cannot be
                        // bound does: this operator never rejects on its own inability to decide.
                        check_executors_.emplace_back(std::nullopt);
                        continue;
                    }
                    check_executors_.emplace_back(std::move(executor.value()));
                }
                check_bound_ = true;
            }

            expressions::expression_executor_t::context_t execution{};
            for (size_t i = 0; i < check_executors_.size(); ++i) {
                if (!check_executors_[i]) {
                    continue;
                }
                auto produced = check_executors_[i]->execute(chunk, chunk.size(), execution);
                // An evaluation error is a violation, not a propagated error -- the same collapse
                // the boxed path made (`check_result.has_error() || !permits(...)`).
                bool violated = produced.has_error();
                if (!violated) {
                    const auto* answer = produced.value();
                    for (uint64_t row = 0; row < chunk.size(); ++row) {
                        // A CHECK is violated only by a definitely-FALSE result; UNKNOWN (a NULL
                        // operand, even under NOT) permits the row -- SQL rejects only definite
                        // FALSE. The validity read gates the value read.
                        const auto tri = answer->validity().row_is_valid(row)
                                             ? types::tri_of(answer->data<bool>()[row])
                                             : types::tri_bool_t::unknown;
                        if (!types::permits(tri)) {
                            violated = true;
                            break;
                        }
                    }
                }
                if (violated) {
                    set_error(core::error_t{
                        core::error_code_t::other_error,
                        std::pmr::string{"CHECK constraint \"" + check_exprs_[i].first + "\" violated", resource_}});
                    return;
                }
            }
        }
    }

} // namespace components::operators
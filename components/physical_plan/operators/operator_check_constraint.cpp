#include "operator_check_constraint.hpp"

#include "constraint_util.hpp"
#include <components/cursor/cursor.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/clone_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/types/logical_value.hpp>

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::operators {

    namespace {

        // NOTHING HERE BUILDS A CONSTANT PREDICATE, and nothing here derives a value for a
        // column. A CHECK over a name the write-set does not carry is a constraint enforced
        // by nothing, so bind_to_write_set_ refuses it rather than substituting a constant
        // that leaves the predicate UNKNOWN — and UNKNOWN permits the row.
        std::optional<size_t> find_col_index(const vector::data_chunk_t& chunk, std::string_view name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                if (chunk.data[c].type().alias() == name) {
                    return c;
                }
            }
            return std::nullopt;
        }

    } // anonymous namespace

    operator_check_constraint_t::operator_check_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::vector<std::string> not_null_columns,
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates,
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs,
        types::parameter_map_t check_params)
        : read_write_operator_t(resource, log, operator_type::check_constraint)
        , not_null_columns_(std::move(not_null_columns))
        , array_size_reqs_(std::move(array_size_reqs))
        , check_predicates_(std::move(check_predicates)) {
        check_params_.insert(check_params.begin(), check_params.end());
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

    core::result_wrapper_t<expressions::expression_ptr>
    operator_check_constraint_t::bind_to_write_set_(const expressions::expression_ptr& predicate,
                                                    const vector::data_chunk_t& chunk,
                                                    std::string_view constraint_name) {
        auto bound = expressions::clone_expression(resource_, predicate);
        if (!bound) {
            // Only a null predicate (or the unreachable `invalid` group) clones to nothing. Passing
            // it on would classify as condition_kind::always and skip the constraint entirely.
            return core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"CHECK constraint \"" + std::string{constraint_name} +
                                     "\" carries no predicate to evaluate",
                                 resource_}};
        }
        // A column reference is a position in the write-set, and nothing else. The write-set IS the
        // materialised row — the INSERT's omissions were expanded above the journal, the UPDATE's
        // write-set is the gathered storage row — so every column of the table is in it, and a name
        // that is not names no column this operator can read. Substituting a constant for it (the
        // column's DEFAULT, or NULL) is how a declared CHECK comes to judge nothing at all: NULL
        // leaves the predicate UNKNOWN, and UNKNOWN PERMITS the row.
        std::string missing_column;
        const auto rebind = [&](expressions::param_storage& operand, auto&& recurse) -> void {
            if (expressions::is_expr(operand)) {
                auto& nested = expressions::as_expr(operand);
                if (nested) {
                    recurse(nested.get(), recurse);
                }
                return;
            }
            if (!expressions::is_key(operand)) {
                return;
            }
            auto& key = expressions::as_key(operand);
            if (key.is_null()) {
                return;
            }
            const std::string name = key.as_string();
            if (auto ordinal = find_col_index(chunk, name); ordinal.has_value()) {
                key.set_path(std::pmr::vector<size_t>{{*ordinal}, resource_});
                return;
            }
            // First one wins: the message names a column, and the walk cannot unwind from here.
            if (missing_column.empty()) {
                missing_column = name;
            }
        };
        const auto walk = [&](expressions::expression_i* node, auto&& self) -> void {
            if (node == nullptr) {
                return;
            }
            if (node->group() == expressions::expression_group::compare) {
                auto* compare = static_cast<expressions::compare_expression_t*>(node);
                if (compare->is_union()) {
                    for (auto& child : compare->children()) {
                        self(child.get(), self);
                    }
                } else {
                    rebind(compare->left(), self);
                    rebind(compare->right(), self);
                }
                return;
            }
            if (node->group() == expressions::expression_group::scalar) {
                for (auto& param : static_cast<expressions::scalar_expression_t*>(node)->params()) {
                    rebind(param, self);
                }
                return;
            }
            if (node->group() == expressions::expression_group::function) {
                for (auto& param : static_cast<expressions::function_expression_t*>(node)->args()) {
                    rebind(param, self);
                }
                return;
            }
            if (node->group() == expressions::expression_group::cast) {
                rebind(static_cast<expressions::cast_expression_t*>(node)->child(), self);
            }
        };
        walk(bound.get(), walk);
        if (!missing_column.empty()) {
            return core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"CHECK constraint \"" + std::string{constraint_name} +
                                     "\" cannot be evaluated: the column \"" + missing_column +
                                     "\" is not in the written row",
                                 resource_}};
        }
        return bound;
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
        if (schema_chunk != nullptr && !check_predicates_.empty()) {
            checks.reserve(check_predicates_.size());
            for (const auto& entry : check_predicates_) {
                compiled_check_t compiled;
                auto bound_r = bind_to_write_set_(entry.second, *schema_chunk, entry.first);
                if (bound_r.has_error()) {
                    // A constraint the engine cannot bind is not a constraint that permits
                    // everything: it fails the statement that would have been judged by it.
                    set_error(bound_r.error());
                    return;
                }
                auto bound = std::move(bound_r.value());
                compiled.condition = expressions::classify_condition(bound);
                if (compiled.condition == expressions::condition_kind::computed) {
                    auto types = schema_chunk->types();
                    auto built = expressions::build_condition_graph(resource_, check_params_, bound.get(), types);
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
                            std::pmr::string{"CHECK constraint \"" + check_predicates_[index].first + "\" violated",
                                             resource_}});
                        return;
                    }
                }
            }
        }
    }

} // namespace components::operators
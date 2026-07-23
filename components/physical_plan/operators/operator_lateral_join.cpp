#include "operator_lateral_join.hpp"

#include "join_utils.hpp"
#include "operator_data.hpp"
#include "predicates/predicate.hpp"

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/vector/data_chunk.hpp>

#include <optional>

namespace components::operators {

    using join_detail::eager_join_builder;
    using join_type = components::logical_plan::join_type;

    operator_lateral_join_t::operator_lateral_join_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     join_type type,
                                                     std::pmr::vector<correlation_t> correlations,
                                                     expressions::expression_ptr on_expression,
                                                     std::pmr::vector<types::complex_logical_type> outer_schema,
                                                     std::pmr::vector<types::complex_logical_type> inner_schema)
        : read_only_operator_t(resource, std::move(log), operator_type::join)
        , type_(type)
        , correlations_(std::move(correlations))
        , on_expression_(std::move(on_expression))
        , outer_schema_(std::move(outer_schema))
        , inner_schema_(std::move(inner_schema)) {}

    void operator_lateral_join_t::reset_subtree(const operator_ptr& op) {
        if (!op) {
            return;
        }
        op->reset_for_reuse();
        op->reset_pipeline_state();
        reset_subtree(op->left());
        reset_subtree(op->right());
    }

    actor_zeta::unique_future<void> operator_lateral_join_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto err = co_await drive_(ctx);
        if (err.contains_error()) {
            set_error(err);
            mark_failed();
            co_return;
        }
        mark_executed();
        co_return;
    }

    actor_zeta::unique_future<core::error_t> operator_lateral_join_t::drive_(pipeline::context_t* ctx) {
        auto* res = resource_;
        if (!outer_ || !inner_) {
            co_return core::error_t::no_error();
        }
        if (!ctx->runner) {
            co_return core::error_t{core::error_code_t::physical_plan_error,
                                    std::pmr::string{"lateral join: no sub-plan runner", res}};
        }
        // The output layout is fully determined at plan time (see the class comment).
        // validate_schema stamps every node's output_types(), so both must be present.
        if (outer_schema_.empty() || inner_schema_.empty()) {
            co_return core::error_t{core::error_code_t::create_physical_plan_error,
                                    std::pmr::string{"lateral join: unresolved side schema", res}};
        }

        const size_t outer_count = outer_schema_.size();
        const size_t inner_count = inner_schema_.size();

        // Resolve each correlated parameter to its outer column index (by column
        // alias: full slash-joined name first, then the trailing segment).
        std::pmr::vector<std::pair<core::parameter_id_t, size_t>> bindings(res);
        bindings.reserve(correlations_.size());
        for (const auto& [param_id, key] : correlations_) {
            const std::string full = key.as_string();
            const std::string last =
                key.storage().empty() ? full : std::string(key.storage().back().data(), key.storage().back().size());
            size_t found = outer_count;
            for (size_t col = 0; col < outer_count; ++col) {
                if (!outer_schema_[col].has_alias()) {
                    continue;
                }
                const std::string& alias = outer_schema_[col].alias();
                if (alias == full || alias == last) {
                    found = col;
                    break;
                }
            }
            if (found == outer_count) {
                co_return core::error_t{
                    core::error_code_t::create_physical_plan_error,
                    std::pmr::string{"lateral join: correlated column '" + full + "' not found in outer schema", res}};
            }
            bindings.emplace_back(param_id, found);
        }

        // Snapshot the correlation slots we are about to overwrite so the caller's
        // parameter map is restored on EVERY exit path. A LATERAL join is only a
        // transient per-outer-row rebind of shared parameters; leaking the last
        // outer row's values into ctx would surprise a sibling operator or a nested
        // LATERAL that reads the same context.
        std::pmr::vector<std::pair<core::parameter_id_t, std::optional<types::logical_value_t>>> saved_slots(res);
        saved_slots.reserve(bindings.size());
        for (const auto& [param_id, col] : bindings) {
            auto it = ctx->parameters.parameters.find(param_id);
            saved_slots.emplace_back(param_id,
                                     it == ctx->parameters.parameters.end()
                                         ? std::nullopt
                                         : std::optional<types::logical_value_t>{it->second});
        }
        struct slot_restorer {
            logical_plan::storage_parameters* parameters;
            const std::pmr::vector<std::pair<core::parameter_id_t, std::optional<types::logical_value_t>>>& saved;
            ~slot_restorer() {
                for (const auto& [param_id, value] : saved) {
                    if (value.has_value()) {
                        parameters->parameters.insert_or_assign(param_id, *value);
                    } else {
                        parameters->parameters.erase(param_id);
                    }
                }
            }
        } restorer{&ctx->parameters, saved_slots};

        // Output layout: (outer columns ++ inner columns), fixed from the plan-time
        // schemas. Built once, up front — so a LEFT join can NULL-pad an outer row
        // even when the inner side produces zero rows for every outer row.
        // Semi-/anti-join output is the OUTER (left) schema ONLY: each outer row is
        // emitted at most once (semi iff the inner side has >=1 matching row, anti iff
        // it has none), with no inner columns. inner/left keep the (outer ++ inner) layout.
        const bool semi_anti = (type_ == join_type::semi || type_ == join_type::anti);
        std::pmr::vector<types::complex_logical_type> out_types(res);
        out_types.reserve(outer_count + (semi_anti ? 0 : inner_count));
        for (const auto& t : outer_schema_) {
            out_types.emplace_back(t);
        }
        if (!semi_anti) {
            for (const auto& t : inner_schema_) {
                out_types.emplace_back(t);
            }
        }
        std::vector<size_t> indices_left;
        indices_left.reserve(outer_count);
        for (size_t i = 0; i < outer_count; ++i) {
            indices_left.push_back(i);
        }
        // Empty for semi/anti: emit_left_only then NULL-pads no right columns, i.e. emits
        // the bare outer row.
        std::vector<size_t> indices_right;
        if (!semi_anti) {
            indices_right.reserve(inner_count);
            for (size_t i = 0; i < inner_count; ++i) {
                indices_right.push_back(outer_count + i);
            }
        }

        chunks_vector_t result(res);
        eager_join_builder builder(res, out_types, indices_left, indices_right, result);
        // The ON predicate spans (outer columns | inner columns); build it over the
        // outer (left/probe) and inner (right/build) schemas. all_true for the comma /
        // ON true forms passes every inner row. Correlation parameters are read live
        // per row-check, so rebinding them per outer row re-uses this one predicate.
        // Semi/anti never carry a real ON (EXISTS filters inside the inner sub-plan), so
        // they use the schema-free all_true predicate — the inner side of an EXISTS body
        // may project an alias-less constant, which the schema-bound predicate builder
        // would reject.
        predicates::predicate_ptr predicate = (on_expression_ && !semi_anti)
                                                  ? predicates::create_predicate(res,
                                                                                 ctx->function_registry,
                                                                                 on_expression_,
                                                                                 outer_schema_,
                                                                                 inner_schema_,
                                                                                 &ctx->parameters,
                                                                                 ctx->session_tz)
                                                  : predicates::create_all_true_predicate(res);

        auto outer_res = co_await ctx->runner->run_subplan(outer_, ctx);
        if (outer_res.has_error()) {
            co_return outer_res.error();
        }
        chunks_vector_t outer_chunks(res);
        for (auto& chunk : outer_res.value()) {
            outer_chunks.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
        }

        for (const auto& outer_chunk : outer_chunks) {
            for (uint64_t row = 0; row < outer_chunk.size(); ++row) {
                // Bind the outer row's values into the correlated parameter slots.
                for (const auto& [param_id, col] : bindings) {
                    ctx->parameters.parameters.insert_or_assign(param_id, outer_chunk.value(col, row));
                }

                reset_subtree(inner_);
                auto inner_res = co_await ctx->runner->run_subplan(inner_, ctx);
                if (inner_res.has_error()) {
                    co_return inner_res.error();
                }

                bool matched = false;
                for (const auto& inner_chunk : inner_res.value()) {
                    if (inner_chunk.size() == 0) {
                        continue;
                    }
                    auto mask_res =
                        predicates::batch_check_1vN(predicate, outer_chunk, inner_chunk, row, inner_chunk.size());
                    if (mask_res.has_error()) {
                        co_return mask_res.error();
                    }
                    const auto& mask = mask_res.value();
                    for (uint64_t inner_row = 0; inner_row < inner_chunk.size(); ++inner_row) {
                        // selects(): only a definite TRUE joins the pair — an UNKNOWN (NULL
                        // operand) ON result matches nothing, exactly as in operator_join.
                        if (types::selects(mask[inner_row])) {
                            matched = true;
                            // inner/left emit every matched (outer ++ inner) pair; semi/anti
                            // only need the EXISTENCE of a match, not the matched rows —
                            // the first hit settles existence, skip the rest of the chunk.
                            if (semi_anti) {
                                break;
                            }
                            builder.emit_matched(outer_chunk, row, inner_chunk, inner_row);
                        }
                    }
                    if (matched && semi_anti) {
                        break; // existence settled — stop scanning inner chunks
                    }
                }
                // Emit the OUTER row once per join semantics:
                //   semi -> iff matched; anti / left -> iff NOT matched (left NULL-pads the right).
                if (type_ == join_type::semi) {
                    if (matched) {
                        builder.emit_left_only(outer_chunk, row);
                    }
                } else if (!matched && (type_ == join_type::anti || type_ == join_type::left)) {
                    builder.emit_left_only(outer_chunk, row);
                }
            }
        }

        builder.flush();
        output_ = make_operator_data(res, std::move(result));
        co_return core::error_t::no_error();
    }

} // namespace components::operators

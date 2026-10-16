#include "operator_match.hpp"

#include <components/expressions/function_expression.hpp>

namespace components::operators {

    operator_match_t::operator_match_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       const expressions::expression_ptr& expression,
                                       logical_plan::row_cap_t cap)
        : read_only_operator_t(resource, log, operator_type::match)
        , expression_(std::move(expression))
        , cap_(cap) {}

    // Shared filter core (R6): filter ONE chunk through the predicate,
    // advancing the caller-owned LIMIT running counter `limit_total` across batches,
    // and append the surviving-rows chunk to `out`. Called by push() (per
    // streamed batch, MEMBER counter). The predicate compares chunk against itself
    // (single-input filter) and surviving rows carry their absolute row_id.
    core::error_t operator_match_t::filter_batch_(std::pmr::memory_resource* resource,
                                                  const vector::vector_t* decisions,
                                                  const std::pmr::vector<types::complex_logical_type>& types,
                                                  const vector::data_chunk_t& chunk,
                                                  uint64_t& limit_total,
                                                  chunks_vector_t& out) {
        // Already at the LIMIT, or an empty input batch: nothing to emit.
        if (!cap_.admits(limit_total) || chunk.size() == 0) {
            return core::error_t::no_error();
        }

        // Build the selection of surviving rows. The selection MUST be sized to the full input
        // length (set_index is unchecked); only the first out_count slots are filled/read.
        vector::indexing_vector_t sel(resource);
        sel.reset(chunk.size());
        uint64_t out_count = 0;
        for (size_t i = 0; i < chunk.size(); i++) {
            const bool keep = decisions == nullptr || (!decisions->is_null(i) && decisions->get_value<bool>(i));
            if (!keep) {
                continue;
            }
            sel.set_index(out_count, i);
            ++out_count;
            // Count-cap: the AUTHORITATIVE affected-row bound for DML …WHERE f(x) LIMIT n
            // (no operator_limit over a DML root), and an advisory read-cap under
            // operator_limit for SELECT. OFFSET is applied by operator_limit (SELECT) and
            // does not exist for DML, so this stream is never skipped, only capped.
            ++limit_total;
            if (!cap_.admits(limit_total)) {
                break;
            }
        }
        if (out_count == 0) {
            return core::error_t::no_error();
        }

        // TYPED, no-box gather, the same one operator_having uses: data_chunk_t::copy routes each
        // column through vector_ops::copy and sets the target cardinality itself. row_ids are gathered
        // unconditionally: over a SINK the input's are zero-filled, so gathering them
        // reproduces exactly the zero sentinel the per-cell path left behind.
        vector::data_chunk_t out_chunk(resource, types, out_count);
        chunk.copy(out_chunk, sel, out_count);
        out.emplace_back(std::move(out_chunk));
        note_emitted();
        return core::error_t::no_error();
    }

    core::error_t operator_match_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Streaming filter: run the per-chunk filter+projection on the single batch
        // handed in via `input`. The predicate + projection metadata depend only on the
        // (stable) chunk schema, so they are built once on the first batch and reused.
        // The LIMIT counter (limit_total_) persists across calls so a LIMIT caps the
        // total emitted across ALL batches. (OFFSET is applied by operator_limit for
        // SELECT and does not exist for DML, so the stream is capped, never skipped.)
        if (!stream_ready_) {
            // Stable resource for the whole streaming run: the operator's own resource_
            // when it has one (scan-source matches), else the input chunk's resource —
            // captured ONCE here. Over a sink (group/join) the match is built with a
            // null resource_ (the create_plan "no table_oid" fallback), so it must fall
            // back to the sink output's resource, which is the sink operator's stable
            // resource and outlives every batch. The cached predicate is allocated on
            // it, so it stays valid across all batches (defect fix (b)).
            stream_resource_ = resource_ ? resource_ : input.resource();
            // Rebind stream_types_ to the stable resource (it was member-initialized
            // with the possibly-null resource_). Destroy + placement-construct so its
            // allocator is the chosen resource — a plain assignment would compare the
            // old (null) allocator and crash.
            stream_types_.~vector();
            new (&stream_types_) std::pmr::vector<types::complex_logical_type>(stream_resource_);
            // Copied element-wise: input.types() lives on different resource.
            stream_types_.reserve(input.column_count());
            for (const auto& column : input.data) {
                stream_types_.push_back(column.type());
            }
            condition_ = expressions::classify_condition(expression_);
            if (condition_ == expressions::condition_kind::computed) {
                auto built = expressions::build_condition_graph(stream_resource_,
                                                                ctx->parameters,
                                                                expression_.get(),
                                                                stream_types_);
                if (built.has_error()) {
                    return built.error();
                }
                graph_ = std::move(built.value());
            }
            stream_ready_ = true;
        }
        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }
        std::optional<vector::data_chunk_t> produced;
        if (graph_) {
            // rebound per outer row by LATERAL, so the parameters are re-read for every chunk
            graph_->set_parameters(&ctx->parameters);
            if (auto error = graph_->process(input, ctx->execution_context); error.contains_error()) {
                return error;
            }
            auto computed = graph_->finalize(ctx->execution_context, input.size());
            if (computed.has_error()) {
                return computed.error();
            }
            produced = std::move(computed.value());
        }
        const vector::vector_t* decisions = produced.has_value() ? &produced->data.front() : nullptr;
        return filter_batch_(stream_resource_, decisions, stream_types_, input, stream_limit_total_, out);
    }

    core::error_t operator_match_t::finalize(pipeline::context_t* /*ctx*/, chunks_vector_t& out) {
        if (emitted() || stream_types_.empty()) {
            return core::error_t::no_error();
        }
        auto* res = stream_resource_ ? stream_resource_ : resource_;
        vector::data_chunk_t empty(res, stream_types_, 0);
        empty.set_cardinality(0);
        out.emplace_back(std::move(empty));
        return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    operator_match_t::source_next(pipeline::context_t* /*ctx*/) {
        // Sourceless no-table match (left_ == nullptr): no input to filter — an empty result.
        co_return std::nullopt;
    }

} // namespace components::operators

#include "operator_join.hpp"
#include "join_utils.hpp"

#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_join_t::operator_join_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     type join_type,
                                     const expressions::expression_ptr& expression)
        : read_only_operator_t(resource, log, operator_type::join)
        , join_type_(join_type)
        , expression_(expression) {}

    core::error_t operator_join_t::build_layout_(pipeline::context_t* context,
                                                 const vector::data_chunk_t& probe_front) {
        // Lazily derive the output layout, condition graph and (right/full) the
        // matched marker once, from the materialized build (right) side and one probe
        // (left) schema chunk. Used by push().
        const auto& build_chunks = right_->output()->chunks();
        // operator_data_t always holds at least one (possibly empty) chunk.
        assert(!build_chunks.empty());

        res_types_ = std::pmr::vector<types::complex_logical_type>{resource_};
        // Nested-loop is never swapped: it evaluates its ON via key.side(), so the
        // probe is always logical-left and the build always logical-right.
        join_detail::compute_join_layout(probe_front,
                                         build_chunks.front(),
                                         /*swapped=*/false,
                                         res_types_,
                                         indices_left_,
                                         indices_right_);
        join_detail::compute_active_indices(probe_front,
                                            build_chunks.front(),
                                            indices_left_,
                                            indices_right_,
                                            &active_indices_);

        condition_ = expressions::classify_condition(expression_);
        if (condition_ == expressions::condition_kind::computed) {
            // The ON is resolved against the two sides' merged layout, so the right side's
            // ordinals start at probe_front.column_count().
            auto merged_types = probe_front.types();
            const auto build_types = build_chunks.front().types();
            merged_types.insert(merged_types.end(), build_types.begin(), build_types.end());

            auto built = expressions::build_condition_graph(resource_,
                                                            context->parameters.parameters,
                                                            expression_.get(),
                                                            merged_types,
                                                            probe_front.column_count());
            if (built.has_error()) {
                return built.error();
            }
            graph_ = std::move(built.value());
        }

        // RIGHT/FULL: size the flat matched marker over all build rows, with
        // per-chunk start offsets so build row (chunk,row) maps to
        // build_matched_[build_chunk_offsets_[chunk] + row].
        build_matched_.clear();
        build_chunk_offsets_.clear();
        build_chunk_offsets_.reserve(build_chunks.size());
        uint64_t total = 0;
        for (const auto& B : build_chunks) {
            build_chunk_offsets_.push_back(total);
            total += B.size();
        }
        if (join_type_ == type::right || join_type_ == type::full) {
            build_matched_.assign(total, 0);
        }

        layout_built_ = true;
        return core::error_t::no_error();
    }

    void operator_join_t::probe_batch_(pipeline::context_t* context,
                                       const vector::data_chunk_t& probe,
                                       chunks_vector_t& out) {
        // Probe ONE left batch against the materialized build (right) chunks and
        // emit per join_type_, in left-major order (mirrors operator_hash_join_t):
        // for each probe row, emit matched rows across the build chunks (build-chunk
        // order); inner emits only matches, left/full also emit a left-only row when
        // no build row matched, right/full additionally mark matched build rows so
        // finalize() can NULL-pad the unmatched ones. NULL padding and the output
        // column layout are produced by the shared join_builder, so the result is
        // identical to operator_hash_join_t and to the nested-loop reference.
        const auto& build_chunks = right_->output()->chunks();
        const bool left_outer = (join_type_ == type::left || join_type_ == type::full);
        const bool mark_matched = (join_type_ == type::right || join_type_ == type::full);

        builder_.set_output(&out);

        chunks_vector_t merged(resource_);
        if (graph_) {
            const auto probe_types = probe.types();
            merged.reserve(build_chunks.size());
            for (const auto& B : build_chunks) {
                merged.push_back(join_detail::merged_chunk(resource_, probe_types, B));
            }
        }
        if (graph_) {
            graph_->set_parameters(&context->parameters.parameters);
        }

        const uint64_t n = probe.size();
        for (uint64_t li = 0; li < n; ++li) {
            bool matched = false;
            for (size_t ci = 0; condition_ != expressions::condition_kind::never && ci < build_chunks.size(); ++ci) {
                const auto& B = build_chunks[ci];
                if (B.size() == 0) {
                    continue;
                }
                std::optional<vector::data_chunk_t> decided;
                if (graph_) {
                    auto& chunk = merged[ci];
                    join_detail::point_at_probe_row(resource_, chunk, probe, li);
                    if (auto error = graph_->process(chunk, context->execution_context); error.contains_error()) {
                        set_error(error);
                        builder_.gather();
                        return;
                    }
                    auto produced = graph_->finalize(context->execution_context, chunk.size());
                    if (produced.has_error()) {
                        set_error(produced.error());
                        builder_.gather();
                        return;
                    }
                    decided = std::move(produced.value());
                }
                const vector::vector_t* decisions = decided.has_value() ? &decided->data.front() : nullptr;
                for (uint64_t rj = 0; rj < B.size(); ++rj) {
                    if (decisions == nullptr || (!decisions->is_null(rj) && decisions->get_value<bool>(rj))) {
                        builder_.emit_matched(probe, li, B, rj);
                        matched = true;
                        if (mark_matched) {
                            build_matched_[build_chunk_offsets_[ci] + rj] = 1;
                        }
                    }
                }
            }
            if (!matched && left_outer) {
                builder_.emit_left_only(probe, li);
            }
        }
        builder_.gather();
    }

    void operator_join_t::emit_unmatched_build_(chunks_vector_t& out) {
        // RIGHT/FULL only: drain build rows that no probe row matched, NULL-padded
        // on the left side. Inner/left finalize to a no-op here.
        if (join_type_ != type::right && join_type_ != type::full) {
            return;
        }
        if (!right_ || !right_->output()) {
            return;
        }
        const auto& build_chunks = right_->output()->chunks();
        builder_.set_output(&out);
        for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
            const auto& B = build_chunks[ci];
            const uint64_t base = build_chunk_offsets_[ci];
            for (uint64_t rj = 0; rj < B.size(); ++rj) {
                if (build_matched_[base + rj] == 0) {
                    builder_.emit_right_only(B, rj);
                }
            }
        }
    }

    core::error_t operator_join_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // The build (right) side is materialized by a separate sub-plan before the
        // first push and always holds at least one (possibly empty) chunk. A truly
        // absent right_ is a degenerate plan: emit nothing (no build rows to
        // preserve, no layout to pad against).
        if (!right_ || !right_->output()) {
            layout_built_ = true;
            return core::error_t::no_error();
        }
        if (!layout_built_) {
            if (auto error = build_layout_(ctx, input); error.contains_error()) {
                return error;
            }
        }
        probe_batch_(ctx, input, out);
        if (has_error()) {
            return get_error();
        }
        return core::error_t::no_error();
    }

    core::error_t operator_join_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        // RIGHT/FULL: drain unmatched build rows, NULL-padded on the left side. The
        // builder holds its output chunk across probe batches, so this is also where
        // the last partial chunk is emitted, for EVERY join type.
        //
        // If push() never ran (the probe source emitted its drain sentinel before
        // any schema'd batch), the layout is unbuilt and res_types_ is empty: with no
        // probe schema there is no left column layout to NULL-pad against, so the
        // only safe action is to skip emission. The common 0-row-probe case still
        // pushes a schema'd batch, so res_types_ is set and this branch is not taken.
        if (!layout_built_ || res_types_.empty()) {
            return core::error_t::no_error();
        }
        emit_unmatched_build_(out);
        builder_.set_output(&out);
        builder_.flush();
        if (builder_.emitted()) {
            note_emitted();
        }
        if (!emitted()) {
            vector::data_chunk_t empty(resource_, res_types_, 0);
            empty.set_cardinality(0);
            out.emplace_back(std::move(empty));
        }
        return core::error_t::no_error();
    }

} // namespace components::operators

#include "operator_join.hpp"
#include "join_utils.hpp"
#include "predicate_executor.hpp"

#include <components/vector/vector_operations.hpp>

namespace components::operators {

    using join_detail::join_builder;

    operator_join_t::operator_join_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     type join_type,
                                     const expressions::expression_ptr& expression)
        : read_only_operator_t(resource, log, operator_type::join)
        , join_type_(join_type)
        , expression_(expression) {}

    void operator_join_t::build_layout_(pipeline::context_t* context, const vector::data_chunk_t& probe_front) {
        // Lazily derive the output layout, predicate and (right/full) the matched
        // marker once, from the materialized build (right) side and one probe
        // (left) schema chunk. Used by push().
        const auto& build_chunks = right_->output()->chunks();
        // operator_data_t always holds at least one (possibly empty) chunk.
        assert(!build_chunks.empty());

        // Nested-loop is never swapped: it evaluates its ON via key.side(), so the
        // probe is always logical-left and the build always logical-right.
        join_detail::compute_join_layout(resource_,
                                         probe_front,
                                         build_chunks.front(),
                                         /*swapped=*/false,
                                         res_schema_,
                                         indices_left_,
                                         indices_right_);

        // A null expression means CROSS -- every pair matches -- and the facade turns that into a
        // constant TRUE rather than a separate all-true predicate, so there is one evaluation path.
        auto executor = predicate_executor_t::create(resource_,
                                                     expression_,
                                                     probe_front.schema(),
                                                     build_chunks.front().schema(),
                                                     context->function_registry,
                                                     &context->parameters,
                                                     context->session_tz);
        if (executor.has_error()) {
            set_error(executor.error());
            return;
        }
        predicate_.emplace(std::move(executor.value()));
        // Kept for probe_batch_, and REFRESHED on every push (see push()): the bound tree reads its
        // parameter slots live, and a correlated (LATERAL) sub-query rebinds them between two runs
        // of this same operator.
        current_parameters_ = &context->parameters;
        current_session_tz_ = context->session_tz;
        // Sized ONCE to the widest build chunk this join can see: set_index is unchecked, and the
        // selection is indexed by BUILD row because that is what a 1-probe-row batch answers.
        match_selection_ = vector::indexing_vector_t{resource_, vector::DEFAULT_VECTOR_CAPACITY};

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
    }

    void operator_join_t::probe_batch_(const vector::data_chunk_t& probe, chunks_vector_t& out) {
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

        join_builder builder(resource_, res_schema_, indices_left_, indices_right_, out);

        const uint64_t n = probe.size();
        for (uint64_t li = 0; li < n; ++li) {
            bool matched = false;
            for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
                const auto& B = build_chunks[ci];
                if (B.size() == 0) {
                    continue;
                }
                // ONE probe row against the whole build chunk. select_matches applies the join rule
                // itself: a pair is emitted only when the ON predicate is definitely TRUE, so a NULL
                // join key yields UNKNOWN and matches nothing.
                match_selection_.reset(B.size());
                auto matches = predicate_->select_matches(probe,
                                                          li,
                                                          B,
                                                          B.size(),
                                                          *current_parameters_,
                                                          current_session_tz_,
                                                          match_selection_);
                if (matches.has_error()) {
                    set_error(matches.error());
                    builder.flush();
                    return;
                }
                for (uint64_t k = 0; k < matches.value(); ++k) {
                    const uint64_t rj = match_selection_.get_index(k);
                    builder.emit_matched(probe, li, B, rj);
                    matched = true;
                    if (mark_matched) {
                        build_matched_[build_chunk_offsets_[ci] + rj] = 1;
                    }
                }
            }
            if (!matched && left_outer) {
                builder.emit_left_only(probe, li);
            }
        }
        builder.flush();
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
        join_builder builder(resource_, res_schema_, indices_left_, indices_right_, out);
        for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
            const auto& B = build_chunks[ci];
            const uint64_t base = build_chunk_offsets_[ci];
            for (uint64_t rj = 0; rj < B.size(); ++rj) {
                if (build_matched_[base + rj] == 0) {
                    builder.emit_right_only(B, rj);
                }
            }
        }
        builder.flush();
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
            build_layout_(ctx, input);
        }
        current_parameters_ = &ctx->parameters;
        current_session_tz_ = ctx->session_tz;
        probe_batch_(input, out);
        if (has_error()) {
            return get_error();
        }
        return core::error_t::no_error();
    }

    core::error_t operator_join_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        // RIGHT/FULL: drain unmatched build rows, NULL-padded on the left side.
        //
        // If push() never ran (the probe source emitted its drain sentinel before
        // any schema'd batch), the layout is unbuilt and res_schema_ is empty: with no
        // probe schema there is no left column layout to NULL-pad against, so the
        // only safe action is to skip emission. The common 0-row-probe case still
        // pushes a schema'd batch, so res_schema_ is set and this branch is not taken.
        if (!layout_built_ || res_schema_.empty()) {
            return core::error_t::no_error();
        }
        emit_unmatched_build_(out);
        return core::error_t::no_error();
    }

} // namespace components::operators

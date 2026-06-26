#include "operator_join.hpp"
#include "join_utils.hpp"
#include "predicates/predicate.hpp"

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

        res_types_ = std::pmr::vector<types::complex_logical_type>{resource_};
        join_detail::compute_join_layout(probe_front,
                                         build_chunks.front(),
                                         res_types_,
                                         indices_left_,
                                         indices_right_);

        predicate_ = expression_ ? predicates::create_predicate(resource_,
                                                                context->function_registry,
                                                                expression_,
                                                                probe_front.types(),
                                                                build_chunks.front().types(),
                                                                &context->parameters,
                                                                context->session_tz)
                                  : predicates::create_all_true_predicate(resource_);

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

        join_builder builder(resource_, res_types_, indices_left_, indices_right_, out);

        const uint64_t n = probe.size();
        for (uint64_t li = 0; li < n; ++li) {
            bool matched = false;
            for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
                const auto& B = build_chunks[ci];
                if (B.size() == 0) {
                    continue;
                }
                auto results = predicates::batch_check_1vN(predicate_, probe, B, li, B.size());
                if (results.has_error()) {
                    set_error(results.error());
                    builder.flush();
                    return;
                }
                const auto& mask = results.value();
                for (uint64_t rj = 0; rj < B.size(); ++rj) {
                    if (mask[rj]) {
                        builder.emit_matched(probe, li, B, rj);
                        matched = true;
                        if (mark_matched) {
                            build_matched_[build_chunk_offsets_[ci] + rj] = 1;
                        }
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
        join_builder builder(resource_, res_types_, indices_left_, indices_right_, out);
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

    core::error_t
    operator_join_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
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
        // any schema'd batch), the layout is unbuilt and res_types_ is empty: with no
        // probe schema there is no left column layout to NULL-pad against, so the
        // only safe action is to skip emission. The common 0-row-probe case still
        // pushes a schema'd batch, so res_types_ is set and this branch is not taken.
        if (!layout_built_ || res_types_.empty()) {
            return core::error_t::no_error();
        }
        emit_unmatched_build_(out);
        return core::error_t::no_error();
    }

} // namespace components::operators

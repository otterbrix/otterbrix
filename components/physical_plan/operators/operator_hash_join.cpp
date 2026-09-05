#include "operator_hash_join.hpp"
#include "join_utils.hpp"

#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector.hpp>
#include <core/operations_helper.hpp>

#include <cstdint>
#include <type_traits>

namespace components::operators {

    using hash_join_detail::right_index_t;
    using hash_join_detail::row_ref;

    namespace {

        // Confirm a probe row against a candidate build row by a TYPED cell-by-cell
        // comparison over every key column (uniform for single- and multi-column
        // keys). A non-matching column short-circuits to false. Callers have already
        // excluded NULLs on both sides (NULL keys never equi-join), so
        // vector::cells_equal's NULL==NULL semantics are never exercised here.
        bool keys_verify(const vector::data_chunk_t& probe,
                         const std::pmr::vector<uint64_t>& probe_cols,
                         uint64_t probe_row,
                         const vector::data_chunk_t& build,
                         const std::pmr::vector<uint64_t>& build_cols,
                         uint64_t build_row) {
            for (size_t k = 0; k < probe_cols.size(); ++k) {
                if (!vector::cells_equal(probe.data[probe_cols[k]], probe_row, build.data[build_cols[k]], build_row)) {
                    return false;
                }
            }
            return true;
        }

        // True iff every key cell of `row` is non-NULL — a row with any NULL key
        // never participates in an equi-join match (build- or probe-side).
        bool
        keys_all_valid(const vector::data_chunk_t& chunk, const std::pmr::vector<uint64_t>& key_cols, uint64_t row) {
            for (uint64_t c : key_cols) {
                if (c >= chunk.column_count() || !chunk.data[c].validity().row_is_valid(row)) {
                    return false;
                }
            }
            return true;
        }

        // Vectorized typed hash of the key columns of one chunk into `out_hashes`
        // (one uint64 per row), via data_chunk_t::hash (per physical_type +
        // combine_hash for multi-column). data_chunk_t::hash is non-const, but the
        // hash is a pure read; the const_cast mirrors operator_group's fast path.
        // `col_ids` is owned by the operator so the call allocates nothing.
        void hash_key_columns(const vector::data_chunk_t& chunk,
                              std::vector<uint64_t>& col_ids,
                              vector::vector_t& out_hashes) {
            const_cast<vector::data_chunk_t&>(chunk).hash(col_ids, out_hashes);
        }
    } // namespace

    operator_hash_join_t::operator_hash_join_t(std::pmr::memory_resource* resource,
                                               log_t log,
                                               type join_type,
                                               size_t left_col,
                                               size_t right_col,
                                               bool swapped)
        : read_only_operator_t(resource, std::move(log), operator_type::hash_join)
        , join_type_(join_type)
        , swapped_(swapped) {
        // Single-column equi-key today (the optimizer stamps one eq(left,right));
        // stored as one-element lists so the build/probe path is arity-agnostic.
        probe_key_cols_.push_back(static_cast<uint64_t>(left_col));
        build_key_cols_.push_back(static_cast<uint64_t>(right_col));
        probe_hash_cols_.assign(probe_key_cols_.begin(), probe_key_cols_.end());
        build_hash_cols_.assign(build_key_cols_.begin(), build_key_cols_.end());
    }

    void operator_hash_join_t::build_index_() {
        right_index_.clear();
        build_matched_.clear();
        build_chunk_offsets_.clear();
        if (!right_ || !right_->output()) {
            return;
        }
        const auto& build_chunks = right_->output()->chunks();

        const bool track_matched = (join_type_ == type::right || join_type_ == type::full);

        // Per-chunk start offsets into the flat marker; total = #build rows.
        build_chunk_offsets_.reserve(build_chunks.size());
        uint64_t total = 0;
        for (const auto& B : build_chunks) {
            build_chunk_offsets_.push_back(total);
            total += B.size();
        }
        right_index_.reserve(total);
        if (track_matched) {
            build_matched_.assign(total, 0);
        }

        for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
            const auto& B = build_chunks[ci];
            if (B.size() == 0) {
                continue;
            }
            hash_key_columns(B, build_hash_cols_, hashes_);
            const auto* h = hashes_.data<uint64_t>();
            for (uint64_t rj = 0; rj < B.size(); ++rj) {
                // Skip NULL build keys — they never equi-join.
                if (!keys_all_valid(B, build_key_cols_, rj)) {
                    continue;
                }
                right_index_.emplace(h[rj], row_ref{static_cast<uint32_t>(ci), static_cast<uint32_t>(rj)});
            }
        }
    }

    void operator_hash_join_t::probe_batch_(const vector::data_chunk_t& probe, chunks_vector_t& out) {
        // build_chunks are needed to (a) verify a candidate and (b) copy matched
        // build rows into the output; both reference the materialized snapshot.
        const auto& build_chunks = right_->output()->chunks();
        const bool left_outer = (join_type_ == type::left || join_type_ == type::full);
        const bool mark_matched = (join_type_ == type::right || join_type_ == type::full);

        builder_.set_output(&out);

        const uint64_t n = probe.size();
        if (n == 0) {
            return;
        }

        hash_key_columns(probe, probe_hash_cols_, hashes_);
        const auto* h = hashes_.data<uint64_t>();

        for (uint64_t li = 0; li < n; ++li) {
            bool matched = false;
            // A NULL probe key matches nothing (left-outer still emits the row).
            if (keys_all_valid(probe, probe_key_cols_, li)) {
                auto range = right_index_.equal_range(h[li]);
                for (auto it = range.first; it != range.second; ++it) {
                    const row_ref& ref = it->second;
                    const auto& B = build_chunks[ref.chunk_index];
                    // Collision-safe: confirm by a typed key comparison.
                    if (!keys_verify(probe, probe_key_cols_, li, B, build_key_cols_, ref.row_index)) {
                        continue;
                    }
                    builder_.emit_matched(probe, li, B, ref.row_index);
                    matched = true;
                    if (mark_matched) {
                        build_matched_[build_chunk_offsets_[ref.chunk_index] + ref.row_index] = 1;
                    }
                }
            }
            if (!matched && left_outer) {
                builder_.emit_left_only(probe, li);
            }
        }
        builder_.gather();
    }

    void operator_hash_join_t::emit_unmatched_build_(chunks_vector_t& out) {
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
                // A NULL-key build row is never marked matched, so it is correctly
                // emitted here (right/full preserve every build row).
                if (build_matched_[base + rj] == 0) {
                    builder_.emit_right_only(B, rj);
                }
            }
        }
    }

    core::error_t operator_hash_join_t::push(pipeline::context_t*, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Degenerate build side → emit nothing. This covers a truly absent right_
        // AND a build side that materialized ZERO chunks: a single-table filter
        // pushed BELOW the join can empty the build scan, and a disk scan
        // over no rows yields an operator_data_t whose chunks() is empty (not one
        // empty chunk). Either way there is no build schema to lay out and, for an
        // inner/right-probe join, no build rows to match — so compute_join_layout
        // must never be handed a missing front chunk (dereferencing
        // build_chunks.front() on an empty vector is UB: a null data_chunk_t whose
        // types() faulted under -O2). Left NULL-padding is not preserved for this
        // degenerate shape, matching the long-standing absent-right behavior;
        // pushdown never pushes a filter onto an outer join's optional side, so an
        // emptied build here is always an inner join with genuinely zero matches.
        if (!right_ || !right_->output() || right_->output()->chunks().empty()) {
            index_built_ = true;
            return core::error_t::no_error();
        }

        // Build the index + derive the output layout once, lazily.
        if (!index_built_) {
            const auto& build_chunks = right_->output()->chunks();
            // Non-empty by the degenerate-build guard above.
            res_types_ = std::pmr::vector<types::complex_logical_type>{resource_};
            join_detail::compute_join_layout(input,
                                             build_chunks.front(),
                                             swapped_,
                                             res_types_,
                                             indices_left_,
                                             indices_right_);
            join_detail::compute_active_indices(input,
                                                build_chunks.front(),
                                                indices_left_,
                                                indices_right_,
                                                &active_indices_);
            build_index_();
            index_built_ = true;
        }

        probe_batch_(input, out);
        return core::error_t::no_error();
    }

    core::error_t operator_hash_join_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        // The builder holds its output chunk ACROSS probe batches, so finalize() is
        // where the last partial chunk is emitted — for EVERY join type, not only the
        // right/full drain below.
        //
        // If push() never ran (the probe source emitted its drain sentinel before
        // any schema'd batch), the index is unbuilt and res_types_ is empty. With no
        // probe schema there is no left column layout to NULL-pad against, so the
        // only safe action is to build the index (so build_matched_ is sized) and
        // skip emission — there is genuinely no probe side to preserve rows next to.
        // The common 0-row-probe case still pushes a schema'd batch, so res_types_
        // is set there and this branch is not taken.
        if (!index_built_) {
            build_index_();
            index_built_ = true;
        }
        if (res_types_.empty()) {
            return core::error_t::no_error();
        }
        emit_unmatched_build_(out);
        builder_.set_output(&out);
        builder_.flush();
        return core::error_t::no_error();
    }

} // namespace components::operators

#include "operator_hash_join.hpp"
#include "join_utils.hpp"

#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <core/operations_helper.hpp>

#include <cstdint>
#include <type_traits>

namespace components::operators {

    using hash_join_detail::right_index_t;
    using hash_join_detail::row_ref;
    using join_detail::join_builder;

    namespace {

        // Typed cell == cell between two flat vectors, read directly from their
        // physical buffers — no logical_value_t round-trip on the hot path. Used to
        // CONFIRM a hash-bucket candidate (collision-safe verify). Callers have
        // already excluded NULLs on both sides (NULL keys never equi-join).
        template<typename T>
        bool scalar_equal(const vector::vector_t& a, uint64_t ai, const vector::vector_t& b, uint64_t bi) {
            if constexpr (std::is_floating_point_v<T>) {
                return core::is_equals<T>(a.data<T>()[ai], b.data<T>()[bi]);
            } else {
                return a.data<T>()[ai] == b.data<T>()[bi];
            }
        }

        bool cell_equal(const vector::vector_t& a, uint64_t ai, const vector::vector_t& b, uint64_t bi) {
            switch (a.type().to_physical_type()) {
                case types::physical_type::BOOL:
                case types::physical_type::INT8:
                    return scalar_equal<int8_t>(a, ai, b, bi);
                case types::physical_type::INT16:
                    return scalar_equal<int16_t>(a, ai, b, bi);
                case types::physical_type::INT32:
                    return scalar_equal<int32_t>(a, ai, b, bi);
                case types::physical_type::INT64:
                    return scalar_equal<int64_t>(a, ai, b, bi);
                case types::physical_type::UINT8:
                    return scalar_equal<uint8_t>(a, ai, b, bi);
                case types::physical_type::UINT16:
                    return scalar_equal<uint16_t>(a, ai, b, bi);
                case types::physical_type::UINT32:
                    return scalar_equal<uint32_t>(a, ai, b, bi);
                case types::physical_type::UINT64:
                    return scalar_equal<uint64_t>(a, ai, b, bi);
                case types::physical_type::INT128:
                    return scalar_equal<types::int128_t>(a, ai, b, bi);
                case types::physical_type::UINT128:
                    return scalar_equal<types::uint128_t>(a, ai, b, bi);
                case types::physical_type::FLOAT:
                    return scalar_equal<float>(a, ai, b, bi);
                case types::physical_type::DOUBLE:
                    return scalar_equal<double>(a, ai, b, bi);
                case types::physical_type::STRING:
                    return a.data<std::string_view>()[ai] == b.data<std::string_view>()[bi];
                default:
                    // The optimizer only stamps a scalar single-column equi-key, so a
                    // nested/unknown physical type never reaches the probe verify.
                    assert(false && "unhandled physical_type in hash-join key verify");
                    return false;
            }
        }

        // Confirm a probe row against a candidate build row by a TYPED cell-by-cell
        // comparison over every key column (uniform for single- and multi-column
        // keys). A non-matching column short-circuits to false.
        bool keys_verify(const vector::data_chunk_t& probe,
                         const std::pmr::vector<uint64_t>& probe_cols,
                         uint64_t probe_row,
                         const vector::data_chunk_t& build,
                         const std::pmr::vector<uint64_t>& build_cols,
                         uint64_t build_row) {
            for (size_t k = 0; k < probe_cols.size(); ++k) {
                if (!cell_equal(probe.data[probe_cols[k]], probe_row, build.data[build_cols[k]], build_row)) {
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
        void hash_key_columns(const vector::data_chunk_t& chunk,
                              const std::pmr::vector<uint64_t>& key_cols,
                              vector::vector_t& out_hashes) {
            std::vector<uint64_t> col_ids(key_cols.begin(), key_cols.end());
            const_cast<vector::data_chunk_t&>(chunk).hash(col_ids, out_hashes);
        }
    } // namespace

    operator_hash_join_t::operator_hash_join_t(std::pmr::memory_resource* resource,
                                               log_t log,
                                               type join_type,
                                               size_t left_col,
                                               size_t right_col)
        : read_only_operator_t(resource, std::move(log), operator_type::hash_join)
        , join_type_(join_type) {
        // Single-column equi-key today (the optimizer stamps one eq(left,right));
        // stored as one-element lists so the build/probe path is arity-agnostic.
        probe_key_cols_.push_back(static_cast<uint64_t>(left_col));
        build_key_cols_.push_back(static_cast<uint64_t>(right_col));
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
            vector::vector_t hashes(resource_, types::logical_type::UBIGINT, B.size());
            hash_key_columns(B, build_key_cols_, hashes);
            const auto* h = hashes.data<uint64_t>();
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

        join_builder builder(resource_, res_types_, indices_left_, indices_right_, out);

        const uint64_t n = probe.size();
        if (n == 0) {
            builder.flush();
            return;
        }

        vector::vector_t hashes(resource_, types::logical_type::UBIGINT, n);
        hash_key_columns(probe, probe_key_cols_, hashes);
        const auto* h = hashes.data<uint64_t>();

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
                    builder.emit_matched(probe, li, B, ref.row_index);
                    matched = true;
                    if (mark_matched) {
                        build_matched_[build_chunk_offsets_[ref.chunk_index] + ref.row_index] = 1;
                    }
                }
            }
            if (!matched && left_outer) {
                builder.emit_left_only(probe, li);
            }
        }
        builder.flush();
    }

    void operator_hash_join_t::emit_unmatched_build_(chunks_vector_t& out) {
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
                // A NULL-key build row is never marked matched, so it is correctly
                // emitted here (right/full preserve every build row).
                if (build_matched_[base + rj] == 0) {
                    builder.emit_right_only(B, rj);
                }
            }
        }
        builder.flush();
    }

    core::error_t operator_hash_join_t::push(pipeline::context_t*, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // The build (right) side is materialized by a separate sub-plan before the
        // first push and always holds at least one (possibly empty) chunk. A truly
        // absent right_ is a degenerate plan: emit nothing (no left layout to
        // pad against, no build rows to preserve).
        if (!right_ || !right_->output()) {
            index_built_ = true;
            return core::error_t::no_error();
        }

        // Build the index + derive the output layout once, lazily.
        if (!index_built_) {
            const auto& build_chunks = right_->output()->chunks();
            // operator_data_t always holds at least one (possibly empty) chunk.
            assert(!build_chunks.empty());
            res_types_ = std::pmr::vector<types::complex_logical_type>{resource_};
            join_detail::compute_join_layout(input, build_chunks.front(), res_types_, indices_left_, indices_right_);
            build_index_();
            index_built_ = true;
        }

        probe_batch_(input, out);
        return core::error_t::no_error();
    }

    core::error_t operator_hash_join_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        // Right/full: drain build rows that no probe row matched, NULL-padded on the
        // left side. Other join types finalize to a no-op.
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
        return core::error_t::no_error();
    }

} // namespace components::operators

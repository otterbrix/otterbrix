#pragma once

#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <unordered_map>

namespace components::operators {

    namespace hash_join_detail {
        // Identifies one materialized build row by (chunk, row). A small struct
        // (no std::tuple / std::pair-of-many, R14): the multimap payload that the
        // typed hash key resolves to.
        struct row_ref {
            uint32_t chunk_index;
            uint32_t row_index;
        };

        // Alt-3 hash+verify index: bucket on a uint64 hash of the join-key cells
        // (typed per physical_type via data_chunk_t::hash) and resolve to the build
        // rows that hashed there. A *multimap* because equi-join keys need not be
        // unique on the build side, and because distinct keys may collide on the
        // hash — the probe always CONFIRMS by a typed cell-by-cell comparison, so a
        // collision only costs an extra failed verify, never a wrong row.
        using right_index_t = std::pmr::unordered_multimap<uint64_t, row_ref>;
    } // namespace hash_join_detail

    // Equi-join fast path: substituted for operator_join_t only when the ON
    // condition is a single eq(left.key, right.key); the matching columns
    // (`left_col`/`right_col`, into the respective input chunks) are detected at
    // plan time and passed in.
    //
    // Builds a hash table over the right (build) side once and probes it with the
    // left (probe) side, turning the nested-loop O(L·R) join into O(L + R). Output
    // layout, NULL padding and chunk-streaming match operator_join_t exactly
    // (shared join_detail helpers), so results are identical to the nested-loop path.
    //
    // The index is HASH+VERIFY and TYPED: the bucket key is a uint64 hash of the
    // key cells (per physical_type), the payload is a build-row ref, and every
    // probe match is confirmed by a typed cell-by-cell comparison — no
    // logical_value_t on the build or probe hot path. ONE uniform build+probe path
    // serves single- AND multi-column keys (the key column list is iterated) and
    // ALL hash-join types (inner / left / right / full).
    //
    // Only inner / left / right / full are ever substituted (cross is not an
    // equi-join); any other join_type is treated as a no-op.
    class operator_hash_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        operator_hash_join_t(std::pmr::memory_resource* resource,
                             log_t log,
                             type join_type,
                             size_t left_col,
                             size_t right_col);

        // The join is a SINK on its build side (it must fully retain the right
        // input before any match can be decided) and STREAMING on its probe side
        // (one left batch in -> matched/padded rows out). A single role models both:
        // execute_pipeline pushes each probe batch through push() and, after the
        // probe is drained, drains unmatched build rows (right/full) via finalize().
        // The build (right_->output()) is materialized by a separate sub-plan before
        // the first push; the index is built lazily on the first push (index_built_).
        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::sink; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

    private:
        type join_type_;
        // Equi-key column indices into the left (probe) / right (build) input
        // chunks. Stored as one-element lists so the build/probe machinery iterates
        // a key column list uniformly for single- and (future) multi-column keys.
        std::pmr::vector<uint64_t> probe_key_cols_{resource_};
        std::pmr::vector<uint64_t> build_key_cols_{resource_};
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        // --- Build/probe state (push + on_execute_impl share this) ---
        bool index_built_{false};
        hash_join_detail::right_index_t right_index_{resource_};
        std::pmr::vector<types::complex_logical_type> res_types_{resource_};
        // RIGHT/FULL only: a flat "matched" marker (one byte per build row) over all
        // build chunks, with per-chunk start offsets so a row_ref{chunk,row} maps to
        // build_matched_[build_chunk_offsets_[chunk] + row]. A flat byte vector (not
        // a nested std::pmr::vector<bool>) keeps allocator handling simple and the
        // marker branch-free. Unmatched build rows are NULL-padded at finalize().
        std::pmr::vector<uint8_t> build_matched_{resource_};
        std::pmr::vector<uint64_t> build_chunk_offsets_{resource_};

        // Design intent (NOT an R6 transitional fallback): push()/finalize() =
        // streaming path (sourced pipelines); on_execute_impl = materialized path
        // for sourceless sub-plans (raw_data joins / recursive-CTE working sets /
        // no-FROM SELECT) that have no streaming probe source. Both route through
        // the SAME build_index_() + probe_batch_() + emit_unmatched_build_() core —
        // two entry points to one implementation for two plan shapes.
        void on_execute_impl(pipeline::context_t* context) override;

        // Build the hash+verify index over the materialized build (right) chunks.
        // NULL build keys are skipped — they never join under SQL equi-join
        // semantics. Also (re)sizes build_matched_ for right/full.
        void build_index_();
        // Probe one left batch against the index and emit per join_type_ via the
        // shared join_builder. Marks matched build rows for right/full.
        void probe_batch_(const vector::data_chunk_t& probe, chunks_vector_t& out);
        // Emit unmatched build rows (right/full) NULL-padded on the left side.
        void emit_unmatched_build_(chunks_vector_t& out);
    };

} // namespace components::operators

#pragma once

#include <components/execution_dag/execution_dag.hpp>
#include <components/expressions/expression.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/physical_plan/operators/join_utils.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

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
    // condition is a single eq of two operands that each read one side; the
    // operands arrive as the probe and build key expressions, each evaluated over
    // its own side's chunks.
    //
    // Builds a hash table over the right (build) side once and probes it with the
    // left (probe) side, turning the nested-loop O(L·R) join into O(L + R). Output
    // layout, NULL padding and chunk-streaming match operator_join_t exactly
    // (shared join_detail helpers), so results are identical to the nested-loop path.
    //
    // The index is HASH+VERIFY and TYPED: the bucket key is a uint64 hash of the
    // key cells (per physical_type), the payload is a build-row ref, and every
    // probe match is confirmed by a typed cell-by-cell comparison. ONE uniform build+probe path
    // serves single- AND multi-column keys (the key column list is iterated) and
    // ALL hash-join types (inner / left / right / full).
    //
    // Only inner / left / right / full are ever substituted (cross is not an
    // equi-join); any other join_type is treated as a no-op.
    class operator_hash_join_t final : public read_only_operator_t {
    public:
        using type = logical_plan::join_type;

        // `swapped` records that the build-side-selection heuristic moved the
        // LOGICAL-left table into the physical build (right_) slot (so the probe /
        // build keys are the operands of the sides after that swap). It is passed
        // through to compute_join_layout so the output is re-assembled in logical
        // [left, right] order. Defaulted so the (still unswapped) call sites and the
        // nested-loop lowering compile unchanged; the build-side heuristic sets it.
        // INNER-only — an outer join is never swapped.
        operator_hash_join_t(std::pmr::memory_resource* resource,
                             log_t log,
                             type join_type,
                             std::pmr::vector<expressions::expression_ptr> probe_keys,
                             std::pmr::vector<expressions::expression_ptr> build_keys,
                             bool swapped = false);

        // The join is a SINK on its build side (it must fully retain the right
        // input before any match can be decided) and STREAMING on its probe side
        // (one left batch in -> matched/padded rows out). A single role models both:
        // execute_pipeline pushes each probe batch through push() and, after the
        // probe is drained, drains unmatched build rows (right/full) via finalize().
        // The build (right_->output()) is materialized by a separate sub-plan before
        // the first push. Right and full joins index it on the first push; inner and
        // left joins only on the first non-empty probe batch (index_built_).

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

        // Drop the lazily-built index + derived layout so a re-driven sub-plan (the
        // recursive-CTE recursive term, re-run per fixpoint iteration over a repointed
        // working set) rebuilds the hash table from the NEW build side instead of reusing
        // the stale one. reset_for_reuse() clears state_/output_ but not this build state.
        void reset_pipeline_state() noexcept override {
            layout_ready_ = false;
            index_built_ = false;
            build_rows_ = 0;
            probe_graph_.reset();
            build_graph_.reset();
            build_keys_.clear();
            probe_keys_.clear();
            right_index_.clear();
            res_types_.clear();
            build_matched_.clear();
            build_chunk_offsets_.clear();
            indices_left_.clear();
            indices_right_.clear();
            builder_.reset();
        }

    private:
        type join_type_;
        // Plan-time orientation: true iff the physical build (right_) side is the
        // LOGICAL-left table. Fixed at construction — NOT cleared in
        // reset_pipeline_state() (a re-driven recursive-CTE term keeps the same plan).
        bool swapped_;
        std::pmr::vector<expressions::expression_ptr> probe_key_exprs_;
        std::pmr::vector<expressions::expression_ptr> build_key_exprs_;
        std::unique_ptr<execution_dag::execution_dag_t> probe_graph_;
        std::unique_ptr<execution_dag::execution_dag_t> build_graph_;
        // One key chunk per build chunk, taken out of the build graph; the probe's is referenced, never copied.
        std::pmr::vector<vector::data_chunk_t> build_keys_{resource_};
        std::pmr::vector<vector::data_chunk_t> probe_keys_{resource_};
        std::vector<uint64_t> key_columns_;
        std::vector<size_t> indices_left_;
        std::vector<size_t> indices_right_;

        // --- Build/probe state (shared by push) ---
        bool layout_ready_{false};
        bool index_built_{false};
        uint64_t build_rows_{0};
        hash_join_detail::right_index_t right_index_{resource_};
        std::pmr::vector<types::complex_logical_type> res_types_{resource_};
        // RIGHT/FULL only: a flat "matched" marker (one byte per build row) over all
        // build chunks, with per-chunk start offsets so a row_ref{chunk,row} maps to
        // build_matched_[build_chunk_offsets_[chunk] + row]. A flat byte vector (not
        // a nested std::pmr::vector<bool>) keeps allocator handling simple and the
        // marker branch-free. Unmatched build rows are NULL-padded at finalize().
        std::pmr::vector<uint8_t> build_matched_{resource_};
        std::pmr::vector<uint64_t> build_chunk_offsets_{resource_};

        join_detail::join_builder builder_{resource_, res_types_, indices_left_, indices_right_};
        vector::vector_t hashes_{resource_, types::logical_type::UBIGINT, vector::DEFAULT_VECTOR_CAPACITY};

        [[nodiscard]] core::error_t key_graph_(pipeline::context_t* ctx,
                                               const std::pmr::vector<expressions::expression_ptr>& keys,
                                               const vector::data_chunk_t& sample,
                                               uint64_t capacity,
                                               std::unique_ptr<execution_dag::execution_dag_t>* graph);
        // Build the hash+verify index over the materialized build (right) chunks.
        // NULL build keys are skipped — they never join under SQL equi-join
        // semantics. Also (re)sizes build_matched_ for right/full.
        [[nodiscard]] core::error_t build_index_(pipeline::context_t* ctx);
        // Probe one left batch against the index and emit per join_type_ via the
        // shared join_builder. Marks matched build rows for right/full.
        [[nodiscard]] core::error_t
        probe_batch_(pipeline::context_t* ctx, const vector::data_chunk_t& probe, chunks_vector_t& out);
        // Emit unmatched build rows (right/full) NULL-padded on the left side.
        void emit_unmatched_build_(chunks_vector_t& out);
    };

} // namespace components::operators

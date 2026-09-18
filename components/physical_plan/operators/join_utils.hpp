#pragma once

#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector_operations.hpp>

#include <vector>

// Shared building blocks for the join operators. operator_join_t (nested-loop,
// all join types) and operator_hash_join_t (equi-join fast path) produce the
// same output layout and stream rows the same way; the only difference is how
// they decide which (left, right) row pairs match. Everything that is common
// lives here so the two operators stay in sync.
namespace components::operators::join_detail {

    // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
    // They must be skipped when copying — vector_ops::copy would dereference a null data_.
    bool is_placeholder(const vector::vector_t& v) noexcept;

    // turn 1 row into a const_vector (every row redirects to the same value)
    vector::vector_t broadcast_row(std::pmr::memory_resource* resource, const vector::vector_t& source, uint64_t row);

    vector::data_chunk_t merged_chunk(std::pmr::memory_resource* resource,
                                      const std::pmr::vector<types::complex_logical_type>& probe_types,
                                      const vector::data_chunk_t& build);

    // Re-point a merged chunk's probe columns at row `row` of `probe`.
    void point_at_probe_row(std::pmr::memory_resource* resource,
                            vector::data_chunk_t& chunk,
                            const vector::data_chunk_t& probe,
                            uint64_t row);

    // Computes the joined output schema and the per-side column→output-slot maps.
    //
    // The output schema is assembled in LOGICAL [left, right] order regardless of
    // which physical input the operator materialized as its build side:
    //   * `left_front`  is the operator's PROBE (physical left) front chunk.
    //   * `right_front` is the operator's BUILD (physical right) front chunk.
    //   * `swapped == false` — probe is logical-left, build is logical-right:
    //       output = [probe cols 0..Lw, build cols Lw..Lw+Bw].
    //   * `swapped == true`  — build is logical-left, probe is logical-right (the
    //       build-side-selection heuristic moved the smaller/logical-left table into
    //       the physical build slot): output = [build cols 0..Bw, probe cols Bw..Bw+Lw].
    // `indices_left`/`indices_right` stay pure source→slot maps: indices_left[c] is
    // the output slot for probe column c, indices_right[c] for build column c. The
    // join_builder gathers each source column into its slot, so it never needs to
    // know the orientation — only these maps change.
    //
    // The width invariant (Lw + Bw == total output cols) holds by construction from
    // the same front chunks, so it is never guarded on the hot path.
    //
    // Duplicate (same-aliased) columns across the two sides stay addressable via
    // their table qualifier; USING/NATURAL column-merging is resolved at the logical
    // layer (validate_logical_plan.cpp), not here.
    void compute_join_layout(const vector::data_chunk_t& left_front,
                             const vector::data_chunk_t& right_front,
                             bool swapped,
                             std::pmr::vector<types::complex_logical_type>& res_types,
                             std::vector<size_t>& indices_left,
                             std::vector<size_t>& indices_right);

    // Computes which OUTPUT slots need a real buffer.
    void compute_active_indices(const vector::data_chunk_t& left_front,
                                const vector::data_chunk_t& right_front,
                                const std::vector<size_t>& indices_left,
                                const std::vector<size_t>& indices_right,
                                std::vector<size_t>* active_indices);

    // Streams join output into a chunks_vector_t where every chunk is
    // ≤ DEFAULT_VECTOR_CAPACITY (1024) rows.
    //
    // Rather than materialize one cell at a time (which allocated a length-(li+1)
    // indexing sequence per cell), the builder BUFFERS the matched source-row
    // indices and writes them out with ONE indexed vector_ops::copy (a gather) per
    // (source-chunk, column). The probe (left) side is single-source per gather
    // window, so it is one gather per left column; the build (right) side may span
    // several chunks, so output rows are REORDERED grouped by their build chunk —
    // one gather per (build-chunk, right column).
    //
    // Consequence: the operator's emit ROW order is UNSPECIFIED (downstream
    // sorts/groups absorb it). COLUMN identity stays logical [left, right] — that is
    // an orthogonal axis fixed by compute_join_layout / the indices_* maps.
    //
    // OWNERSHIP: the builder OWNS the pending output chunk across probe batches, and
    // the operator owns the builder
    // A gather window is used in exactly one of two modes, matching the callers:
    //   * probe mode  — emit_matched / emit_left_only: every buffered row has a valid
    //     left (probe) source; right source is a build chunk (matched) or NULL (left-only).
    //   * drain mode  — emit_right_only only: every row's left source is NULL, right
    //     source is a build chunk.
    // NULL-padding therefore routes to the LOGICAL side via the same source→slot maps.
    // Both modes write into the SAME pending chunk, which is correct: they produce rows
    // in one output layout, and only the per-window gather differs.
    class join_builder {
    public:
        join_builder(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& out_types,
                     const std::vector<size_t>& indices_left,
                     const std::vector<size_t>& indices_right,
                     const std::vector<size_t>& active_indices);

        // The destination for completed chunks. push()/finalize() each receive their own
        // out vector, so the operator re-seats this per call; the PENDING chunk is
        // unaffected and keeps filling across them.
        void set_output(chunks_vector_t* out) noexcept { out_ = out; }
        // Whether this builder has pushed any chunk since construction
        [[nodiscard]] bool emitted() const noexcept { return emitted_; }

        void emit_matched(const vector::data_chunk_t& L, uint64_t li, const vector::data_chunk_t& R, uint64_t rj);

        // L row with NULLs on all right-side output columns.
        void emit_left_only(const vector::data_chunk_t& L, uint64_t li);

        // R row with NULLs on all left-side output columns.
        void emit_right_only(const vector::data_chunk_t& R, uint64_t rj);

        // Copy every buffered row into the pending chunk and release the source
        // pointers. MUST run before the sources die — i.e. before push() returns.
        // Emits only chunks that filled completely; a partial chunk stays pending.
        void gather();

        // gather(), then emit the pending chunk even if it is only partly filled.
        // The operator calls this once, at the end of the whole probe.
        void flush();

        // Drop pending output and buffered rows without emitting (re-driven sub-plan).
        void reset() noexcept;

    private:
        // Bounds the index buffers. Mid-batch, so the sources are still alive.
        void ensure_space_();

        void ensure_slots_();

        void ensure_pending_();

        void emit_pending_();

        // Write buffered rows [begin, end) into the pending chunk at `dst_offset`.
        void gather_range_(uint64_t begin, uint64_t end, uint64_t dst_offset);

        std::pmr::memory_resource* resource_;
        const std::pmr::vector<types::complex_logical_type>& out_types_;
        const std::vector<size_t>& indices_left_;
        const std::vector<size_t>& indices_right_;
        const std::vector<size_t>& active_indices_;
        chunks_vector_t* out_ = nullptr;
        bool emitted_ = false;

        // The pending output chunk, held in a one-element vector: data_chunk_t has no
        // empty state to use as a sentinel and no default constructor, and the vector's
        // own buffer is allocated once and reused across every chunk.
        chunks_vector_t pending_;
        uint64_t pending_rows_ = 0;

        // Buffered source-row indices for the current gather window.
        // `left_chunk_` is the single probe source (nullptr in drain mode); a NULL
        // entry in `buf_right_chunks_` marks a left-only row (NULL right side).
        const vector::data_chunk_t* left_chunk_ = nullptr;
        uint64_t filled_ = 0;
        std::pmr::vector<uint64_t> buf_left_rows_;
        std::pmr::vector<const vector::data_chunk_t*> buf_right_chunks_;
        std::pmr::vector<uint64_t> buf_right_rows_;

        // Gather scratch. Members, not locals: these were rebuilt on every flush, which
        // is once per output chunk on a path whose cost is allocator calls. clear()/
        // assign() keep the capacity.
        std::pmr::vector<uint32_t> entry_group_;
        std::pmr::vector<const vector::data_chunk_t*> group_chunk_;
        std::pmr::vector<uint64_t> group_start_;
        std::pmr::vector<uint64_t> order_;
        std::pmr::vector<uint64_t> cursor_;
        std::pmr::vector<uint8_t> slot_live_;
        bool slots_ready_ = false;
        // Sized once at DEFAULT_VECTOR_CAPACITY and rewritten per gather — a window
        // never exceeds it, and vector_ops::copy only reads the first `count` entries.
        vector::indexing_vector_t indexing_;
    };

    // Eager join output builder for operators whose BUILD (right) side is NOT
    // materialized once for the whole probe but REGENERATED per outer row — namely
    // operator_lateral_join_t, which re-runs its inner sub-plan for every outer row.
    //
    // The lazy join_builder above buffers raw (left_chunk, right_chunk) POINTERS and
    // gathers them at flush(). That is sound ONLY when (i) a single probe chunk feeds
    // the whole build and (ii) every build chunk outlives flush(). LATERAL breaks
    // BOTH invariants: the outer input spans several persistent chunks (so a single
    // overwritten left_chunk_ would mix rows across a flush window), and each inner
    // result is destroyed at the end of its outer-row iteration (so a buffered right
    // pointer would dangle by the post-loop flush → heap-use-after-free). This builder
    // instead COPIES each row's left+right cells into the current output chunk at emit
    // time, while both sources are still alive and correctly identified — no pointer
    // buffering, so neither invariant is needed.
    //
    // Row/column semantics match join_builder: logical [left, right] output layout via
    // the indices_* source→slot maps, NULL-padding the absent side for emit_left_only.
    // It is a distinct strategy for a distinct operator — the two builders never share
    // an instance or a flush path (no runtime lazy/eager toggle).
    class eager_join_builder {
    public:
        eager_join_builder(std::pmr::memory_resource* resource,
                           const std::pmr::vector<types::complex_logical_type>& out_types,
                           const std::vector<size_t>& indices_left,
                           const std::vector<size_t>& indices_right,
                           chunks_vector_t& out_chunks);

        // Matched (L row li) × (R row rj): copy both source rows into the output now.
        void emit_matched(const vector::data_chunk_t& L, uint64_t li, const vector::data_chunk_t& R, uint64_t rj);

        // L row li with NULLs on all right-side output columns.
        void emit_left_only(const vector::data_chunk_t& L, uint64_t li);

        void flush();

    private:
        // Gather source row `srow` into output row `filled_` via the side's source→slot map.
        void copy_row_(const vector::data_chunk_t& src, uint64_t srow, const std::vector<size_t>& slots);

        void null_side_(const std::vector<size_t>& slots);

        void advance_();

        std::pmr::memory_resource* resource_;
        const std::pmr::vector<types::complex_logical_type>& out_types_;
        const std::vector<size_t>& indices_left_;
        const std::vector<size_t>& indices_right_;
        chunks_vector_t& out_chunks_;
        vector::data_chunk_t cur_;
        vector::indexing_vector_t idx1_;
        uint64_t filled_ = 0;
    };

} // namespace components::operators::join_detail

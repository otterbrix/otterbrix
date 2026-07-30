#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

#include <unordered_map>

namespace components::operators {

    // SELECT DISTINCT. A SINK on its single (LEFT) input: rows arrive batch-by-batch
    // through push(), the first occurrence of each distinct row is retained, and each
    // freshly-unique row is emitted downstream immediately (in input order). Duplicate
    // detection is the engine's canonical typed hash + verify (data_chunk_t::hash +
    // vector::cells_equal), the SAME semantics as GROUP BY / HASH JOIN / UNIQUE — so a
    // FLOAT is deduped by the one float-equality policy, a 128-bit / DECIMAL / nested
    // key is compared by value, and there is no lossy string key.
    class operator_distinct_t final : public read_only_operator_t {
    public:
        operator_distinct_t(std::pmr::memory_resource* resource, log_t log);

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        [[nodiscard]] core::error_t finalize(pipeline::context_t* ctx, chunks_vector_t& out) override;

        // DISTINCT ON: dedup on this ON-key column subset (indices into the operator's input layer)
        // instead of the whole row; the full row is still emitted. Empty = whole-row dedup (plain
        // DISTINCT). A setter (not a defaulted ctor arg) so no get_default_resource() allocation (R14).
        void set_on_keys(std::pmr::vector<size_t> on_keys) { on_keys_ = std::move(on_keys); }

    private:
        // An address into retained_ that stays valid across retained_ reallocation: the
        // vector MOVES its data_chunk_t elements when it grows, but an INDEX resolves to
        // the current element on every lookup (a pointer would dangle).
        struct retained_row_ref_t {
            std::size_t chunk_idx;
            uint64_t row;
        };

        // Distinct-row identity index, accumulated ACROSS input batches (push) so the
        // first occurrence of a row anywhere in the stream wins. hash -> the retained
        // rows carrying that hash; a hash collision is resolved by cells_equal against
        // the retained row.
        std::pmr::unordered_map<uint64_t, std::pmr::vector<retained_row_ref_t>> seen_;
        // Copies of the distinct rows kept for collision verification, on the operator's
        // stable resource_ (which outlives every transient input batch). Grows with
        // DISTINCT cardinality. retained_fill_ is the fill level of retained_.back().
        std::pmr::vector<vector::data_chunk_t> retained_;
        uint64_t retained_fill_{0};
        // DISTINCT ON key subset (empty ⇒ whole-row dedup). Set via set_on_keys by the planner.
        std::pmr::vector<size_t> on_keys_;

        // The shared dedup core: for each row of each chunk, hash it, verify against the
        // retained rows, and on first occurrence copy the row into `out` (chunks of
        // <= DEFAULT_VECTOR_CAPACITY) AND into retained_. Output preserves input order.
        void emit_distinct_(std::pmr::memory_resource* res, const chunks_vector_t& chunks, chunks_vector_t& out);
        // True iff (chunk,row) duplicates an already-retained distinct row (hash hit +
        // a cells_equal match on every column).
        bool is_duplicate_(const vector::data_chunk_t& chunk, uint64_t row, uint64_t hash) const;
        // Copy (chunk,row) into retained_ and record its ref under `hash`.
        void retain_(const vector::data_chunk_t& chunk,
                     uint64_t row,
                     uint64_t hash,
                     const vector::schema_t& schema,
                     std::pmr::memory_resource* res);
    };

} // namespace components::operators

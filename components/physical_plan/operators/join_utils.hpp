#pragma once

#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>

#include <algorithm>
#include <vector>

// Shared building blocks for the join operators. operator_join_t (nested-loop,
// all join types) and operator_hash_join_t (equi-join fast path) produce the
// same output layout and stream rows the same way; the only difference is how
// they decide which (left, right) row pairs match. Everything that is common
// lives here so the two operators stay in sync.
namespace components::operators::join_detail {

    // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
    // They must be skipped when copying — vector_ops::copy would dereference a null data_.
    inline bool is_placeholder(const vector::vector_t& v) noexcept {
        return v.data() == nullptr && v.auxiliary() == nullptr;
    }

    // Computes the joined output schema and the per-side column→output-slot maps.
    //
    // PostgreSQL-style: left columns map 1:1 to the first slots, then every right
    // column is appended in its own slot. Duplicate (same-aliased) columns across
    // the two sides stay addressable via their table qualifier; USING/NATURAL
    // column-merging is resolved at the logical layer (validate_logical_plan.cpp),
    // not here.
    //
    // `res_types` is (re)filled starting from the left front-chunk types.
    inline void compute_join_layout(const vector::data_chunk_t& left_front,
                                    const vector::data_chunk_t& right_front,
                                    std::pmr::vector<types::complex_logical_type>& res_types,
                                    std::vector<size_t>& indices_left,
                                    std::vector<size_t>& indices_right) {
        res_types = left_front.types();
        auto right_types = right_front.types();
        size_t left_col_count = left_front.column_count();
        size_t right_col_count = right_front.column_count();

        indices_left.clear();
        indices_right.clear();
        indices_left.reserve(left_col_count);
        indices_right.reserve(right_col_count);
        for (size_t i = 0; i < left_col_count; ++i) {
            indices_left.emplace_back(i);
        }
        for (size_t i = 0; i < right_col_count; ++i) {
            indices_right.emplace_back(left_col_count + i);
            res_types.push_back(right_types[i]);
        }
    }

    // Streams join output into a chunks_vector_t where every chunk is
    // ≤ DEFAULT_VECTOR_CAPACITY (1024) rows. Emits rows one at a time via
    // vector_ops::copy and flushes on each full chunk.
    class join_builder {
    public:
        join_builder(std::pmr::memory_resource* resource,
                     const std::pmr::vector<types::complex_logical_type>& out_types,
                     const std::vector<size_t>& indices_left,
                     const std::vector<size_t>& indices_right,
                     chunks_vector_t& out_chunks)
            : resource_(resource)
            , out_types_(out_types)
            , indices_left_(indices_left)
            , indices_right_(indices_right)
            , out_chunks_(out_chunks)
            , cur_(resource, out_types, vector::DEFAULT_VECTOR_CAPACITY) {}

        void flush() {
            if (filled_ == 0) {
                return;
            }
            cur_.set_cardinality(filled_);
            out_chunks_.emplace_back(std::move(cur_));
            cur_ = vector::data_chunk_t(resource_, out_types_, vector::DEFAULT_VECTOR_CAPACITY);
            filled_ = 0;
        }

        void emit_matched(const vector::data_chunk_t& L, uint64_t li, const vector::data_chunk_t& R, uint64_t rj) {
            ensure_space();
            copy_left_row(L, li);
            copy_right_row(R, rj);
            ++filled_;
        }

        // L row with NULLs on all right-side output columns.
        void emit_left_only(const vector::data_chunk_t& L, uint64_t li) {
            ensure_space();
            copy_left_row(L, li);
            for (size_t c = 0; c < indices_right_.size(); ++c) {
                cur_.data[indices_right_[c]].validity().set_invalid(filled_);
            }
            ++filled_;
        }

        // R row with NULLs on all left-side output columns.
        void emit_right_only(const vector::data_chunk_t& R, uint64_t rj) {
            ensure_space();
            copy_right_row(R, rj);
            for (size_t c = 0; c < indices_left_.size(); ++c) {
                cur_.data[indices_left_[c]].validity().set_invalid(filled_);
            }
            ++filled_;
        }

    private:
        void ensure_space() {
            if (filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
                flush();
            }
        }

        void copy_left_row(const vector::data_chunk_t& L, uint64_t li) {
            for (size_t c = 0; c < L.column_count(); ++c) {
                if (is_placeholder(L.data[c]))
                    continue;
                vector::vector_ops::copy(L.data[c], cur_.data[indices_left_[c]], li + 1, li, filled_);
            }
        }

        void copy_right_row(const vector::data_chunk_t& R, uint64_t rj) {
            for (size_t c = 0; c < R.column_count(); ++c) {
                if (is_placeholder(R.data[c]))
                    continue;
                vector::vector_ops::copy(R.data[c], cur_.data[indices_right_[c]], rj + 1, rj, filled_);
            }
        }

        std::pmr::memory_resource* resource_;
        const std::pmr::vector<types::complex_logical_type>& out_types_;
        const std::vector<size_t>& indices_left_;
        const std::vector<size_t>& indices_right_;
        chunks_vector_t& out_chunks_;
        vector::data_chunk_t cur_;
        uint64_t filled_ = 0;
    };

} // namespace components::operators::join_detail

#pragma once

#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::operators::join {
    // Streams join output into a std::vector<data_chunk_t> where every chunk is
    // ≤ DEFAULT_VECTOR_CAPACITY (1024) rows. Emits rows one at a time via
    // vector_ops::copy and flushes on each full chunk.
    class join_builder_t {
    public:
        join_builder_t(std::pmr::memory_resource* resource,
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

        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
        // They must be skipped when copying — vector_ops::copy would dereference a null data_.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }

        void ensure_space() {
            if (filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
                flush();
            }
        }

        void copy_left_row(const vector::data_chunk_t& L, uint64_t li) {
            for (size_t c = 0; c < L.column_count(); ++c) {
                if (is_placeholder(L.data[c])) {
                    continue;
                }
                vector::vector_ops::copy(L.data[c], cur_.data[indices_left_[c]], li + 1, li, filled_);
            }
        }

        void copy_right_row(const vector::data_chunk_t& R, uint64_t rj) {
            for (size_t c = 0; c < R.column_count(); ++c) {
                if (is_placeholder(R.data[c])) {
                    continue;
                }
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

} // namespace components::operators::join


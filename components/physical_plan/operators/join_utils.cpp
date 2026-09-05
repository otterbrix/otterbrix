#include "join_utils.hpp"

#include <algorithm>
#include <unordered_map>

namespace components::operators::join_detail {

    bool is_placeholder(const vector::vector_t& v) noexcept { return v.data() == nullptr && v.auxiliary() == nullptr; }

    vector::vector_t broadcast_row(std::pmr::memory_resource* resource, const vector::vector_t& source, uint64_t row) {
        vector::vector_t value(resource, source.type(), 1);
        // copy's third argument is the END index in the source, not a length
        vector::vector_ops::copy(source, value, row + 1, row, 0);
        value.set_vector_type(vector::vector_type::CONSTANT);
        return value;
    }

    vector::data_chunk_t merged_chunk(std::pmr::memory_resource* resource,
                                      const std::pmr::vector<types::complex_logical_type>& probe_types,
                                      const vector::data_chunk_t& build) {
        const uint64_t capacity = build.size() > 0 ? build.size() : 1;
        vector::data_chunk_t chunk(resource, {}, capacity);
        chunk.data.reserve(probe_types.size() + build.column_count());
        for (const auto& type : probe_types) {
            chunk.data.emplace_back(resource, type, 1);
        }
        for (const auto& column : build.data) {
            vector::vector_t vec(resource, column.type(), capacity);
            vec.reference(column);
            chunk.data.push_back(std::move(vec));
        }
        chunk.set_cardinality(build.size());
        return chunk;
    }

    void point_at_probe_row(std::pmr::memory_resource* resource,
                            vector::data_chunk_t& chunk,
                            const vector::data_chunk_t& probe,
                            uint64_t row) {
        for (size_t column = 0; column < probe.column_count(); column++) {
            chunk.data[column] = broadcast_row(resource, probe.data[column], row);
        }
    }

    void compute_join_layout(const vector::data_chunk_t& left_front,
                             const vector::data_chunk_t& right_front,
                             bool swapped,
                             std::pmr::vector<types::complex_logical_type>& res_types,
                             std::vector<size_t>& indices_left,
                             std::vector<size_t>& indices_right) {
        auto left_types = left_front.types();   // probe types
        auto right_types = right_front.types(); // build types
        const size_t left_col_count = left_front.column_count();
        const size_t right_col_count = right_front.column_count();

        indices_left.clear();
        indices_right.clear();
        indices_left.reserve(left_col_count);
        indices_right.reserve(right_col_count);

        if (!swapped) {
            // Probe == logical-left, build == logical-right.
            res_types = left_types;
            for (size_t i = 0; i < left_col_count; ++i) {
                indices_left.emplace_back(i);
            }
            for (size_t i = 0; i < right_col_count; ++i) {
                indices_right.emplace_back(left_col_count + i);
                res_types.push_back(right_types[i]);
            }
        } else {
            // Build == logical-left, probe == logical-right.
            res_types = right_types;
            for (size_t i = 0; i < right_col_count; ++i) {
                indices_right.emplace_back(i);
            }
            for (size_t i = 0; i < left_col_count; ++i) {
                indices_left.emplace_back(right_col_count + i);
                res_types.push_back(left_types[i]);
            }
        }
    }

    void compute_active_indices(const vector::data_chunk_t& left_front,
                                const vector::data_chunk_t& right_front,
                                const std::vector<size_t>& indices_left,
                                const std::vector<size_t>& indices_right,
                                std::vector<size_t>* active_indices) {
        active_indices->clear();
        for (size_t column = 0; column < left_front.column_count(); ++column) {
            if (!is_placeholder(left_front.data[column])) {
                active_indices->push_back(indices_left[column]);
            }
        }
        for (size_t column = 0; column < right_front.column_count(); ++column) {
            if (!is_placeholder(right_front.data[column])) {
                active_indices->push_back(indices_right[column]);
            }
        }
    }

    join_builder::join_builder(std::pmr::memory_resource* resource,
                               const std::pmr::vector<types::complex_logical_type>& out_types,
                               const std::vector<size_t>& indices_left,
                               const std::vector<size_t>& indices_right,
                               const std::vector<size_t>& active_indices)
        : resource_(resource)
        , out_types_(out_types)
        , indices_left_(indices_left)
        , indices_right_(indices_right)
        , active_indices_(active_indices)
        , pending_(resource)
        , buf_left_rows_(resource)
        , buf_right_chunks_(resource)
        , buf_right_rows_(resource)
        , entry_group_(resource)
        , group_chunk_(resource)
        , group_start_(resource)
        , order_(resource)
        , cursor_(resource)
        , slot_live_(resource)
        , indexing_(resource, uint64_t{vector::DEFAULT_VECTOR_CAPACITY}) {}

    void
    join_builder::emit_matched(const vector::data_chunk_t& L, uint64_t li, const vector::data_chunk_t& R, uint64_t rj) {
        ensure_space_();
        left_chunk_ = &L;
        buf_left_rows_.push_back(li);
        buf_right_chunks_.push_back(&R);
        buf_right_rows_.push_back(rj);
        ++filled_;
    }

    void join_builder::emit_left_only(const vector::data_chunk_t& L, uint64_t li) {
        ensure_space_();
        left_chunk_ = &L;
        buf_left_rows_.push_back(li);
        buf_right_chunks_.push_back(nullptr);
        buf_right_rows_.push_back(0);
        ++filled_;
    }

    // R row with NULLs on all left-side output columns.
    void join_builder::emit_right_only(const vector::data_chunk_t& R, uint64_t rj) {
        ensure_space_();
        buf_left_rows_.push_back(0);
        buf_right_chunks_.push_back(&R);
        buf_right_rows_.push_back(rj);
        ++filled_;
    }
    void join_builder::gather() {
        uint64_t pos = 0;
        while (pos < filled_) {
            ensure_pending_();
            const uint64_t room = vector::DEFAULT_VECTOR_CAPACITY - pending_rows_;
            const uint64_t take = std::min(room, filled_ - pos);
            gather_range_(pos, pos + take, pending_rows_);
            pending_rows_ += take;
            pos += take;
            if (pending_rows_ == vector::DEFAULT_VECTOR_CAPACITY) {
                emit_pending_();
            }
        }
        filled_ = 0;
        left_chunk_ = nullptr;
        buf_left_rows_.clear();
        buf_right_chunks_.clear();
        buf_right_rows_.clear();
    }

    void join_builder::flush() {
        gather();
        emit_pending_();
    }

    void join_builder::reset() noexcept {
        pending_.clear();
        pending_rows_ = 0;
        filled_ = 0;
        left_chunk_ = nullptr;
        buf_left_rows_.clear();
        buf_right_chunks_.clear();
        buf_right_rows_.clear();
        slots_ready_ = false;
    }

    // Bounds the index buffers. Mid-batch, so the sources are still alive.
    void join_builder::ensure_space_() {
        if (filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
            gather();
        }
    }

    void join_builder::ensure_slots_() {
        if (slots_ready_) {
            return;
        }
        slot_live_.assign(out_types_.size(), 0);
        for (size_t slot : active_indices_) {
            slot_live_[slot] = 1;
        }
        slots_ready_ = true;
    }

    void join_builder::ensure_pending_() {
        if (!pending_.empty()) {
            return;
        }
        ensure_slots_();
        pending_.emplace_back(resource_, out_types_, active_indices_, vector::DEFAULT_VECTOR_CAPACITY);
        pending_rows_ = 0;
    }

    void join_builder::emit_pending_() {
        if (pending_.empty()) {
            return;
        }
        if (pending_rows_ == 0) {
            pending_.clear();
            return;
        }
        pending_.back().set_cardinality(pending_rows_);
        out_->push_back(std::move(pending_.back()));
        pending_.clear();
        pending_rows_ = 0;
    }

    void join_builder::gather_range_(uint64_t begin, uint64_t end, uint64_t dst_offset) {
        auto& dst = pending_.back();
        const uint64_t count = end - begin;

        // Group by build-chunk pointer. A linear scan, not a hash map: the group
        // count is the number of build chunks a window touches (typically one), so
        // the map this replaced allocated a bucket array and a node per group to
        // answer a question a few comparisons settle.
        group_chunk_.clear();
        entry_group_.clear();
        entry_group_.reserve(count);
        for (uint64_t k = begin; k < end; ++k) {
            const auto* right_chunk = buf_right_chunks_[k];
            uint32_t group = 0;
            for (; group < group_chunk_.size(); ++group) {
                if (group_chunk_[group] == right_chunk) {
                    break;
                }
            }
            if (group == group_chunk_.size()) {
                group_chunk_.push_back(right_chunk);
            }
            entry_group_.push_back(group);
        }
        const uint32_t group_count = static_cast<uint32_t>(group_chunk_.size());

        // Counting sort into `order_` (window slot → buffer offset from `begin`),
        // grouped by build chunk so each right-column gather is one contiguous run.
        group_start_.assign(group_count + 1, 0);
        for (uint64_t k = 0; k < count; ++k) {
            ++group_start_[entry_group_[k] + 1];
        }
        for (uint32_t group = 0; group < group_count; ++group) {
            group_start_[group + 1] += group_start_[group];
        }
        order_.assign(count, 0);
        cursor_.assign(group_start_.begin(), group_start_.end());
        for (uint64_t k = 0; k < count; ++k) {
            order_[cursor_[entry_group_[k]]++] = k;
        }

        // --- Left (probe) columns. Single source, so one gather over the window. ---
        if (left_chunk_ != nullptr) {
            for (uint64_t slot = 0; slot < count; ++slot) {
                indexing_.set_index(slot, buf_left_rows_[begin + order_[slot]]);
            }
            for (size_t column = 0; column < left_chunk_->column_count(); ++column) {
                if (is_placeholder(left_chunk_->data[column])) {
                    continue;
                }
                const size_t out_slot = indices_left_[column];
                if (!slot_live_[out_slot]) {
                    continue;
                }
                vector::vector_ops::copy(left_chunk_->data[column],
                                         dst.data[out_slot],
                                         indexing_,
                                         count,
                                         0,
                                         dst_offset,
                                         count);
            }
        } else {
            // Drain mode: every row is right-only → NULL-pad the left columns.
            for (size_t column = 0; column < indices_left_.size(); ++column) {
                const size_t out_slot = indices_left_[column];
                if (!slot_live_[out_slot]) {
                    continue;
                }
                for (uint64_t slot = 0; slot < count; ++slot) {
                    dst.data[out_slot].validity().set_invalid(dst_offset + slot);
                }
            }
        }

        // --- Right (build) columns, one gather per (build-chunk, column). ---
        for (uint32_t group = 0; group < group_count; ++group) {
            const auto* right_chunk = group_chunk_[group];
            const uint64_t start = group_start_[group];
            const uint64_t stop = group_start_[group + 1];
            const uint64_t run = stop - start;
            if (run == 0) {
                continue;
            }
            if (right_chunk == nullptr) {
                // left-only rows → NULL-pad all right columns over the run.
                for (size_t column = 0; column < indices_right_.size(); ++column) {
                    const size_t out_slot = indices_right_[column];
                    if (!slot_live_[out_slot]) {
                        continue;
                    }
                    for (uint64_t slot = start; slot < stop; ++slot) {
                        dst.data[out_slot].validity().set_invalid(dst_offset + slot);
                    }
                }
                continue;
            }
            for (uint64_t i = 0; i < run; ++i) {
                indexing_.set_index(i, buf_right_rows_[begin + order_[start + i]]);
            }
            for (size_t column = 0; column < right_chunk->column_count(); ++column) {
                if (is_placeholder(right_chunk->data[column])) {
                    continue;
                }
                const size_t out_slot = indices_right_[column];
                if (!slot_live_[out_slot]) {
                    continue;
                }
                vector::vector_ops::copy(right_chunk->data[column],
                                         dst.data[out_slot],
                                         indexing_,
                                         run,
                                         0,
                                         dst_offset + start,
                                         run);
            }
        }
    }

    eager_join_builder::eager_join_builder(std::pmr::memory_resource* resource,
                                           const std::pmr::vector<types::complex_logical_type>& out_types,
                                           const std::vector<size_t>& indices_left,
                                           const std::vector<size_t>& indices_right,
                                           chunks_vector_t& out_chunks)
        : resource_(resource)
        , out_types_(out_types)
        , indices_left_(indices_left)
        , indices_right_(indices_right)
        , out_chunks_(out_chunks)
        , cur_(resource, out_types, vector::DEFAULT_VECTOR_CAPACITY)
        , idx1_(resource, uint64_t{1}) {}

    void eager_join_builder::emit_matched(const vector::data_chunk_t& L,
                                          uint64_t li,
                                          const vector::data_chunk_t& R,
                                          uint64_t rj) {
        copy_row_(L, li, indices_left_);
        copy_row_(R, rj, indices_right_);
        advance_();
    }

    void eager_join_builder::emit_left_only(const vector::data_chunk_t& L, uint64_t li) {
        copy_row_(L, li, indices_left_);
        null_side_(indices_right_);
        advance_();
    }

    void eager_join_builder::flush() {
        if (filled_ == 0) {
            return;
        }
        cur_.set_cardinality(filled_);
        out_chunks_.emplace_back(std::move(cur_));
        cur_ = vector::data_chunk_t(resource_, out_types_, vector::DEFAULT_VECTOR_CAPACITY);
        filled_ = 0;
    }
    void
    eager_join_builder::copy_row_(const vector::data_chunk_t& src, uint64_t srow, const std::vector<size_t>& slots) {
        idx1_.set_index(0, srow);
        const size_t cols = src.column_count();
        for (size_t c = 0; c < cols; ++c) {
            if (is_placeholder(src.data[c])) {
                continue;
            }
            vector::vector_ops::copy(src.data[c], cur_.data[slots[c]], idx1_, 1, 0, filled_, 1);
        }
    }

    void eager_join_builder::null_side_(const std::vector<size_t>& slots) {
        for (size_t c = 0; c < slots.size(); ++c) {
            cur_.data[slots[c]].validity().set_invalid(filled_);
        }
    }

    void eager_join_builder::advance_() {
        if (++filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
            flush();
        }
    }

} // namespace components::operators::join_detail

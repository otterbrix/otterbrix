#include "block.hpp"
#include <absl/crc/crc32c.h>
#include <algorithm>
#include <cassert>
#include <core/buffer.hpp>
#include <cstring>

namespace core::b_plus_tree {
    size_t SECTOR_SIZE = 4096;

    block_t::iterator::iterator(const block_t* block, block_t::metadata* metadata)
        : block_(block)
        , metadata_(metadata) {
        rebuild_data();
    }

    block_t::iterator::iterator(const iterator& other)
        : block_(other.block_)
        , metadata_(other.metadata_) {
        rebuild_data();
    }

    block_t::iterator::iterator(iterator&& other) noexcept
        : block_(other.block_)
        , metadata_(other.metadata_)
        , data_(std::move(other.data_)) {}

    void block_t::iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_metadata_) {
            data_.item.data = block_->internal_buffer_ + metadata_->offset;
            data_.item.size = metadata_->size;
            data_.index = metadata_->index;
        } else {
            data_.item.data = nullptr;
            data_.item.size = 0;
            data_.index = index_t();
        }
    }

    block_t::r_iterator::r_iterator(const block_t* block, block_t::metadata* metadata)
        : block_(block)
        , metadata_(metadata) {
        rebuild_data();
    }

    block_t::r_iterator::r_iterator(const r_iterator& other)
        : block_(other.block_)
        , metadata_(other.metadata_) {
        rebuild_data();
    }

    block_t::r_iterator::r_iterator(r_iterator&& other) noexcept
        : block_(other.block_)
        , metadata_(other.metadata_)
        , data_(std::move(other.data_)) {}

    void block_t::r_iterator::rebuild_data() {
        if (metadata_ < block_->end_ && metadata_ >= block_->last_metadata_) {
            data_.item.data = block_->internal_buffer_ + metadata_->offset;
            data_.item.size = metadata_->size;
            data_.index = metadata_->index;
        } else {
            data_.item.data = nullptr;
            data_.item.size = 0;
            data_.index = index_t();
        }
    }

    block_t::block_t(std::pmr::memory_resource* resource, index_t (*func)(const item_data&))
        : resource_(resource)
        , key_func_(func) {}

    block_t::~block_t() {
        if (!internal_buffer_ || !is_valid_) {
            return;
        }
        resource_->deallocate(internal_buffer_, full_size_);
    }

    void block_t::initialize(uint32_t size) {
        assert(!is_valid_ && "block was already initialized!");
        assert(size < MAX_BLOCK_SIZE && "block cannot handle this size!");
        assert((size & (SECTOR_SIZE - 1)) == 0 && "block size should be a multiple of SECTOR_SIZE!");
        internal_buffer_ = static_cast<data_ptr_t>(resource_->allocate(size));
        full_size_ = size;
        checksum_ = reinterpret_cast<uint32_t*>(internal_buffer_);
        count_ = checksum_ + 1;
        *count_ = 0;
        unique_indices_count_ = count_ + 1;
        *unique_indices_count_ = 0;
        end_ = reinterpret_cast<metadata*>(internal_buffer_ + size);

        restore_block();
    }

    size_t block_t::available_memory() const { return available_memory_; }

    size_t block_t::occupied_memory() const { return full_size_ - available_memory_ - header_size; }

    bool block_t::is_memory_available(size_t request_size) const {
        return request_size + metadata_size <= available_memory_;
    }

    bool block_t::is_empty() const { return last_metadata_ == end_; }

    bool block_t::is_valid() const { return is_valid_; }

    bool block_t::append(data_ptr_t data, uint32_t size) noexcept { return append({data, size}); }

    bool block_t::append(item_data item) noexcept {
        index_t index = key_func_(item);
        return append(index, item);
    }

    bool block_t::append(const index_t& index, item_data item) noexcept {
        assert(is_valid_ && "block is not initialized!");
        assert(item.size != 0 && "can not append an empty buffer!");
        if (!is_memory_available(item.size)) {
            return false;
        }

        auto range = find_index_range_(index);
        for (metadata* it = range.begin; it != range.end; it++) {
            if (it->size == item.size && std::memcmp(item.data, internal_buffer_ + it->offset, item.size) == 0) {
                return false;
            }
        }

        (*count_)++;
        if (range.begin == range.end) {
            (*unique_indices_count_)++;
        }

        std::memmove(last_metadata_ - 1,
                     last_metadata_,
                     static_cast<size_t>(range.begin - last_metadata_) * metadata_size);
        range.begin--;
        last_metadata_--;
        range.begin->offset = static_cast<uint32_t>(buffer_ - internal_buffer_);
        range.begin->size = item.size;

        std::memcpy(buffer_, item.data, item.size);
        buffer_ += item.size;
        available_memory_ -= item.size + metadata_size;

        if (index.type() == components::types::physical_type::STRING) {
            auto sv = index.value<components::types::physical_type::STRING>();
            range.begin->index = index_t(internal_buffer_ + range.begin->offset + (sv.data() - item.data),
                                         static_cast<uint32_t>(sv.size()));
        } else {
            range.begin->index = index;
        }

        return true;
    }

    bool block_t::remove(data_ptr_t data, uint32_t size) noexcept { return remove({data, size}); }

    bool block_t::remove(item_data item) noexcept {
        index_t index = key_func_(item);
        return remove(index, item);
    }

    bool block_t::remove(const index_t& index, item_data item) noexcept {
        assert(is_valid_ && "block is not initialized!");

        auto range = find_index_range_(index);
        if (range.begin == range.end) {
            return false;
        }

        (*count_)--;
        if (range.begin + 1 == range.end) {
            (*unique_indices_count_)--;
        }
        for (metadata* it = range.begin; it != range.end; it++) {
            if (it->size == item.size && std::memcmp(item.data, internal_buffer_ + it->offset, item.size) == 0) {
                uint32_t chunk_size = it->size;
                data_ptr_t chunk_start = internal_buffer_ + it->offset;
                data_ptr_t next_chunk_start = chunk_start + it->size;
                std::memmove(chunk_start, next_chunk_start, static_cast<size_t>(buffer_ - next_chunk_start));
                buffer_ -= it->size;
                available_memory_ += it->size + metadata_size;
                std::memmove(last_metadata_ + 1,
                             last_metadata_,
                             static_cast<size_t>(it - last_metadata_) * metadata_size);
                last_metadata_++;

                auto offset = static_cast<uint32_t>(chunk_start - internal_buffer_);
                for (auto it_ = last_metadata_; it_ != end_; it_++) {
                    if (it_->offset > offset) {
                        it_->offset -= chunk_size;
                        if (it_->index.type() == components::types::physical_type::STRING) {
                            auto sv = it_->index.value<components::types::physical_type::STRING>();
                            it_->index = index_t(sv.data() - chunk_size, static_cast<uint32_t>(sv.size()));
                        }
                    }
                }
            }
        }

        return true;
    }

    bool block_t::remove_index(const index_t& index) {
        assert(is_valid_ && "block is not initialized!");

        auto range = find_index_range_(index);
        if (range.begin == range.end) {
            return false;
        }

        auto batch_size = static_cast<uint32_t>(range.end - range.begin);

        *count_ -= batch_size;
        (*unique_indices_count_)--;

        remove_range_(range);

        return true;
    }

    bool block_t::contains_index(const index_t& index) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);
        return range.begin != range.end;
    }

    bool block_t::contains(item_data item) const {
        index_t index = key_func_(item);
        return contains(index, item);
    }

    bool block_t::contains(const index_t& index, item_data item) const {
        assert(is_valid_ && "block is not initialized!");

        auto range = find_index_range_(index);

        for (metadata* it = range.begin; it != range.end; it++) {
            if (it->size == item.size && std::memcmp(item.data, internal_buffer_ + it->offset, item.size) == 0) {
                return true;
            }
        }

        return false;
    }

    uint32_t block_t::item_count(const index_t& index) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);

        return static_cast<uint32_t>(range.end - range.begin);
    }

    block_t::item_data block_t::get_item(const index_t& index, uint32_t position) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);

        if (position >= range.end - range.begin) {
            return {nullptr, 0};
        }

        return {internal_buffer_ + (range.begin + position)->offset, (range.begin + position)->size};
    }

    void block_t::get_items(std::vector<item_data>& items, const index_t& index) const {
        assert(is_valid_ && "block is not initialized!");
        auto range = find_index_range_(index);

        items.reserve(static_cast<size_t>(items.size() + range.end - range.begin));
        for (metadata* it = range.begin; it != range.end; it++) {
            items.emplace_back(internal_buffer_ + it->offset, it->size);
        }
    }

    uint32_t block_t::count() const {
        assert(is_valid_ && "block is not initialized!");
        return *count_;
    }

    uint32_t block_t::unique_indices_count() const {
        assert(is_valid_ && "block is not initialized!");
        return *unique_indices_count_;
    }

    block_t::index_t block_t::min_index() const {
        assert(is_valid_ && "block is not initialized!");
        if (last_metadata_ == end_) {
            return std::numeric_limits<index_t>::min();
        }

        return (end_ - 1)->index;
    }
    block_t::index_t block_t::max_index() const {
        assert(is_valid_ && "block is not initialized!");
        if (last_metadata_ == end_) {
            return std::numeric_limits<index_t>::max();
        }

        return last_metadata_->index;
    }

    data_ptr_t block_t::internal_buffer() {
        assert(is_valid_ && "block is not initialized!");
        return internal_buffer_;
    }

    uint32_t block_t::block_size() const { return full_size_; }

    void block_t::reset() {
        *count_ = 0;
        *unique_indices_count_ = 0;
        restore_block();
    }

    void block_t::restore_block() {
        last_metadata_ = end_ - *count_;
        buffer_ = internal_buffer_ + header_size;
        for (auto it = last_metadata_; it < end_; it++) {
            buffer_ += it->size;
            if (it->index.type() == components::types::physical_type::STRING) {
                it->index = key_func_({internal_buffer_ + it->offset, it->size});
            }
        }
        available_memory_ = static_cast<uint32_t>(reinterpret_cast<data_ptr_t>(last_metadata_) - buffer_);
        is_valid_ = true;
    }

    void block_t::resize(uint32_t new_size) {
        if (!is_valid_) {
            return;
        }

        assert((new_size & (SECTOR_SIZE - 1)) == 0 && "block size should be a multiple of SECTOR_SIZE!");
        assert(occupied_memory() < new_size && "block data won't fit in new size");
        auto new_buffer = static_cast<data_ptr_t>(resource_->allocate(new_size));
        std::memcpy(new_buffer, internal_buffer_, static_cast<size_t>(buffer_ - internal_buffer_));
        std::memcpy(new_buffer + new_size - *unique_indices_count_ * metadata_size,
                    last_metadata_,
                    *unique_indices_count_ * metadata_size);
        auto* new_last_metadata = reinterpret_cast<metadata*>(
            new_buffer + full_size_ - (reinterpret_cast<data_ptr_t>(last_metadata_) - internal_buffer_));
        for (metadata* it = last_metadata_; reinterpret_cast<data_ptr_t>(new_last_metadata) < new_buffer + new_size;
             new_last_metadata++, it++) {
            if (it->index.type() == components::types::physical_type::STRING) {
                auto sv = it->index.value<components::types::physical_type::STRING>();
                new_last_metadata->index =
                    index_t(new_buffer + (sv.data() - internal_buffer_), static_cast<uint32_t>(sv.size()));
            }
        }

        auto buffer_offset = static_cast<size_t>(buffer_ - internal_buffer_);
        resource_->deallocate(internal_buffer_, full_size_);
        internal_buffer_ = new_buffer;
        buffer_ = internal_buffer_ + buffer_offset;
        available_memory_ = new_size - full_size_;
        full_size_ = new_size;
        checksum_ = reinterpret_cast<uint32_t*>(internal_buffer_);
        count_ = checksum_ + 1;
        unique_indices_count_ = count_ + 1;
        end_ = reinterpret_cast<metadata*>(internal_buffer_ + new_size);
        last_metadata_ = end_ - *unique_indices_count_;
    }

    // TODO: try to split into smaller blocks if current size is greater then DEFAULT_BLOCK_SIZE
    std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> block_t::split_append(const index_t& index,
                                                                                        item_data item) {
        // if append is possible, split in half, append to required part
        assert(is_valid_ && "block is not initialized!");
        assert(available_memory_ + (buffer_ - internal_buffer_) <= full_size_);

        std::unique_ptr<block_t> splited_block;

        // calculate mid point (size wise)
        size_t accumulated_size = 0;
        metadata* split_item = last_metadata_;
        for (; split_item < end_ && accumulated_size < full_size_ / 2; split_item++) {
            accumulated_size += split_item->size + metadata_size;
        }
        if (split_item != end_) {
            accumulated_size += split_item->size + metadata_size;
        }

        bool append_possible;
        bool this_block = false;
        index_t split_index = split_item == end_ ? std::numeric_limits<index_t>::min() : split_item->index;
        if (split_item == end_ && (index > min_index() && index < max_index())) {
            append_possible = false;
        } else if (split_index < index) {
            // will go into second block
            append_possible = full_size_ - metadata_size > item.size + accumulated_size;
            this_block = false;
        } else {
            // will go into first block
            append_possible = available_memory_ + accumulated_size > item.size + metadata_size;
            this_block = true;
        }

        if (append_possible) {
            splited_block = split(static_cast<uint32_t>(split_item - last_metadata_));

            if (this_block) {
                append(index, item);
            } else {
                splited_block->append(index, item);
            }

            return {std::move(splited_block), nullptr};
        }

        split_item = find_index_range_(index).begin;
        if (split_item == last_metadata_) {
            splited_block =
                create_initialize(resource_, key_func_, align_to_block_size(item.size + header_size + metadata_size));
            splited_block->append(index, item); // always true
            return {std::move(splited_block), nullptr};
        } else {
            splited_block = split(static_cast<uint32_t>(split_item - last_metadata_));

            if (append(index, item)) {
                return {std::move(splited_block), nullptr};
            }
            if (splited_block->append(index, item)) {
                return {std::move(splited_block), nullptr};
            }

            std::unique_ptr<block_t> middle_block =
                create_initialize(resource_, key_func_, align_to_block_size(item.size + header_size + metadata_size));
            middle_block->append(index, item); // always true
            return {std::move(middle_block), std::move(splited_block)};
        }
    }

    std::unique_ptr<block_t> block_t::split(uint32_t count) {
        assert(is_valid_ && "block is not initialized!");
        assert(count <= *count_);

        std::unique_ptr<block_t> splited_block = create_initialize(resource_, key_func_, full_size_);
        if (count == 0) {
            return splited_block;
        }
        if (count == *count_) {
            // faster to swap pointers then to move all documents
            std::swap(internal_buffer_, splited_block->internal_buffer_);
            std::swap(buffer_, splited_block->buffer_);
            std::swap(last_metadata_, splited_block->last_metadata_);
            std::swap(end_, splited_block->end_);
            std::swap(count_, splited_block->count_);
            std::swap(unique_indices_count_, splited_block->unique_indices_count_);
            std::swap(checksum_, splited_block->checksum_);
            std::swap(available_memory_, splited_block->available_memory_);
            return splited_block;
        }

        metadata* split_item = last_metadata_ + count;

        std::vector<index_t> moved_indices;
        moved_indices.reserve(count);
        for (auto split_meta = split_item - 1; split_meta >= last_metadata_; split_meta--) {
            auto item = metadata_to_item_data_(split_meta);
            splited_block->append(split_meta->index, item);
            moved_indices.emplace_back(split_meta->index);
        }
        moved_indices.erase(std::unique(moved_indices.begin(), moved_indices.end()), moved_indices.end());

        *count_ -= count;
        *unique_indices_count_ -=
            static_cast<uint32_t>(moved_indices.size() - (split_item->index == moved_indices.front()));

        remove_range_({last_metadata_, split_item});

        return splited_block;
    }

    std::unique_ptr<block_t> block_t::split_uniques(uint32_t count) {
        assert(is_valid_ && "block is not initialized!");
        assert(count <= *unique_indices_count_);

        std::unique_ptr<block_t> splited_block = create_initialize(resource_, key_func_, full_size_);
        if (count == 0) {
            return splited_block;
        }
        if (count == *unique_indices_count_) {
            // faster to swap pointers then to move all documents
            std::swap(internal_buffer_, splited_block->internal_buffer_);
            std::swap(buffer_, splited_block->buffer_);
            std::swap(last_metadata_, splited_block->last_metadata_);
            std::swap(end_, splited_block->end_);
            std::swap(count_, splited_block->count_);
            std::swap(unique_indices_count_, splited_block->unique_indices_count_);
            std::swap(checksum_, splited_block->checksum_);
            std::swap(available_memory_, splited_block->available_memory_);
            return splited_block;
        }

        metadata* split_item = last_metadata_;
        index_t prev_index = last_metadata_->index;
        std::vector<index_t> moved_indices;
        moved_indices.reserve(count);
        for (size_t i = 1, index_count = 1; i < static_cast<size_t>(end_ - last_metadata_) && index_count <= count;
             i++) {
            index_t index = (last_metadata_ + i)->index;
            if (index != prev_index) {
                prev_index = index;
                index_count++;
                moved_indices.emplace_back(index);
            }
            splited_block->append(metadata_to_item_data_(split_item++));
            (*count_)--;
        }
        *unique_indices_count_ -= count;

        remove_range_({last_metadata_, split_item});
        return splited_block;
    }

    void block_t::merge(std::unique_ptr<block_t>&& other) {
        assert(is_valid_ && "other is not initialized!");
        assert(other->is_valid_ && "other block is not initialized!");
        assert(available_memory_ >= other->occupied_memory());
        assert(available_memory_ + (buffer_ - internal_buffer_) <= full_size_);
        if (other->count() == 0) {
            return;
        }

        if (other->min_index() >= max_index()) {
            bool overlap = other->min_index() == max_index();
            uint32_t delta_offset = static_cast<uint32_t>(buffer_ - internal_buffer_) - header_size;
            uint32_t additional_offset = static_cast<uint32_t>(other->buffer_ - other->internal_buffer_) - header_size;
            std::memcpy(buffer_,
                        other->internal_buffer_ + header_size,
                        static_cast<size_t>(other->buffer_ - other->internal_buffer_) - header_size);
            buffer_ += additional_offset;
            std::memcpy(last_metadata_ - other->count(), other->last_metadata_, metadata_size * other->count());
            for (size_t i = 0; i < other->count(); i++) {
                uint32_t old_offset = (--last_metadata_)->offset;
                last_metadata_->offset += delta_offset;
                if (last_metadata_->index.type() == components::types::physical_type::STRING) {
                    auto sv = last_metadata_->index.value<components::types::physical_type::STRING>();
                    last_metadata_->index = index_t(internal_buffer_ + last_metadata_->offset +
                                                        (sv.data() - (other->internal_buffer_ + old_offset)),
                                                    static_cast<uint32_t>(sv.size()));
                }
            }
            available_memory_ -= additional_offset + metadata_size * other->count();
            *count_ += other->count();
            *unique_indices_count_ += other->unique_indices_count() - overlap;
        } else if (other->max_index() <= min_index()) {
            bool overlap = other->max_index() == min_index();
            uint32_t delta_offset = static_cast<uint32_t>(buffer_ - internal_buffer_) - header_size;
            uint32_t additional_offset = static_cast<uint32_t>(other->buffer_ - other->internal_buffer_) - header_size;
            std::memcpy(buffer_, other->internal_buffer_ + header_size, additional_offset);
            buffer_ += additional_offset;
            std::memcpy(last_metadata_ - other->count(), last_metadata_, metadata_size * other->count());
            for (metadata* it = last_metadata_ - other->count(); it != end_ - other->count(); it++) {
                if (it->index.type() == components::types::physical_type::STRING) {
                    auto sv = it->index.value<components::types::physical_type::STRING>();
                    it->index = index_t(sv.data() - metadata_size * other->count(), static_cast<uint32_t>(sv.size()));
                }
            }
            std::memcpy(last_metadata_, other->last_metadata_, metadata_size * other->count());
            for (metadata* it = last_metadata_; it < end_; it++) {
                uint32_t old_offset = it->offset;
                it->offset += delta_offset;
                if (it->index.type() == components::types::physical_type::STRING) {
                    auto sv = it->index.value<components::types::physical_type::STRING>();
                    it->index =
                        index_t(internal_buffer_ + it->offset + (sv.data() - (other->internal_buffer_ + old_offset)),
                                static_cast<uint32_t>(sv.size()));
                }
            }
            last_metadata_ -= other->count();
            available_memory_ -= additional_offset + metadata_size * other->count();
            *count_ += other->count();
            *unique_indices_count_ += other->unique_indices_count() - overlap;
        } else {
            // there is range overlapping and cannot be copied trivially
            for (metadata* it = other->end_ - 1; it >= other->last_metadata_; it--) {
                append(it->index, metadata_to_item_data_(it));
            }
        }
    }

    void block_t::recalculate_checksum() { *checksum_ = calculate_checksum_(); }

    bool block_t::varify_checksum() const { return *checksum_ == calculate_checksum_(); }

    block_t::metadata_range block_t::find_index_range_(const index_t& index) const {
        if (!is_valid_) {
            return {end_, end_};
        }

        metadata_range result;
        result.begin = std::lower_bound(last_metadata_, end_, index, [](const metadata& meta, const index_t& index) {
            return meta.index > index;
        });
        result.end = std::lower_bound(result.begin, end_, index, [](const metadata& meta, const index_t& index) {
            return meta.index >= index;
        });
        return result;
    }

    void block_t::remove_range_(metadata_range range) {
        assert(range.begin != range.end);
        auto batch_size = static_cast<uint32_t>(range.end - range.begin);

        // similar to gap_tracker for segment tree but with limited and inversed funcionality
        std::vector<std::pair<uint32_t, uint32_t>> untouched_spaces;
        untouched_spaces.emplace_back(header_size, data_ptr_t(buffer_) - internal_buffer_);

        for (metadata* it = range.begin; it != range.end; it++) {
            auto untouched_gap = untouched_spaces.begin();
            for (; untouched_gap < untouched_spaces.end(); ++untouched_gap) {
                if (untouched_gap->first > it->offset) {
                    break;
                }
            }
            assert(untouched_gap != untouched_spaces.begin() && "required untouched_gap is out of tracker scope");
            auto prev = untouched_gap - 1;
            std::pair<uint32_t, uint32_t> next;
            next.first = it->offset + it->size;
            next.second = prev->second + prev->first - it->offset - it->size;
            prev->second = it->offset - prev->first;
            if (next.second != 0) {
                untouched_spaces.emplace(untouched_gap, std::move(next));
            }
            available_memory_ += it->size;
        }

        for (size_t i = 0; i + 1 < untouched_spaces.size();) {
            if (untouched_spaces[i].first + untouched_spaces[i].second == untouched_spaces[i + 1].first) {
                untouched_spaces[i].second += untouched_spaces[i + 1].second;
                untouched_spaces.erase(untouched_spaces.begin() + static_cast<int64_t>(i + 1));
            } else {
                i++;
            }
        }
        std::memmove(last_metadata_ + batch_size,
                     last_metadata_,
                     static_cast<size_t>(range.begin - last_metadata_) * metadata_size);
        available_memory_ += batch_size * metadata_size;
        last_metadata_ += batch_size;

        uint32_t tracked_offset = 0;
        uint32_t required_offset = 0;
        auto next_it = untouched_spaces.begin() + 1;
        for (auto it = untouched_spaces.begin(); next_it != untouched_spaces.end(); ++it, ++next_it) {
            tracked_offset = it->first + it->second - required_offset;
            required_offset += next_it->first - (it->first + it->second);
            std::memmove(internal_buffer_ + tracked_offset, internal_buffer_ + next_it->first, next_it->second);
            for (metadata* meta = last_metadata_; meta != end_; meta++) {
                if (meta->offset >= next_it->first && meta->offset < next_it->first + next_it->second) {
                    meta->offset -= required_offset;
                    if (meta->index.type() == components::types::physical_type::STRING) {
                        auto sv = meta->index.value<components::types::physical_type::STRING>();
                        meta->index = index_t(sv.data() - required_offset, static_cast<uint32_t>(sv.size()));
                    }
                }
            }
        }
        buffer_ -= required_offset;
    }

    block_t::item_data block_t::metadata_to_item_data_(const metadata* meta) const {
        return {internal_buffer_ + meta->offset, meta->size};
    }

    uint32_t block_t::calculate_checksum_() const {
        assert(is_valid_ && "block is not initialized!");
        auto crc_buffer = reinterpret_cast<data_ptr_t>(count_);
        auto size = static_cast<size_t>(full_size_ - (crc_buffer - internal_buffer_));
        return static_cast<uint32_t>(absl::ComputeCrc32c({reinterpret_cast<const char*>(crc_buffer), size}));
    }

} // namespace core::b_plus_tree

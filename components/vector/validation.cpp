#include "validation.hpp"
#include <cassert>
#include <cstring>

namespace components::vector {

    validity_data_t::validity_data_t(std::pmr::memory_resource* resource, uint64_t size)
        : data_(new (resource->allocate(sizeof(uint64_t) * size, alignof(uint64_t))) uint64_t[size],
                core::pmr::array_deleter_t(resource, size, alignof(uint64_t))) {
        for (uint64_t i = 0; i < size; i++) {
            data_[i] = MAX_ENTRY;
        }
    }

    validity_data_t::validity_data_t(std::pmr::memory_resource* resource, uint64_t* mask, uint64_t size)
        : data_(new (resource->allocate(sizeof(uint64_t) * size, alignof(uint64_t))) uint64_t[size],
                core::pmr::array_deleter_t(resource, size, alignof(uint64_t))) {
        if (mask) {
            for (uint64_t i = 0; i < size; i++) {
                data_[i] = mask[i];
            }
        }
    }

    validity_mask_t::validity_mask_t(std::pmr::memory_resource* resource, uint64_t size)
        : resource_(resource)
        , validity_data_(std::make_shared<validity_data_t>(resource, size))
        , validity_mask_(validity_data_->data())
        , count_(size) {}

    validity_mask_t::validity_mask_t(const validity_mask_t& other)
        : resource_(other.resource_)
        , count_(other.count_) {
        if (other.all_valid()) {
            validity_data_ = nullptr;
            validity_mask_ = nullptr;
        } else {
            validity_data_ = std::make_shared<validity_data_t>(resource_, other.validity_mask_, count_);
            validity_mask_ = validity_data_->data();
        }
    }

    validity_mask_t& validity_mask_t::operator=(const validity_mask_t& other) {
        assert(resource_ == other.resource_);
        count_ = other.count_;
        if (other.all_valid()) {
            validity_data_ = nullptr;
            validity_mask_ = nullptr;
        } else {
            validity_data_ = std::make_shared<validity_data_t>(resource_, other.validity_mask_, count_);
            validity_mask_ = validity_data_->data();
        }
        return *this;
    }

    validity_mask_t::validity_mask_t(validity_mask_t&& other) noexcept
        : resource_(other.resource_)
        , validity_data_(std::move(other.validity_data_))
        , validity_mask_(other.validity_mask_)
        , count_(other.count_) {
        other.validity_mask_ = nullptr;
    }

    validity_mask_t& validity_mask_t::operator=(validity_mask_t&& other) noexcept {
        assert(resource_ == other.resource_);
        validity_data_ = std::move(other.validity_data_);
        validity_mask_ = other.validity_mask_;
        other.validity_mask_ = nullptr;
        count_ = other.count_;
        return *this;
    }

    void validity_mask_t::set_invalid(uint64_t entry_idx, uint64_t idx_in_entry) {
        if (!validity_mask_) {
            resize(resource_, count_);
        }
        validity_mask_[entry_idx] &= ~(uint64_t(1) << uint64_t(idx_in_entry));
    }

    void validity_mask_t::set_invalid(uint64_t row_idx) {
        if (!validity_mask_) {
            resize(resource_, count_);
        }
        uint64_t entry_idx, idx_in_entry;
        entry_index(row_idx, entry_idx, idx_in_entry);
        set_invalid(entry_idx, idx_in_entry);
    }

    void validity_mask_t::set_valid(uint64_t row_idx) {
        if (!validity_mask_) {
            resize(resource_, count_);
        }
        validity_mask_[row_idx] = true;
    }

    void validity_mask_t::set_all_invalid(uint64_t count) {
        if (count == 0) {
            return;
        }
        if (!validity_mask_) {
            resize(resource_, count);
        }
        auto last_entry_index = validity_data_t::entry_count(count) - 1;
        for (uint64_t i = 0; i < last_entry_index; i++) {
            validity_mask_[i] = 0;
        }
        auto last_entry_bits = count % BITS_PER_VALUE;
        validity_mask_[last_entry_index] = (last_entry_bits == 0) ? 0 : validity_data_t::MAX_ENTRY << (last_entry_bits);
    }

    bool validity_mask_t::row_is_valid(uint64_t index) const {
        if (!validity_mask_) {
            return true;
        }
        uint64_t entry_idx, idx_in_entry;
        entry_index(index, entry_idx, idx_in_entry);
        auto entry = get_validity_entry(entry_idx);
        return entry & (uint64_t(1) << uint64_t(idx_in_entry));
    }

    uint64_t validity_mask_t::count_valid(uint64_t count) const {
        if (all_valid() || count == 0) {
            return count;
        }

        uint64_t valid = 0;
        const auto entry_count = validity_data_t::entry_count(count);
        for (uint64_t entry_idx = 0; entry_idx < entry_count;) {
            auto entry = get_validity_entry(entry_idx++);
            if (entry_idx == entry_count && count % BITS_PER_VALUE != 0) {
                uint64_t idx_in_entry;
                entry_index(count, entry_idx, idx_in_entry);
                for (uint64_t i = 0; i < idx_in_entry; ++i) {
                    valid += uint64_t(entry & uint64_t(1) << uint64_t(i));
                }
                break;
            }

            if (entry == validity_data_t::MAX_ENTRY) {
                valid += BITS_PER_VALUE;
                continue;
            }

            while (entry) {
                entry &= (entry - 1);
                ++valid;
            }
        }

        return valid;
    }

    void validity_mask_t::set(uint64_t row_index, bool valid) {
        if (valid) {
            if (!validity_mask_) {
                return;
            }
            uint64_t entry_idx, idx_in_entry;
            entry_index(row_index, entry_idx, idx_in_entry);
            validity_mask_[entry_idx] |= (uint64_t(1) << uint64_t(idx_in_entry));
        } else {
            if (!validity_mask_) {
                assert(row_index <= count_);
                validity_data_ = std::make_unique<validity_data_t>(resource(), count_);
                validity_mask_ = validity_data_->data();
            }
            uint64_t entry_idx, idx_in_entry;
            entry_index(row_index, entry_idx, idx_in_entry);
            validity_mask_[entry_idx] &= ~(uint64_t(1) << uint64_t(idx_in_entry));
        }
    }

    void validity_mask_t::slice(const validity_mask_t& other, uint64_t offset, uint64_t count) {
        if (other.all_valid()) {
            validity_mask_ = nullptr;
            validity_data_.reset();
            return;
        }
        if (offset == 0) {
            validity_mask_ = other.validity_mask_;
            validity_data_ = other.validity_data_;
            count_ = other.count_;
            return;
        }
        validity_mask_t new_mask(resource(), count);
        new_mask.slice_in_place(other, 0, offset, count);
        validity_mask_ = new_mask.validity_mask_;
        validity_data_ = new_mask.validity_data_;
        count_ = new_mask.count_;
    }

    void validity_mask_t::slice_in_place(const validity_mask_t& other,
                                         uint64_t target_offset,
                                         uint64_t source_offset,
                                         uint64_t count) {
        if (all_valid() && other.all_valid()) {
            return;
        }
        if (!validity_mask_) {
            resize(resource_, count_);
        }
        uint64_t ragged = count % BITS_PER_VALUE;
        uint64_t entire_units = count / BITS_PER_VALUE;
        if (is_aligned(source_offset) && is_aligned(target_offset)) {
            auto target_validity = validity_mask_;
            auto source_validity = other.validity_mask_;
            auto source_offset_entries = validity_data_t::entry_count(source_offset);
            auto target_offset_entries = validity_data_t::entry_count(target_offset);
            if (!source_validity) {
                memset(target_validity + target_offset_entries, 0xFF, sizeof(uint64_t) * entire_units);
            } else {
                memcpy(target_validity + target_offset_entries,
                       source_validity + source_offset_entries,
                       sizeof(uint64_t) * entire_units);
            }
            if (ragged) {
                auto src_entry = source_validity ? source_validity[source_offset_entries + entire_units]
                                                 : validity_data_t::MAX_ENTRY;
                src_entry &= (validity_data_t::MAX_ENTRY >> (BITS_PER_VALUE - ragged));

                target_validity += target_offset_entries + entire_units;
                auto tgt_entry = *target_validity;
                tgt_entry &= (validity_data_t::MAX_ENTRY << ragged);

                *target_validity = tgt_entry | src_entry;
            }
            return;
        } else if (is_aligned(target_offset)) {
            uint64_t tail = source_offset % BITS_PER_VALUE;
            uint64_t head = BITS_PER_VALUE - tail;
            auto source_validity = other.validity_mask_ + (source_offset / BITS_PER_VALUE);
            auto target_validity = this->validity_mask_ + (target_offset / BITS_PER_VALUE);
            auto src_entry = *source_validity++;
            for (uint64_t i = 0; i < entire_units; ++i) {
                uint64_t tgt_entry = src_entry >> tail;
                src_entry = *source_validity++;
                tgt_entry |= (src_entry << head);
                *target_validity++ = tgt_entry;
            }
            if (ragged) {
                uint64_t tgt_entry = (src_entry >> tail);
                if (head < ragged) {
                    src_entry = *source_validity++;
                    tgt_entry |= (src_entry << head);
                }
                tgt_entry &= (validity_data_t::MAX_ENTRY >> (BITS_PER_VALUE - ragged));
                tgt_entry |= *target_validity & (validity_data_t::MAX_ENTRY << ragged);
                *target_validity++ = tgt_entry;
            }
            return;
        }

        for (uint64_t i = 0; i < count; i++) {
            set(target_offset + i, other.row_is_valid(source_offset + i));
        }
    }
    void validity_mask_t::combine(const validity_mask_t& other, size_t count) {
        if (other.all_valid()) {
            // X & 1 = X
            return;
        }
        if (all_valid()) {
            // 1 & Y = Y
            validity_data_ = std::make_shared<validity_data_t>(resource_, other.validity_mask_, count_);
            validity_mask_ = validity_data_->data();
            return;
        }
        if (validity_mask_ == other.validity_mask_) {
            // X & X == X
            return;
        }

        validity_data_ = std::make_shared<validity_data_t>(resource_, validity_mask_, count_);
        validity_mask_ = validity_data_->data();

        auto entry_count = validity_data_t::entry_count(count);
        for (size_t i = 0; i < entry_count; i++) {
            data()[i] = data()[i] & other.data()[i];
        }
    }

    void validity_mask_t::resize(std::pmr::memory_resource* resource, uint64_t new_size) {
        uint64_t old_size = count_;
        if (new_size <= old_size && !all_valid()) {
            return;
        }
        count_ = new_size;
        auto new_size_count = validity_data_t::entry_count(new_size);
        auto old_size_count = validity_data_t::entry_count(old_size);
        auto new_validity_data = std::make_unique<validity_data_t>(resource, new_size);
        auto new_owned_data = new_validity_data->data();
        if (validity_mask_) {
            for (uint64_t entry_idx = 0; entry_idx < old_size_count; entry_idx++) {
                new_owned_data[entry_idx] = validity_mask_[entry_idx];
            }
            for (uint64_t entry_idx = old_size_count; entry_idx < new_size_count; entry_idx++) {
                new_owned_data[entry_idx] = validity_data_t::MAX_ENTRY;
            }
        }
        validity_data_ = std::move(new_validity_data);
        validity_mask_ = validity_data_->data();
    }

    uint64_t validity_mask_t::get_validity_entry(uint64_t entry_idx) const {
        if (!validity_mask_) {
            return validity_data_t::MAX_ENTRY;
        }
        return validity_mask_[entry_idx];
    }

    void validity_mask_t::copy_indexing(const validity_mask_t& other,
                                        const indexing_vector_t& indexing,
                                        uint64_t source_offset,
                                        uint64_t target_offset,
                                        uint64_t count) {
        if (!other.is_mask_set() && !is_mask_set()) {
            return;
        }

        if (!indexing.is_set() && is_aligned(source_offset) && is_aligned(target_offset)) {
            slice_in_place(other, target_offset, source_offset, count);
            return;
        }
        for (uint64_t i = 0; i < count; i++) {
            auto source_idx = indexing.get_index(source_offset + i);
            set(target_offset + i, other.row_is_valid(source_idx));
        }
    }

} // namespace components::vector
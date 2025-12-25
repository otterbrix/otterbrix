#pragma once

#include <core/pmr.hpp>
#include <cstdint>

#include "indexing_vector.hpp"

namespace components::vector {

    enum class validity_serialization_type : uint8_t
    {
        BITMASK = 0,
        VALID_VALUES = 1,
        INVALID_VALUES = 2
    };

    class validity_data_t {
    public:
        static constexpr uint64_t BITS_PER_VALUE = sizeof(uint64_t) * 8;
        static constexpr uint64_t MAX_ENTRY = uint64_t(-1);

        explicit validity_data_t(std::pmr::memory_resource* resource, uint64_t size);
        explicit validity_data_t(std::pmr::memory_resource* resource, uint64_t* mask, uint64_t size);
        validity_data_t(validity_data_t&& other) noexcept = default;
        validity_data_t& operator=(validity_data_t&& other) noexcept = default;
        ~validity_data_t() = default;

        uint64_t* data() const noexcept { return data_.get(); }
        uint64_t* data() noexcept { return data_.get(); }

        static constexpr uint64_t entry_count(uint64_t count) {
            return (count + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
        }
        std::pmr::memory_resource* resource() const noexcept { return data_.get_deleter().resource(); }

    private:
        std::unique_ptr<uint64_t[], core::pmr::array_deleter_t> data_;
    };

    class validity_mask_t {
    public:
        static constexpr uint64_t BITS_PER_VALUE = validity_data_t::BITS_PER_VALUE;
        static constexpr uint64_t STANDARD_ENTRY_COUNT =
            (DEFAULT_VECTOR_CAPACITY + (BITS_PER_VALUE - 1)) / BITS_PER_VALUE;
        static constexpr uint64_t STANDARD_MASK_SIZE = STANDARD_ENTRY_COUNT * sizeof(uint64_t);

        static constexpr uint64_t validity_mask_size(uint64_t count = DEFAULT_VECTOR_CAPACITY) {
            return validity_data_t::entry_count(count) * sizeof(uint64_t);
        }

        explicit validity_mask_t(std::pmr::memory_resource* resource, uint64_t size);
        explicit validity_mask_t(uint64_t* ptr)
            : resource_(nullptr)
            , validity_mask_(ptr)
            , count_(DEFAULT_VECTOR_CAPACITY) {}
        validity_mask_t(const validity_mask_t& other);
        validity_mask_t& operator=(const validity_mask_t& other);
        validity_mask_t(validity_mask_t&& other) noexcept;
        validity_mask_t& operator=(validity_mask_t&& other) noexcept;

        void set_invalid(uint64_t entry_idx, uint64_t idx_in_entry);
        void set_invalid(uint64_t row_idx);
        void set_valid(uint64_t row_idx);

        void set_all_invalid(uint64_t count);
        bool is_mask_set() const { return validity_mask_; }
        bool is_aligned(uint64_t offset) const noexcept { return offset % BITS_PER_VALUE == 0; }
        void copy_indexing(const validity_mask_t& other,
                           const indexing_vector_t& indexing,
                           uint64_t source_offset,
                           uint64_t target_offset,
                           uint64_t count);
        bool row_is_valid(uint64_t index) const;
        bool all_valid() const noexcept { return !validity_mask_; }

        uint64_t count_valid(uint64_t count) const;
        void set(uint64_t row_index, bool valid);
        void slice(const validity_mask_t& other, uint64_t offset, uint64_t count);
        void
        slice_in_place(const validity_mask_t& other, uint64_t target_offset, uint64_t source_offset, uint64_t count);
        void combine(const validity_mask_t& other, size_t count);
        void reset(uint64_t target_count = DEFAULT_VECTOR_CAPACITY) {
            validity_mask_ = nullptr;
            validity_data_.reset();
            count_ = target_count;
        }

        uint64_t* data() noexcept { return validity_mask_; }
        uint64_t* data() const noexcept { return validity_mask_; }
        uint64_t get_validity_entry(uint64_t entry_idx) const;
        uint64_t& operator[](uint64_t index) const { return validity_mask_[index]; }
        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        uint64_t count() const noexcept { return count_; }
        void resize(std::pmr::memory_resource* resource, uint64_t new_size);

    private:
        static void entry_index(uint64_t row_idx, uint64_t& entry_idx, uint64_t& idx_in_entry) {
            entry_idx = row_idx / BITS_PER_VALUE;
            idx_in_entry = row_idx % BITS_PER_VALUE;
        }

        std::pmr::memory_resource* resource_;
        std::shared_ptr<validity_data_t> validity_data_;
        uint64_t* validity_mask_;
        uint64_t count_;
    };

    namespace validity_details {
        // LOWER_MASKS contains masks with all the lower bits set until a specific value
        // LOWER_MASKS[0] has the 0 lowest bits set:
        // 0b0000000000000000000000000000000000000000000000000000000000000000
        // LOWER_MASKS[10] has the 10 lowest bits set:
        // 0b0000000000000000000000000000000000000000000000000000000111111111
        static constexpr uint64_t LOWER_MASKS[] = {0x0,
                                                   0x1,
                                                   0x3,
                                                   0x7,
                                                   0xf,
                                                   0x1f,
                                                   0x3f,
                                                   0x7f,
                                                   0xff,
                                                   0x1ff,
                                                   0x3ff,
                                                   0x7ff,
                                                   0xfff,
                                                   0x1fff,
                                                   0x3fff,
                                                   0x7fff,
                                                   0xffff,
                                                   0x1ffff,
                                                   0x3ffff,
                                                   0x7ffff,
                                                   0xfffff,
                                                   0x1fffff,
                                                   0x3fffff,
                                                   0x7fffff,
                                                   0xffffff,
                                                   0x1ffffff,
                                                   0x3ffffff,
                                                   0x7ffffff,
                                                   0xfffffff,
                                                   0x1fffffff,
                                                   0x3fffffff,
                                                   0x7fffffff,
                                                   0xffffffff,
                                                   0x1ffffffff,
                                                   0x3ffffffff,
                                                   0x7ffffffff,
                                                   0xfffffffff,
                                                   0x1fffffffff,
                                                   0x3fffffffff,
                                                   0x7fffffffff,
                                                   0xffffffffff,
                                                   0x1ffffffffff,
                                                   0x3ffffffffff,
                                                   0x7ffffffffff,
                                                   0xfffffffffff,
                                                   0x1fffffffffff,
                                                   0x3fffffffffff,
                                                   0x7fffffffffff,
                                                   0xffffffffffff,
                                                   0x1ffffffffffff,
                                                   0x3ffffffffffff,
                                                   0x7ffffffffffff,
                                                   0xfffffffffffff,
                                                   0x1fffffffffffff,
                                                   0x3fffffffffffff,
                                                   0x7fffffffffffff,
                                                   0xffffffffffffff,
                                                   0x1ffffffffffffff,
                                                   0x3ffffffffffffff,
                                                   0x7ffffffffffffff,
                                                   0xfffffffffffffff,
                                                   0x1fffffffffffffff,
                                                   0x3fffffffffffffff,
                                                   0x7fffffffffffffff,
                                                   0xffffffffffffffff};

        // UPPER_MASKS contains masks with all the highest bits set until a specific value
        // UPPER_MASKS[0] has the 0 highest bits set:
        // 0b0000000000000000000000000000000000000000000000000000000000000000
        // UPPER_MASKS[10] has the 10 highest bits set:
        // 0b1111111111110000000000000000000000000000000000000000000000000000
        static constexpr uint64_t UPPER_MASKS[] = {0x0,
                                                   0x8000000000000000,
                                                   0xc000000000000000,
                                                   0xe000000000000000,
                                                   0xf000000000000000,
                                                   0xf800000000000000,
                                                   0xfc00000000000000,
                                                   0xfe00000000000000,
                                                   0xff00000000000000,
                                                   0xff80000000000000,
                                                   0xffc0000000000000,
                                                   0xffe0000000000000,
                                                   0xfff0000000000000,
                                                   0xfff8000000000000,
                                                   0xfffc000000000000,
                                                   0xfffe000000000000,
                                                   0xffff000000000000,
                                                   0xffff800000000000,
                                                   0xffffc00000000000,
                                                   0xffffe00000000000,
                                                   0xfffff00000000000,
                                                   0xfffff80000000000,
                                                   0xfffffc0000000000,
                                                   0xfffffe0000000000,
                                                   0xffffff0000000000,
                                                   0xffffff8000000000,
                                                   0xffffffc000000000,
                                                   0xffffffe000000000,
                                                   0xfffffff000000000,
                                                   0xfffffff800000000,
                                                   0xfffffffc00000000,
                                                   0xfffffffe00000000,
                                                   0xffffffff00000000,
                                                   0xffffffff80000000,
                                                   0xffffffffc0000000,
                                                   0xffffffffe0000000,
                                                   0xfffffffff0000000,
                                                   0xfffffffff8000000,
                                                   0xfffffffffc000000,
                                                   0xfffffffffe000000,
                                                   0xffffffffff000000,
                                                   0xffffffffff800000,
                                                   0xffffffffffc00000,
                                                   0xffffffffffe00000,
                                                   0xfffffffffff00000,
                                                   0xfffffffffff80000,
                                                   0xfffffffffffc0000,
                                                   0xfffffffffffe0000,
                                                   0xffffffffffff0000,
                                                   0xffffffffffff8000,
                                                   0xffffffffffffc000,
                                                   0xffffffffffffe000,
                                                   0xfffffffffffff000,
                                                   0xfffffffffffff800,
                                                   0xfffffffffffffc00,
                                                   0xfffffffffffffe00,
                                                   0xffffffffffffff00,
                                                   0xffffffffffffff80,
                                                   0xffffffffffffffc0,
                                                   0xffffffffffffffe0,
                                                   0xfffffffffffffff0,
                                                   0xfffffffffffffff8,
                                                   0xfffffffffffffffc,
                                                   0xfffffffffffffffe,
                                                   0xffffffffffffffff};
    } // namespace validity_details

} // namespace components::vector
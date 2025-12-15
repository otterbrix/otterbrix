#pragma once

#include <components/types/physical_value.hpp>

#include <cassert>
#include <components/types/types.hpp>
#include <cstdlib>
#include <functional>
#include <memory>
#include <memory_resource>
#include <vector>

namespace core::b_plus_tree {

    typedef char data_t;
    typedef data_t* data_ptr_t;
    typedef const data_t* const_data_ptr_t;

    constexpr uint32_t INVALID_SIZE = uint32_t(-1);
    // The default block size
    constexpr uint32_t DEFAULT_BLOCK_SIZE = 262144; // 32 Kb
    // Block is used as page analog and 4 Gb is enough for that purpose
    constexpr uint32_t MAX_BLOCK_SIZE = uint32_t(-1) - 1; // 4 Gb

    // Align size (ceiling)
    static inline uint32_t align_to_block_size(uint32_t size) {
        return (size + (DEFAULT_BLOCK_SIZE - 1)) / DEFAULT_BLOCK_SIZE * DEFAULT_BLOCK_SIZE;
    }

    //TODO: create a multi value index for secondary comparisons (this also could be used to make array into a key)
    class block_t {
    public:
        using index_t = components::types::physical_value;

    private:
        struct metadata {
            uint32_t offset;
            uint32_t size;
            index_t index;
        };

    public:
        struct item_data {
            data_ptr_t data;
            uint32_t size;

            explicit item_data() noexcept
                : data(nullptr)
                , size(0) {}
            item_data(data_ptr_t d, uint32_t s) noexcept
                : data(d)
                , size(s) {}

            explicit operator bool() const noexcept { return data != nullptr; }
        };

        struct metadata_range {
            metadata* begin = nullptr;
            metadata* end = nullptr;
        };

        class iterator {
        public:
            struct iterator_data {
                index_t index;
                item_data item;
            };
            iterator(const block_t* block, metadata* metadata);
            iterator(const iterator& other);
            iterator(iterator&& other) noexcept;

            inline const iterator_data& operator*() const { return data_; }
            inline const iterator_data* operator->() const { return &data_; }

            // note: metadata is stored in reverse order, so iterator increment and decrement is reversed
            inline const iterator& operator++() {
                metadata_--;
                rebuild_data();
                return *this;
            }
            inline const iterator& operator--() {
                metadata_++;
                rebuild_data();
                return *this;
            }
            inline iterator operator++(int) {
                auto temp = iterator(block_, metadata_);
                metadata_--;
                rebuild_data();
                return temp;
            }
            inline iterator operator--(int) {
                auto temp = iterator(block_, metadata_);
                metadata_++;
                rebuild_data();
                return temp;
            }
            inline iterator operator+(int i) { return iterator(block_, metadata_ - i); }
            inline iterator operator-(int i) { return iterator(block_, metadata_ + i); }
            friend inline iterator operator+(int i, const iterator& rhs) {
                return iterator(rhs.block_, rhs.metadata_ - i);
            }
            friend inline iterator operator-(int i, const iterator& rhs) {
                return iterator(rhs.block_, rhs.metadata_ + i);
            }
            friend long int operator-(const iterator& lhs, const iterator& rhs) {
                assert(lhs.block_ == rhs.block_);
                return rhs.metadata_ - lhs.metadata_;
            }

            inline iterator& operator=(const iterator& rhs) {
                metadata_ = rhs.metadata_;
                rebuild_data();
                return *this;
            }
            inline iterator& operator+=(int rhs) {
                metadata_ -= rhs;
                rebuild_data();
                return *this;
            }
            inline iterator& operator-=(int rhs) {
                metadata_ += rhs;
                rebuild_data();
                return *this;
            }

            inline bool operator==(const iterator& rhs) { return metadata_ == rhs.metadata_; }
            inline bool operator<(const iterator& rhs) { return metadata_ > rhs.metadata_; }
            inline bool operator>(const iterator& rhs) { return metadata_ < rhs.metadata_; }
            inline bool operator!=(const iterator& rhs) { return !(*this == rhs); }
            inline bool operator<=(const iterator& rhs) { return !(*this > rhs); }
            inline bool operator>=(const iterator& rhs) { return !(*this < rhs); }

        private:
            void rebuild_data();

            const block_t* block_;
            metadata* metadata_;
            iterator_data data_;
        };

        class r_iterator {
        public:
            struct iterator_data {
                index_t index;
                item_data item;
            };
            r_iterator(const block_t* block, metadata* metadata);
            r_iterator(const r_iterator& other);
            r_iterator(r_iterator&& other) noexcept;

            inline const iterator_data& operator*() const { return data_; }
            inline const iterator_data* operator->() const { return &data_; }

            // note: metadata is stored in reverse order, so iterator increment and decrement is reversed
            inline const r_iterator& operator++() {
                metadata_++;
                rebuild_data();
                return *this;
            }
            inline const r_iterator& operator--() {
                metadata_--;
                rebuild_data();
                return *this;
            }
            inline r_iterator operator++(int) {
                auto temp = r_iterator(block_, metadata_);
                metadata_++;
                rebuild_data();
                return temp;
            }
            inline r_iterator operator--(int) {
                auto temp = r_iterator(block_, metadata_);
                metadata_--;
                rebuild_data();
                return temp;
            }
            inline r_iterator operator+(int i) { return r_iterator(block_, metadata_ + i); }
            inline r_iterator operator-(int i) { return r_iterator(block_, metadata_ - i); }
            friend inline r_iterator operator+(int i, const r_iterator& rhs) {
                return r_iterator(rhs.block_, rhs.metadata_ + i);
            }
            friend inline r_iterator operator-(int i, const r_iterator& rhs) {
                return r_iterator(rhs.block_, rhs.metadata_ - i);
            }
            friend long int operator-(const r_iterator& lhs, const r_iterator& rhs) {
                assert(lhs.block_ == rhs.block_);
                return lhs.metadata_ - rhs.metadata_;
            }

            inline r_iterator& operator=(const r_iterator& rhs) {
                metadata_ = rhs.metadata_;
                rebuild_data();
                return *this;
            }
            inline r_iterator& operator+=(int rhs) {
                metadata_ += rhs;
                rebuild_data();
                return *this;
            }
            inline r_iterator& operator-=(int rhs) {
                metadata_ -= rhs;
                rebuild_data();
                return *this;
            }

            inline bool operator==(const r_iterator& rhs) { return metadata_ == rhs.metadata_; }
            inline bool operator<(const r_iterator& rhs) { return metadata_ < rhs.metadata_; }
            inline bool operator>(const r_iterator& rhs) { return metadata_ > rhs.metadata_; }
            inline bool operator!=(const r_iterator& rhs) { return !(*this == rhs); }
            inline bool operator<=(const r_iterator& rhs) { return !(*this > rhs); }
            inline bool operator>=(const r_iterator& rhs) { return !(*this < rhs); }

        private:
            void rebuild_data();

            const block_t* block_;
            metadata* metadata_;
            iterator_data data_;
        };

        friend class iterator;
        friend class r_iterator;

        block_t(std::pmr::memory_resource* resource, index_t (*func)(const item_data&));
        ~block_t();
        block_t(const block_t&) = delete;
        block_t(block_t&&) = default;
        block_t& operator=(const block_t&) = delete;
        block_t& operator=(block_t&&) = default;

        void initialize(uint32_t size = DEFAULT_BLOCK_SIZE);

        size_t available_memory() const;
        // does not include header
        size_t occupied_memory() const;
        bool is_memory_available(size_t request_size) const;
        bool is_empty() const;
        bool is_valid() const;

        bool append(data_ptr_t data, uint32_t size) noexcept;
        bool append(item_data item) noexcept;
        bool append(const index_t& index, item_data item) noexcept;
        bool remove(data_ptr_t data, uint32_t size) noexcept;
        bool remove(item_data item) noexcept;
        bool remove(const index_t& index, item_data item) noexcept;
        // remove all entries with this index
        bool remove_index(const index_t& index);

        bool contains_index(const index_t& index) const;
        bool contains(item_data item) const;
        bool contains(const index_t& index, item_data item) const;
        uint32_t item_count(const index_t& index) const;
        item_data get_item(const index_t& index, uint32_t position) const;
        void get_items(std::vector<item_data>& items, const index_t& index) const;

        uint32_t count() const;
        uint32_t unique_indices_count() const;
        index_t min_index() const;
        index_t max_index() const;

        data_ptr_t internal_buffer();
        uint32_t block_size() const;
        // should be called when internal_buffer is used for reading to restore metadata
        void restore_block();
        void resize(uint32_t new_size);
        void reset();

        // after splits this block will contain the first half
        // best case: second block >= first block
        [[nodiscard]] std::pair<std::unique_ptr<block_t>, std::unique_ptr<block_t>> split_append(const index_t& index,
                                                                                                 item_data item);
        // creates new block and puts last "count" elements there
        [[nodiscard]] std::unique_ptr<block_t> split(uint32_t count);
        // creates new block and puts last "count" indices there
        [[nodiscard]] std::unique_ptr<block_t> split_uniques(uint32_t count);
        // merge other block to this one
        void merge(std::unique_ptr<block_t>&& block);

        void recalculate_checksum();
        // return false if block is not in the same state as when check sum was calculated
        bool varify_checksum() const;

        // block is an ordered container, data cannot be modified by iterator
        iterator begin() const { return cbegin(); }
        iterator end() const { return cend(); }
        iterator cbegin() const { return iterator(this, end_ - 1); }
        iterator cend() const { return iterator(this, last_metadata_ - 1); }
        r_iterator rbegin() const { return r_iterator({this, last_metadata_}); }
        r_iterator rend() const { return r_iterator({this, end_}); }

    private:
        metadata_range find_index_range_(const index_t& index) const;
        void remove_range_(metadata_range range);
        item_data metadata_to_item_data_(const metadata* meta) const;
        uint32_t calculate_checksum_() const;

        std::pmr::memory_resource* resource_ = nullptr;
        index_t (*key_func_)(const item_data&);
        // The pointer to the internal buffer that will be read or written, including the buffer header
        data_ptr_t internal_buffer_ = nullptr;
        bool is_valid_ = false;

        // start location of free part
        data_ptr_t buffer_ = nullptr;
        // store explicitly to avoid calculating it every time
        metadata* end_ = nullptr;
        // where last written id/pointer pair is located
        metadata* last_metadata_ = nullptr;

        uint32_t full_size_ = 0;
        // The size of free part
        uint32_t available_memory_ = 0;

        // header data
        uint32_t* count_ = nullptr;
        uint32_t* unique_indices_count_ = nullptr;
        uint32_t* checksum_ = nullptr;

    public:
        // header_size is used to pack this values inside block buffer to avoid recalculating them
        static constexpr uint32_t header_size = sizeof(std::remove_pointer_t<decltype(count_)>) +
                                                sizeof(std::remove_pointer_t<decltype(unique_indices_count_)>) +
                                                sizeof(std::remove_pointer_t<decltype(checksum_)>);
        static constexpr uint32_t metadata_size = sizeof(metadata);
    };

    [[nodiscard]] static inline std::unique_ptr<block_t>
    create_initialize(std::pmr::memory_resource* resource,
                      block_t::index_t (*func)(const block_t::item_data&),
                      uint32_t size = DEFAULT_BLOCK_SIZE) {
        std::unique_ptr<block_t> block = std::make_unique<block_t>(resource, func);
        block->initialize(size);
        return block;
    }
} // namespace core::b_plus_tree
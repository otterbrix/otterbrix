#pragma once

#include <chrono>
#include <map>
#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

#include "block.hpp"
#include <core/file/file_system.hpp>

namespace core::b_plus_tree {

    class gap_tracker_t {
    public:
        struct gap_t {
            size_t offset;
            size_t size;
        };

        gap_tracker_t(size_t min, size_t max) { init(min, max); }
        void init(size_t min, size_t max) {
            empty_spaces_.clear();
            empty_spaces_.push_back({min, max - min});
        }
        size_t find_gap(size_t size) {
            auto gap = empty_spaces_.begin();
            for (; gap < empty_spaces_.end(); gap++) {
                if (gap->size >= size) {
                    size_t result = gap->offset;
                    if (gap->size == size) {
                        empty_spaces_.erase(gap);
                    } else {
                        gap->offset += size;
                        gap->size -= size;
                    }
                    return result;
                }
            }
            assert(false && "Not enough memory in gap_tracker_t");
            return INVALID_SIZE;
        }
        void remove_gap(gap_t required_gap) {
            auto gap = empty_spaces_.begin();
            for (; gap < empty_spaces_.end(); gap++) {
                if (gap->offset > required_gap.offset) {
                    break;
                }
            }
            empty_spaces_.insert(gap, required_gap);

            clean_gaps();
        }
        void clean_gaps() {
            for (size_t i = 0; i < empty_spaces_.size() - 1;) {
                if (empty_spaces_[i].offset + empty_spaces_[i].size == empty_spaces_[i + 1].offset) {
                    empty_spaces_[i].size += empty_spaces_[i + 1].size;
                    empty_spaces_.erase(empty_spaces_.begin() + static_cast<int32_t>(i) + 1);
                } else {
                    i++;
                }
            }
        }
        std::vector<gap_t>& empty_spaces() { return empty_spaces_; }

    private:
        std::vector<gap_t> empty_spaces_;
    };

    // TODO: move memory overflow checks to b_plus_tree
    class segment_tree_t {
        struct block_metadata {
            size_t file_offset;
            size_t size;
            block_t::index_t min_index;
            block_t::index_t max_index;
        };
        struct metadata_range {
            block_metadata* begin = nullptr;
            block_metadata* end = nullptr;
        };
        struct node_t {
            std::unique_ptr<block_t> block;
            std::chrono::time_point<std::chrono::system_clock> last_used;
            bool modified;
        };
        using it = std::vector<node_t>::iterator;
        static constexpr size_t block_metadata_size = sizeof(block_metadata);

    public:
        using index_t = block_t::index_t;
        using item_data = block_t::item_data;

        static constexpr double merge_check = 4.0 / 5.0; // 80%
        static constexpr size_t header_size =
            2 * DEFAULT_BLOCK_SIZE; // this will give 2^14 - 1 block capacity and 16 free bytes for something later

        // it is possible to just use segments_::iterator, but it won't work correctly if block is not loaded
        // and there won't be any overhead of node_t shown
        class iterator {
        public:
            // const segment_tree_t* will block from trying to load a block_t, if it is needed by the iterator
            iterator(segment_tree_t* seg_tree, block_metadata* metadata);

            inline const block_t& operator*() {
                load_block();
                return *block_;
            }
            inline const block_t* operator->() {
                load_block();
                return block_;
            }

            inline const iterator& operator++() {
                metadata_--;
                get_block();
                return *this;
            }
            inline const iterator& operator--() {
                metadata_++;
                get_block();
                return *this;
            }
            inline iterator operator++(int) {
                auto temp = iterator(seg_tree_, metadata_);
                metadata_++;
                get_block();
                return temp;
            }
            inline iterator operator--(int) {
                auto temp = iterator(seg_tree_, metadata_);
                metadata_--;
                get_block();
                return temp;
            }
            inline iterator operator+(int i) { return iterator(seg_tree_, metadata_ + i); }
            inline iterator operator-(int i) { return iterator(seg_tree_, metadata_ - i); }
            friend inline iterator operator+(int i, const iterator& rhs) {
                return iterator(rhs.seg_tree_, rhs.metadata_ + i);
            }
            friend inline iterator operator-(int i, const iterator& rhs) {
                return iterator(rhs.seg_tree_, rhs.metadata_ - i);
            }
            friend int operator-(const iterator& lhs, const iterator& rhs) {
                assert(lhs.seg_tree_ == rhs.seg_tree_);
                return lhs.metadata_ - rhs.metadata_;
            }

            inline iterator& operator=(const iterator& rhs) {
                metadata_ = rhs.metadata_;
                get_block();
                return *this;
            }
            inline iterator& operator+=(int rhs) {
                metadata_ += rhs;
                get_block();
                return *this;
            }
            inline iterator& operator-=(int rhs) {
                metadata_ -= rhs;
                get_block();
                return *this;
            }

            inline bool operator==(const iterator& rhs) { return metadata_ == rhs.metadata_; }
            inline bool operator<(const iterator& rhs) { return metadata_ < rhs.metadata_; }
            inline bool operator>(const iterator& rhs) { return metadata_ > rhs.metadata_; }
            inline bool operator!=(const iterator& rhs) { return !(*this == rhs); }
            inline bool operator<=(const iterator& rhs) { return !(*this > rhs); }
            inline bool operator>=(const iterator& rhs) { return !(*this < rhs); }

        private:
            void get_block();
            void load_block();

            segment_tree_t* seg_tree_;
            block_metadata* metadata_;
            block_t* block_;
        };

        class r_iterator {
        public:
            // const segment_tree_t* will block from trying to load a block_t, if it is needed by the iterator
            r_iterator(segment_tree_t* seg_tree, block_metadata* metadata);

            inline const block_t& operator*() {
                load_block();
                return *block_;
            }
            inline const block_t* operator->() {
                load_block();
                return block_;
            }

            inline const r_iterator& operator++() {
                metadata_++;
                get_block();
                return *this;
            }
            inline const r_iterator& operator--() {
                metadata_--;
                get_block();
                return *this;
            }
            inline r_iterator operator++(int) {
                auto temp = r_iterator(seg_tree_, metadata_);
                metadata_--;
                get_block();
                return temp;
            }
            inline r_iterator operator--(int) {
                auto temp = r_iterator(seg_tree_, metadata_);
                metadata_++;
                get_block();
                return temp;
            }
            inline r_iterator operator+(int i) { return r_iterator(seg_tree_, metadata_ - i); }
            inline r_iterator operator-(int i) { return r_iterator(seg_tree_, metadata_ + i); }
            friend inline r_iterator operator+(int i, const r_iterator& rhs) {
                return r_iterator(rhs.seg_tree_, rhs.metadata_ - i);
            }
            friend inline r_iterator operator-(int i, const r_iterator& rhs) {
                return r_iterator(rhs.seg_tree_, rhs.metadata_ + i);
            }
            friend int operator-(const r_iterator& lhs, const r_iterator& rhs) {
                assert(lhs.seg_tree_ == rhs.seg_tree_);
                return rhs.metadata_ - lhs.metadata_;
            }

            inline r_iterator& operator=(const r_iterator& rhs) {
                metadata_ = rhs.metadata_;
                get_block();
                return *this;
            }
            inline r_iterator& operator+=(int rhs) {
                metadata_ -= rhs;
                get_block();
                return *this;
            }
            inline r_iterator& operator-=(int rhs) {
                metadata_ += rhs;
                get_block();
                return *this;
            }

            inline bool operator==(const r_iterator& rhs) { return metadata_ == rhs.metadata_; }
            inline bool operator<(const r_iterator& rhs) { return metadata_ > rhs.metadata_; }
            inline bool operator>(const r_iterator& rhs) { return metadata_ < rhs.metadata_; }
            inline bool operator!=(const r_iterator& rhs) { return !(*this == rhs); }
            inline bool operator<=(const r_iterator& rhs) { return !(*this > rhs); }
            inline bool operator>=(const r_iterator& rhs) { return !(*this < rhs); }

        private:
            void get_block();
            void load_block();

            segment_tree_t* seg_tree_;
            block_metadata* metadata_;
            block_t* block_;
        };

        friend class iterator;
        friend class r_iterator;

        segment_tree_t(std::pmr::memory_resource* resource,
                       index_t (*func)(const item_data&),
                       std::unique_ptr<filesystem::file_handle_t> file);
        ~segment_tree_t();

        // will try to maintain default block size if possible
        bool append(data_ptr_t data, size_t size);
        bool append(item_data item);
        bool append(const index_t& index, item_data item);
        bool remove(data_ptr_t data, size_t size);
        bool remove(item_data item);
        bool remove(const index_t& index, item_data item);
        bool remove_index(const index_t& index);
        [[nodiscard]] std::unique_ptr<segment_tree_t> split(std::unique_ptr<filesystem::file_handle_t> file);
        // requires other->count() > this->count()
        void balance_with(std::unique_ptr<segment_tree_t>& other);
        void merge(std::unique_ptr<segment_tree_t>& other);

        // due to lazy loading this batch can't be const anymore
        bool contains_index(const index_t& index);
        bool contains(item_data item);
        bool contains(const index_t& index, item_data item);

        size_t item_count(const index_t& index);
        item_data get_item(const index_t& index, size_t position);
        void get_items(std::vector<item_data>& result, const index_t& index);

        index_t min_index() const; // with 0 blocks will give [0,INVALID_ID] range:
        index_t max_index() const; // with 0 blocks will give [0,INVALID_ID] range:

        size_t blocks_count() const;
        size_t count() const;
        size_t unique_indices_count() const;
        // flush to disk
        void flush();
        // load all tree segment at once from scratch
        void clean_load();
        // clear current blocks, load only block's metadata
        void lazy_load();

        // segment_tree is an ordered container, data cannot be modified by iterator
        iterator begin() const { return cbegin(); }
        iterator end() const { return cend(); }
        iterator cbegin() const { return iterator(const_cast<segment_tree_t*>(this), metadata_begin_); }
        iterator cend() const { return iterator(const_cast<segment_tree_t*>(this), metadata_end_); }
        r_iterator rbegin() const { return r_iterator({const_cast<segment_tree_t*>(this), metadata_end_ - 1}); }
        r_iterator rend() const { return r_iterator({const_cast<segment_tree_t*>(this), metadata_begin_ - 1}); }

    private:
        metadata_range find_range_(const index_t& index) const;
        void remove_range_(metadata_range range);
        [[nodiscard]] node_t construct_new_node_(const index_t& index, item_data item);
        [[nodiscard]] node_t construct_new_node_(item_data item);
        void load_segment_(block_metadata* metadata);
        void unload_old_segments_();
        // header changes will be handled here:
        void insert_segment_(it pos, node_t&& block);
        void remove_segment_(it pos);
        void update_metadata_(it pos, block_metadata* metadata);
        void close_gaps_();

        std::pmr::memory_resource* resource_;
        index_t (*key_func_)(const item_data&);
        std::vector<node_t> segments_; // will become boost::intrusive

        size_t* header_;
        size_t* item_count_;
        size_t* unique_id_count_;
        block_metadata* metadata_begin_;
        block_metadata* metadata_end_;
        // keep track of gaps in block record and try to fill them when creating new blocks
        gap_tracker_t gap_tracker_{header_size, INVALID_SIZE};

        std::unique_ptr<filesystem::file_handle_t> file_;
        std::vector<std::pair<std::unique_ptr<std::pmr::string>, std::unique_ptr<std::pmr::string>>> string_storage_;
    };

} // namespace core::b_plus_tree
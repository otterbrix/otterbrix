#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory_resource>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "block.hpp"
#include <core/file/file_system.hpp>

namespace core::b_plus_tree {

#ifdef DEV_MODE
    // `leaf_flushes_without_changes` narrows to flushes that wrote no block — a diagnostic, not a bound.
    uint64_t leaf_flushes() noexcept;
    uint64_t leaf_flushes_without_changes() noexcept;
    void reset_leaf_flushes() noexcept;
#endif

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

    // Reported, not asserted (asserts compile out under NDEBUG): an unreadable block refuses to flush.
    enum class load_failure_t : uint8_t
    {
        none = 0,
        data_corruption,   // a block's stored checksum did not match the bytes that came back
        io_error,          // the file would not hand the bytes over at all
        out_of_memory,     // no memory for the block, even after evicting this leaf's residents
        capacity_exceeded, // metadata array full (max_segments); insert would overflow the header
    };

    [[nodiscard]] std::string_view to_string(load_failure_t failure) noexcept;

#ifdef DEV_MODE
    void dev_set_max_segments(size_t limit) noexcept;
    [[nodiscard]] size_t max_segments_limit() noexcept;
#endif

    // First failure wins (compare_exchange only sets from `none`); shared by every leaf of a btree_t.
    class failure_channel_t {
    public:
        void report(load_failure_t failure) noexcept {
            auto expected = static_cast<uint8_t>(load_failure_t::none);
            state_.compare_exchange_strong(expected,
                                           static_cast<uint8_t>(failure),
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed);
        }
        [[nodiscard]] load_failure_t peek() const noexcept {
            return static_cast<load_failure_t>(state_.load(std::memory_order_acquire));
        }
        [[nodiscard]] load_failure_t take() noexcept {
            return static_cast<load_failure_t>(
                state_.exchange(static_cast<uint8_t>(load_failure_t::none), std::memory_order_acq_rel));
        }
        void clear() noexcept { state_.store(static_cast<uint8_t>(load_failure_t::none), std::memory_order_release); }

    private:
        std::atomic<uint8_t> state_{static_cast<uint8_t>(load_failure_t::none)};
    };

    // TODO: move memory overflow checks to b_plus_tree
    class segment_tree_t {
        struct header_t {
            size_t header_checksum_;
            size_t segments_count_;
            size_t item_count_;
            size_t unique_id_count_;
        };

        static_assert(std::is_standard_layout_v<header_t>);
        static_assert(std::is_trivially_constructible_v<header_t>);
        static_assert(std::is_trivially_copyable_v<header_t>);

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
            // Empty stand-in for a read/checksum failure; travels with the node, refused by insert_segment_().
            bool unreadable = false;
        };
        using it = std::vector<node_t>::iterator;
        static constexpr size_t block_metadata_size = sizeof(block_metadata);

    public:
        using index_t = block_t::index_t;
        using item_data = block_t::item_data;

        static constexpr double merge_check = 4.0 / 5.0;
        static constexpr size_t header_size = 2 * DEFAULT_BLOCK_SIZE;
        // Bound checked in read_header_() and insert_segment_(), or either could walk past the header allocation.
        static constexpr size_t max_segments = (header_size - sizeof(header_t)) / block_metadata_size;

        class iterator {
        public:
            iterator(segment_tree_t* seg_tree, block_metadata* metadata);
            iterator(const iterator& other);
            iterator(iterator&& other) noexcept;

            // Unlike operator*/operator->, may return nullptr: an out_of_memory can't even allocate the stand-in.
            [[nodiscard]] inline const block_t* get() {
                load_block();
                return block_;
            }
            inline const block_t& operator*() {
                load_block();
                return *block_;
            }
            inline const block_t* operator->() {
                load_block();
                return block_;
            }

            // Must move the same direction as postfix below; tree traversal relies on it.
            inline const iterator& operator++() {
                metadata_++;
                get_block();
                return *this;
            }
            inline const iterator& operator--() {
                metadata_--;
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
            friend long int operator-(const iterator& lhs, const iterator& rhs) {
                assert(lhs.seg_tree_ == rhs.seg_tree_);
                return lhs.metadata_ - rhs.metadata_;
            }

            inline iterator& operator=(const iterator& rhs) {
                // Must copy seg_tree_ too — a past bug left the iterator reading the OLD tree's table.
                seg_tree_ = rhs.seg_tree_;
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
            r_iterator(segment_tree_t* seg_tree, block_metadata* metadata);
            r_iterator(const r_iterator& other);
            r_iterator(r_iterator&& other) noexcept;

            [[nodiscard]] inline const block_t* get() {
                load_block();
                return block_;
            }
            inline const block_t& operator*() {
                load_block();
                return *block_;
            }
            inline const block_t* operator->() {
                load_block();
                return block_;
            }

            inline const r_iterator& operator++() {
                metadata_--;
                get_block();
                return *this;
            }
            inline const r_iterator& operator--() {
                metadata_++;
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
            friend long int operator-(const r_iterator& lhs, const r_iterator& rhs) {
                assert(lhs.seg_tree_ == rhs.seg_tree_);
                return rhs.metadata_ - lhs.metadata_;
            }

            inline r_iterator& operator=(const r_iterator& rhs) {
                seg_tree_ = rhs.seg_tree_;
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

        // Pinned-handle ctor: holds one fd for the leaf's whole life. Test-only fault-injection seam.
        segment_tree_t(std::pmr::memory_resource* resource,
                       index_t (*func)(const item_data&),
                       std::unique_ptr<filesystem::file_handle_t> file);
        // Opens/closes a lease per operation: the pinned ctor exhausted the fd table under parallel tests.
        segment_tree_t(std::pmr::memory_resource* resource,
                       index_t (*func)(const item_data&),
                       filesystem::local_file_system_t& fs,
                       filesystem::path_t file_path);
        ~segment_tree_t();

        bool append(data_ptr_t data, uint32_t size);
        bool append(item_data item);
        bool append(const index_t& index, item_data item);
        bool remove(data_ptr_t data, uint32_t size);
        bool remove(item_data item);
        bool remove(const index_t& index, item_data item);
        bool remove_index(const index_t& index);
        [[nodiscard]] std::unique_ptr<segment_tree_t> split(std::unique_ptr<filesystem::file_handle_t> file);
        [[nodiscard]] std::unique_ptr<segment_tree_t> split(filesystem::path_t new_file_path);
        // requires other->count() > this->count()
        void balance_with(std::unique_ptr<segment_tree_t>& other);
        // false = nothing moved; all-or-nothing since btree_t deletes the source leaf regardless.
        [[nodiscard]] bool merge(std::unique_ptr<segment_tree_t>& other);

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
        // False when a write/truncate/fsync failed (leaf stays dirty), or while holding an unreadable block.
        [[nodiscard]] bool flush();
        void clean_load();
        void lazy_load();

        // THE REFUSAL CHANNEL: sticky, and shared with the owning btree_t so one read covers every leaf.
        [[nodiscard]] load_failure_t load_failure() const noexcept { return channel_->peek(); }
        void reset_load_failure() noexcept { channel_->clear(); }
        [[nodiscard]] failure_channel_t* failure_channel() const noexcept { return channel_; }
        void set_failure_channel(failure_channel_t* channel) noexcept {
            channel_ = channel != nullptr ? channel : &own_failures_;
        }
        // DERIVED, not remembered: a block reading back later makes the leaf writable again by itself.
        [[nodiscard]] bool poisoned() const noexcept {
            return abandoned_.load(std::memory_order_acquire) ||
                   unreadable_segments_.load(std::memory_order_acquire) != 0;
        }

        iterator begin() const { return cbegin(); }
        iterator end() const { return cend(); }
        iterator cbegin() const { return iterator(const_cast<segment_tree_t*>(this), metadata_begin_); }
        iterator cend() const { return iterator(const_cast<segment_tree_t*>(this), metadata_end_); }
        r_iterator rbegin() const { return r_iterator({const_cast<segment_tree_t*>(this), metadata_end_ - 1}); }
        r_iterator rend() const { return r_iterator({const_cast<segment_tree_t*>(this), metadata_begin_ - 1}); }

    private:
        // Deliberately coarse: set from every mutation path, not derived (a wrong derivation loses data).
        void mark_dirty_() noexcept { dirty_.store(true, std::memory_order_release); }

        void report_failure_(load_failure_t failure) noexcept {
            last_failure_.store(failure, std::memory_order_release);
            channel_->report(failure);
        }
        [[nodiscard]] load_failure_t last_failure_of_this_leaf_() const noexcept {
            return last_failure_.load(std::memory_order_acquire);
        }
        // Puts a VALID empty stand-in in place of an unreadable block, so callers stay memory-safe without null checks.
        void poison_segment_(it node, load_failure_t failure);
        void clear_segment_poison_(it node) noexcept;
        void ensure_loaded_(block_metadata* metadata);
        // A caller taking items OUT of a block must ask BEFORE doing that, or a refusal destroys items already out.
        [[nodiscard]] bool reserve_segments_(size_t count) noexcept;
        void abandon_leaf_(load_failure_t failure);
        [[nodiscard]] bool read_header_(filesystem::file_handle_t& file);

        // Pinned mode points at the leaf's handle; lazy mode OWNS one, closed when the operation's frame ends.
        struct file_lease_t {
            filesystem::file_handle_t* handle = nullptr;
            std::unique_ptr<filesystem::file_handle_t> opened;
            explicit operator bool() const noexcept { return handle != nullptr; }
            filesystem::file_handle_t* operator->() const noexcept { return handle; }
            filesystem::file_handle_t& operator*() const noexcept { return *handle; }
        };
        [[nodiscard]] file_lease_t lease_file_() const;
        void initialize_header_region_();
        [[nodiscard]] std::unique_ptr<segment_tree_t> split_into_(std::unique_ptr<segment_tree_t> splited_tree);
        [[nodiscard]] size_t header_region_checksum_() const;

        metadata_range find_range_(const index_t& index) const;
        void remove_range_(metadata_range range);
        [[nodiscard]] node_t construct_new_node_(const index_t& index, item_data item);
        [[nodiscard]] node_t construct_new_node_(item_data item);
        void load_segment_(block_metadata* metadata);
        void unload_old_segments_();
        [[nodiscard]] bool insert_segment_(it pos, node_t&& block);
        void remove_segment_(it pos);
        void update_metadata_(it pos, block_metadata* metadata);
        void close_gaps_();

        // flush() CLEARS THIS BEFORE writing, not after, or a concurrent mark_dirty_() mid-flush would be lost.
        std::atomic<bool> dirty_{true}; // a freshly built leaf has never been written

        // While nonzero, flush() writes nothing — the stand-ins are empty and would destroy live rows.
        std::atomic<size_t> unreadable_segments_{0};
        // The leaf gave up as a WHOLE; only a load replacing it from the file clears this.
        std::atomic<bool> abandoned_{false};
        std::atomic<load_failure_t> last_failure_{load_failure_t::none};
        failure_channel_t own_failures_;
        failure_channel_t* channel_ = &own_failures_;

        std::pmr::memory_resource* resource_;
        index_t (*key_func_)(const item_data&);
        std::vector<node_t> segments_; // will become boost::intrusive

        header_t* header_;
        block_metadata* metadata_begin_;
        block_metadata* metadata_end_;
        gap_tracker_t gap_tracker_{header_size, INVALID_SIZE};

        // Pinned mode: the handle lives here. Lazy mode: null, fs_/file_path_ below open a lease.
        std::unique_ptr<filesystem::file_handle_t> file_;
        filesystem::local_file_system_t* fs_ = nullptr;
        filesystem::path_t file_path_;
        std::vector<std::pair<std::unique_ptr<std::pmr::string>, std::unique_ptr<std::pmr::string>>> string_storage_;
    };

} // namespace core::b_plus_tree
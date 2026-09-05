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
    // Test-observable counts of LEAF flushes. One segment_tree_t is one B+tree leaf owning one
    // file, and btree_t::flush() walks every leaf, so an unneeded leaf flush still costs a header
    // write, a truncate and an fsync. `leaf_flushes_without_changes` narrows that to the flushes
    // that wrote no block — a diagnostic, not a bound: a header-only rewrite can be legitimate.
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

    // Reported here rather than asserted (asserts compile out under NDEBUG). Policy: a leaf with
    // an unreadable block serves nothing from it and refuses to flush, but stays otherwise usable.
    enum class load_failure_t : uint8_t
    {
        none = 0,
        data_corruption, // a block's stored checksum did not match the bytes that came back
        io_error,        // the file would not hand the bytes over at all
        out_of_memory,   // no memory for the block, even after evicting this leaf's residents
        capacity_exceeded, // metadata array full (max_segments); insert would overflow the header
    };

    [[nodiscard]] std::string_view to_string(load_failure_t failure) noexcept;

#ifdef DEV_MODE
    // Test-only override of max_segments (the real value needs ~2GB to fill for real: 8191 blocks
    // x 256KB). 0 restores the real limit.
    void dev_set_max_segments(size_t limit) noexcept;
    [[nodiscard]] size_t max_segments_limit() noexcept;
#endif

    // First failure wins (compare_exchange only sets from `none`); a later report doesn't
    // overwrite an unread one. btree_t shares one instance across all its leaves.
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
            return static_cast<load_failure_t>(state_.exchange(static_cast<uint8_t>(load_failure_t::none),
                                                               std::memory_order_acq_rel));
        }
        void clear() noexcept { state_.store(static_cast<uint8_t>(load_failure_t::none), std::memory_order_release); }

    private:
        std::atomic<uint8_t> state_{static_cast<uint8_t>(load_failure_t::none)};
    };

    // TODO: move memory overflow checks to b_plus_tree
    class segment_tree_t {
        struct header_t {
            // CRC32C over everything after this field (it's first, so "after" = the rest of the
            // header). flush() computes it, read_header_() verifies it before trusting the header.
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
            // Marks `block` as an empty stand-in for a read/checksum failure (never write it
            // back). Unlike leaf-wide poisoning, this travels with the node across
            // split/balance/merge; insert_segment_() refuses any node carrying it.
            bool unreadable = false;
        };
        using it = std::vector<node_t>::iterator;
        static constexpr size_t block_metadata_size = sizeof(block_metadata);

    public:
        using index_t = block_t::index_t;
        using item_data = block_t::item_data;

        // 80%
        static constexpr double merge_check = 4.0 / 5.0;
        static constexpr size_t header_size = 2 * DEFAULT_BLOCK_SIZE;
        // Bound checked in read_header_() (segments_count_ read from disk) and insert_segment_()
        // (metadata_end_ growth); without it, either could walk past the header allocation.
        // = 8191, one below MAX_NODE_CAPACITY.
        static constexpr size_t max_segments = (header_size - sizeof(header_t)) / block_metadata_size;

        // it is possible to just use segments_::iterator, but it won't work correctly if block is not loaded
        // and there won't be any overhead of node_t shown
        class iterator {
        public:
            // const segment_tree_t* will block from trying to load a block_t, if it is needed by the iterator
            iterator(segment_tree_t* seg_tree, block_metadata* metadata);
            iterator(const iterator& other);
            iterator(iterator&& other) noexcept;

            // Unlike operator*/operator->, may return nullptr: on out_of_memory the poison
            // stand-in itself can't be allocated, so the slot stays empty rather than valid-empty.
            [[nodiscard]] inline const block_t* get() {
                load_block();
                return block_;
            }
            // Assume the block is loadable; only safe after a successful get().
            inline const block_t& operator*() {
                load_block();
                return *block_;
            }
            inline const block_t* operator->() {
                load_block();
                return block_;
            }

            // Must move the same direction as the postfix form below; a mismatch stays latent
            // since all tree traversal uses postfix.
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
                // Must copy seg_tree_ too, not just metadata_: omitting it left the iterator
                // reading the OLD tree's segment table after assignment across trees.
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
            // const segment_tree_t* will block from trying to load a block_t, if it is needed by the iterator
            r_iterator(segment_tree_t* seg_tree, block_metadata* metadata);
            r_iterator(const r_iterator& other);
            r_iterator(r_iterator&& other) noexcept;

            // See iterator::get() — same nullptr contract for the allocation-refusal case.
            [[nodiscard]] inline const block_t* get() {
                load_block();
                return block_;
            }
            // Assume the block is loadable; only safe after a successful get().
            inline const block_t& operator*() {
                load_block();
                return *block_;
            }
            inline const block_t* operator->() {
                load_block();
                return block_;
            }

            // Same contract as iterator's prefix forms — must match the postfix direction below.
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
                // Same as iterator::operator=: the owning tree travels with the position.
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

        // Pinned-handle ctor: holds one fd for the leaf's whole life. Test-only fault-injection
        // seam (faulty_leaf_file_t); production uses the lazy ctor below instead.
        segment_tree_t(std::pmr::memory_resource* resource,
                       index_t (*func)(const item_data&),
                       std::unique_ptr<filesystem::file_handle_t> file);
        // Opens/closes a lease per operation instead of holding a descriptor per leaf: the pinned
        // ctor above exhausted the process fd table when many leaves ran under parallel tests.
        segment_tree_t(std::pmr::memory_resource* resource,
                       index_t (*func)(const item_data&),
                       filesystem::local_file_system_t& fs,
                       filesystem::path_t file_path);
        ~segment_tree_t();

        // will try to maintain default block size if possible
        bool append(data_ptr_t data, uint32_t size);
        bool append(item_data item);
        bool append(const index_t& index, item_data item);
        bool remove(data_ptr_t data, uint32_t size);
        bool remove(item_data item);
        bool remove(const index_t& index, item_data item);
        bool remove_index(const index_t& index);
        [[nodiscard]] std::unique_ptr<segment_tree_t> split(std::unique_ptr<filesystem::file_handle_t> file);
        // Lazy-mode twin of split() above; only a lazy-mode leaf can produce one.
        [[nodiscard]] std::unique_ptr<segment_tree_t> split(filesystem::path_t new_file_path);
        // requires other->count() > this->count()
        void balance_with(std::unique_ptr<segment_tree_t>& other);
        // false = nothing moved (unreadable block, or destination can't fit source); all-or-nothing
        // since btree_t deletes the source leaf regardless.
        [[nodiscard]] bool merge(std::unique_ptr<segment_tree_t>& other);

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
        // Persist to disk. Returns false when any of the writes, the truncate or the fsync failed;
        // the leaf then stays dirty so the next flush retries it, and the caller must treat the
        // data as NOT durable. It also returns false, without writing anything at all, while this
        // leaf holds a block it could not read back -- see poisoned_.
        [[nodiscard]] bool flush();
        // load all tree segment at once from scratch
        void clean_load();
        // clear current blocks, load only block's metadata
        void lazy_load();

        // THE REFUSAL CHANNEL, and the only way anything above this class can learn that a read
        // did not go through. Sticky: it survives the call that raised it, because the caller that
        // has to act on it is the one that asked the QUESTION, not the one that touched the block.
        // A leaf attached to a btree_t reports into the tree's channel instead of its own, so one
        // read of btree_t::load_failure() covers a walk over any number of leaves.
        [[nodiscard]] load_failure_t load_failure() const noexcept { return channel_->peek(); }
        void reset_load_failure() noexcept { channel_->clear(); }
        [[nodiscard]] failure_channel_t* failure_channel() const noexcept { return channel_; }
        // Redirect this leaf's reports. nullptr restores its own cell.
        void set_failure_channel(failure_channel_t* channel) noexcept {
            channel_ = channel != nullptr ? channel : &own_failures_;
        }
        // True while this leaf holds a block whose bytes on the device it could not read back, or
        // while it has given up as a whole. DERIVED, not remembered: a block that reads back on a
        // later attempt leaves the count, so a leaf whose only trouble was one refused read
        // becomes writable again by itself. Only abandon_leaf_() sticks until the next load.
        [[nodiscard]] bool poisoned() const noexcept {
            return abandoned_.load(std::memory_order_acquire) ||
                   unreadable_segments_.load(std::memory_order_acquire) != 0;
        }

        // segment_tree is an ordered container, data cannot be modified by iterator
        iterator begin() const { return cbegin(); }
        iterator end() const { return cend(); }
        iterator cbegin() const { return iterator(const_cast<segment_tree_t*>(this), metadata_begin_); }
        iterator cend() const { return iterator(const_cast<segment_tree_t*>(this), metadata_end_); }
        r_iterator rbegin() const { return r_iterator({const_cast<segment_tree_t*>(this), metadata_end_ - 1}); }
        r_iterator rend() const { return r_iterator({const_cast<segment_tree_t*>(this), metadata_begin_ - 1}); }

    private:
        // Set whenever anything this leaf's file would have to reflect changes: a block's
        // contents, the header, the on-disk layout, or an un-fsynced write. Deliberately coarse
        // -- one bit set from every mutation path rather than derived at flush time, since
        // deriving it would re-read state the mutation already knows and a wrong derivation
        // loses data silently at restart.
        void mark_dirty_() noexcept { dirty_.store(true, std::memory_order_release); }

        void report_failure_(load_failure_t failure) noexcept {
            last_failure_.store(failure, std::memory_order_release);
            channel_->report(failure);
        }
        [[nodiscard]] load_failure_t last_failure_of_this_leaf_() const noexcept {
            return last_failure_.load(std::memory_order_acquire);
        }
        // Put an empty stand-in in place of a block that could not be read, and say why. The
        // stand-in is a VALID block, which is what keeps every caller below memory-safe without
        // asking each of them to test for a null: it answers "nothing here" to every question.
        void poison_segment_(it node, load_failure_t failure);
        // Take a segment back out of the unreadable count. The only way in is poison_segment_(),
        // and the ways out are a read that finally worked and the segment leaving the leaf.
        void clear_segment_poison_(it node) noexcept;
        // Make sure this segment's block is resident, RE-READING one whose bytes did not arrive
        // last time -- see node_t::unreadable for why `if (!block)` is not enough.
        void ensure_loaded_(block_metadata* metadata);
        // True when insert_segment_() would take `count` more entries. False means the metadata
        // array is full: the leaf is poisoned and the reason is on the channel, exactly as
        // insert_segment_() would have left it.
        // A caller that takes items OUT of a block before it can hand them anywhere -- both
        // split_uniques() and split_append() do -- has to ask BEFORE it does that. A refusal
        // afterwards destroys the items that are already out of the block.
        [[nodiscard]] bool reserve_segments_(size_t count) noexcept;
        // Give up on the whole leaf: empty it, say why, and make sure nothing writes that
        // emptiness anywhere. Used when a block that could NOT be read is the only thing that
        // could have made this leaf's metadata usable -- see the STRING note at its definition.
        void abandon_leaf_(load_failure_t failure);
        // Read the leaf header off the file, verify its seal, and check that
        // the segment count it names fits the region that holds the metadata array.
        // False = nothing was loaded and the failure is on the channel; the leaf is left
        // empty and openable.
        [[nodiscard]] bool read_header_(filesystem::file_handle_t& file);

        // ONE OPERATION'S CLAIM ON THE LEAF'S FILE. In pinned mode it points
        // at the handle the leaf owns; in lazy mode it OWNS a handle opened for this
        // operation and closes it when the operation's frame ends. A lease that could not
        // open answers false and the operation refuses the way it refuses a failed read.
        struct file_lease_t {
            filesystem::file_handle_t* handle = nullptr;
            std::unique_ptr<filesystem::file_handle_t> opened;
            explicit operator bool() const noexcept { return handle != nullptr; }
            filesystem::file_handle_t* operator->() const noexcept { return handle; }
            filesystem::file_handle_t& operator*() const noexcept { return *handle; }
        };
        [[nodiscard]] file_lease_t lease_file_() const;
        // The shared tail of both constructors: allocate and zero the header region.
        void initialize_header_region_();
        // The tail every split shares once its destination exists.
        [[nodiscard]] std::unique_ptr<segment_tree_t> split_into_(std::unique_ptr<segment_tree_t> splited_tree);
        // CRC32C over the header region past the checksum field itself.
        [[nodiscard]] size_t header_region_checksum_() const;

        metadata_range find_range_(const index_t& index) const;
        void remove_range_(metadata_range range);
        [[nodiscard]] node_t construct_new_node_(const index_t& index, item_data item);
        [[nodiscard]] node_t construct_new_node_(item_data item);
        void load_segment_(block_metadata* metadata);
        void unload_old_segments_();
        // header changes will be handled here:
        // False = the metadata array is full and NOTHING was inserted; the leaf is poisoned, so
        // no half-built state reaches the device.
        [[nodiscard]] bool insert_segment_(it pos, node_t&& block);
        void remove_segment_(it pos);
        void update_metadata_(it pos, block_metadata* metadata);
        void close_gaps_();

        // Atomic, and flush() CLEARS IT BEFORE writing rather than after. btree_t::flush() locks
        // only tree_mutex_ and explicitly does not lock the leaves, while btree_t::append() releases
        // its node locks before mutating the leaf — so a writer can be inside a leaf while that leaf
        // is being flushed. Clearing after the write would drop a mark_dirty_() raised during it and
        // lose that change forever; clearing first only ever costs one redundant flush.
        // In otterbrix the disk index is owned by a single actor, so that race cannot happen there —
        // but core/b_plus_tree is a standalone library with its own locking and its own
        // multithreaded test, and must not depend on it.
        std::atomic<bool> dirty_{true}; // a freshly built leaf has never been written

        // Same race as dirty_ above (written by a load inside the leaf, read by flush() without
        // holding the leaf), so atomic for the same reason: a torn read decides whether an empty
        // stand-in overwrites live rows. How many blocks of this leaf could not be read back;
        // while nonzero, flush() writes nothing (the stand-ins are empty and would destroy rows
        // still on the device). A block that reads back later decrements it, so a transient
        // refusal doesn't wedge the leaf until restart. Per-leaf so one unreadable leaf doesn't
        // stop the others from writing.
        std::atomic<size_t> unreadable_segments_{0};
        // The leaf gave up as a WHOLE rather than about one block, so no re-read of one block can
        // undo it and only a load that replaces the leaf from the file clears it. Two ways in:
        // abandon_leaf_(), where the block that would have made the metadata usable did not come;
        // and the metadata array running out of room, which nothing about the leaf will fix.
        std::atomic<bool> abandoned_{false};
        // The reason THIS leaf last reported. The channel it reports into may be shared with every
        // other leaf of the tree and holds the FIRST reason anyone raised, so it cannot answer
        // "why did the block I just asked for not load".
        std::atomic<load_failure_t> last_failure_{load_failure_t::none};
        failure_channel_t own_failures_;
        failure_channel_t* channel_ = &own_failures_;

        std::pmr::memory_resource* resource_;
        index_t (*key_func_)(const item_data&);
        std::vector<node_t> segments_; // will become boost::intrusive

        header_t* header_;
        block_metadata* metadata_begin_;
        block_metadata* metadata_end_;
        // keep track of gaps in block record and try to fill them when creating new blocks
        gap_tracker_t gap_tracker_{header_size, INVALID_SIZE};

        // Pinned mode: the handle lives here. Lazy mode: this stays null and fs_ +
        // file_path_ below are how a lease opens one. Exactly one of the two shapes per
        // instance, chosen by the constructor.
        std::unique_ptr<filesystem::file_handle_t> file_;
        filesystem::local_file_system_t* fs_ = nullptr;
        filesystem::path_t file_path_;
        std::vector<std::pair<std::unique_ptr<std::pmr::string>, std::unique_ptr<std::pmr::string>>> string_storage_;
    };

} // namespace core::b_plus_tree
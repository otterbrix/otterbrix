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

    // WHAT THIS LIBRARY CAN FAIL TO DO WHEN IT READS THE DISK. Everything below used to be read
    // with the result dropped, and a block's checksum was verified only inside an assert -- so
    // under -DNDEBUG a block that never arrived, or arrived changed, was restored as if it were
    // sound: restore_block() takes header_->count_ at face value and places the metadata cursor
    // by it.
    //
    // THE RULE EVERY USE OF THIS OBEYS IS LOUD, NOT FATAL. A leaf that met a block it could not
    // read serves NOTHING out of that block rather than serving something else, refuses to flush
    // (so the empty stand-in it holds can never be written over the rows still on the device),
    // and reports the reason here. It still opens, it still answers about the blocks it CAN read,
    // and its files can still be deleted -- which is all DROP INDEX needs.
    enum class load_failure_t : uint8_t
    {
        none = 0,
        data_corruption, // a block's stored checksum did not match the bytes that came back
        io_error,        // the file would not hand the bytes over at all
        out_of_memory,   // no memory for the block, even after evicting this leaf's residents
        // The leaf's metadata array is full: max_segments entries fit in the header region and a
        // block was asked for beyond that. Reported rather than written, because writing it goes
        // straight past the header allocation.
        capacity_exceeded,
    };

    [[nodiscard]] std::string_view to_string(load_failure_t failure) noexcept;

#ifdef DEV_MODE
    // A leaf holds segment_tree_t::max_segments blocks -- 8191 of them, one per 256 KB block, so
    // filling one for real costs two gigabytes. This lowers the number the INSERT guard compares
    // against so the guard runs for real; the number it defends is the same constant either way,
    // and the load-side clamp that shares it is driven at its true value by poking a leaf header.
    // 0 restores segment_tree_t::max_segments.
    void dev_set_max_segments(size_t limit) noexcept;
    [[nodiscard]] size_t max_segments_limit() noexcept;
#endif

    // One sticky cell. FIRST FAILURE WINS: a later, milder reason must not overwrite the reason a
    // caller has not read yet, and a caller that reads it gets the same answer as long as it does
    // not clear it. btree_t points every one of its leaves at a single one of these, so a walk
    // that crosses many leaves has one place to look afterwards.
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
            // THE SEAL OF THE WHOLE HEADER REGION (wave #325). Everything a range lookup
            // stands on -- the three counters AND the block_metadata array behind them,
            // min_index/max_index boundaries included -- lives in this region, and until
            // this field existed none of it was covered by any checksum: a flipped bit in
            // a key boundary passed the single segments-count bound check and mis-routed
            // every later lookup, silently. CRC32C over the region past this field,
            // computed by flush() as the region is written and verified by read_header_()
            // before one byte of it is believed. First field, so the coverage arithmetic
            // is simply "everything after it".
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
            // The block in memory is NOT what the file holds: the read was refused, or the bytes
            // that came back failed their checksum, and `block` is the empty stand-in that took
            // its place. Nothing may ever write it back -- that would put the stand-in over the
            // only copy of the rows.
            //
            // THE FLAG TRAVELS WITH THE BLOCK, and the leaf-wide refusal does not: split(),
            // balance_with() and merge() move whole node_t values into ANOTHER leaf, which is
            // clean and flushes happily. So this flag, not the leaf's, is what the writer and
            // every structural walk have to look at -- and insert_segment_() refuses a node
            // carrying it outright.
            //
            // It is also what makes the stand-in RETRYABLE: the slot is filled, so no
            // `if (!block)` guard ever asks about it again. ensure_loaded_() asks about this
            // instead, so a transient refusal stops answering "nothing here" once the device
            // is fine again.
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
        // HOW MANY BLOCK METADATA ENTRIES THE HEADER REGION ACTUALLY HOLDS, spelled out where
        // something can compare against it. Two things used to take it on faith:
        //   - header_->segments_count_ comes off the DISK and places metadata_end_, so a count
        //     larger than this walked past the allocation on every later lookup;
        //   - insert_segment_ moved metadata_end_ forward with nothing stopping it at the end.
        // The comment that stood here said "2^14 - 1 block capacity", which was never true of this
        // arithmetic -- it is 8191, one BELOW MAX_NODE_CAPACITY, and blocks per leaf are bounded by
        // unique indices per leaf, which btree_t bounds by max_node_capacity_.
        static constexpr size_t max_segments = (header_size - sizeof(header_t)) / block_metadata_size;

        // it is possible to just use segments_::iterator, but it won't work correctly if block is not loaded
        // and there won't be any overhead of node_t shown
        class iterator {
        public:
            // const segment_tree_t* will block from trying to load a block_t, if it is needed by the iterator
            iterator(segment_tree_t* seg_tree, block_metadata* metadata);
            iterator(const iterator& other);
            iterator(iterator&& other) noexcept;

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
            friend long int operator-(const iterator& lhs, const iterator& rhs) {
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
            r_iterator(const r_iterator& other);
            r_iterator(r_iterator&& other) noexcept;

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
            friend long int operator-(const r_iterator& lhs, const r_iterator& rhs) {
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

        // PINNED-HANDLE MODE: this leaf holds the handle it is given for its whole
        // life. It is the FAULT-INJECTION SEAM of the unit tests (faulty_leaf_file_t
        // wraps a handle and refuses chosen block reads/writes), and nothing in
        // production uses it any more -- see the lazy ctor below.
        segment_tree_t(std::pmr::memory_resource* resource,
                       index_t (*func)(const item_data&),
                       std::unique_ptr<filesystem::file_handle_t> file);
        // LAZY MODE (wave #326): the leaf remembers WHERE its file is and holds NO
        // descriptor at rest. One segment_tree_t is one B+tree leaf, and the pinned mode
        // above made that one permanently open descriptor per leaf -- a tree of N leaves
        // held N descriptors for the life of the process, and under a parallel test run
        // the process descriptor table was exhausted by neighbours and surfaced as
        // "file could not be opened" inside unrelated stores. Every operation that
        // touches the file takes a lease (open, use, close); block loads are 256 KB
        // reads, so the open beside them is noise.
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
        // The lazy-mode twin: the split-off half is built over a PATH and opens its file
        // only when an operation needs it. Only a lazy-mode leaf can hand out one.
        [[nodiscard]] std::unique_ptr<segment_tree_t> split(filesystem::path_t new_file_path);
        // requires other->count() > this->count()
        void balance_with(std::unique_ptr<segment_tree_t>& other);
        // False = NOTHING was moved: one side holds a block it could not read, or the destination
        // cannot hold the source's segments. All or nothing, because btree_t deletes the source.
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
        // Set whenever anything this leaf's FILE would have to reflect has changed: a block's
        // contents, the header (segment count / per-block metadata), the on-disk layout, or a
        // write that has not been fsynced yet.
        //
        // Deliberately coarse — one bit per leaf, set from every mutation path rather than derived
        // at flush time. Deriving it would have to re-read state the mutation already knows about,
        // and a wrong derivation loses data silently at restart. mark_dirty_() is the single place
        // that sets it, so the paths stay greppable.
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
        //
        // A caller that takes items OUT of a block before it can hand them anywhere -- both
        // split_uniques() and split_append() do -- has to ask BEFORE it does that. A refusal
        // afterwards destroys the items that are already out of the block.
        [[nodiscard]] bool reserve_segments_(size_t count) noexcept;
        // Give up on the whole leaf: empty it, say why, and make sure nothing writes that
        // emptiness anywhere. Used when a block that could NOT be read is the only thing that
        // could have made this leaf's metadata usable -- see the STRING note at its definition.
        void abandon_leaf_(load_failure_t failure);
        // Read the leaf header off the file, verify its seal (wave #325), and check that
        // the segment count it names fits the region that holds the metadata array.
        // False = nothing was loaded and the failure is on the channel; the leaf is left
        // empty and openable.
        [[nodiscard]] bool read_header_(filesystem::file_handle_t& file);

        // ONE OPERATION'S CLAIM ON THE LEAF'S FILE (wave #326). In pinned mode it points
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
        //
        // In otterbrix the disk index is owned by a single actor, so that race cannot happen there —
        // but core/b_plus_tree is a standalone library with its own locking and its own
        // multithreaded test, and must not depend on it.
        std::atomic<bool> dirty_{true}; // a freshly built leaf has never been written

        // THE SAME RACE THE COMMENT ABOVE dirty_ DESCRIBES, and the reason these are atomic too:
        // they are written by a load running inside the leaf and read by flush(), which does not
        // hold the leaf. A torn read here decides whether the empty stand-in gets written over
        // the rows, so it is not a flag that may be approximately right.
        //
        // How many blocks of this leaf could not be read back. While it is not zero, flush()
        // writes nothing -- the stand-ins in memory are empty and writing them would destroy the
        // rows that are still on the device. A block that reads back on a later attempt leaves
        // the count, so a TRANSIENT refusal stops making the leaf unwritable by itself; that is
        // the whole difference between "loud" and "wedged until the process restarts".
        // Per-leaf on purpose -- one unreadable leaf must not stop the other leaves from writing.
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
#pragma once

#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>

#include <core/result_wrapper.hpp>

#include "buffer_handle.hpp"
#include "file_buffer.hpp"

namespace components::table::storage {

    // "no scratch-file copy". Matches temporary_spill_file_t::INVALID_SLOT; kept here so
    // block_handle.hpp does not have to include the spill file's header.
    inline constexpr uint64_t INVALID_TEMP_SLOT = UINT64_MAX;

    class block_manager_t;
    class buffer_pool_t;
    class buffer_handle_t;

    enum class block_state : uint8_t
    {
        UNLOADED = 0,
        LOADED = 1
    };
    enum class memory_tag : uint8_t
    {
        BASE_TABLE = 0,
        HASH_TABLE = 1,
        PARQUET_READER = 2,
        CSV_READER = 3,
        ORDER_BY = 4,
        ART_INDEX = 5,
        COLUMN_DATA = 6,
        METADATA = 7,
        OVERFLOW_STRINGS = 8,
        IN_MEMORY_TABLE = 9,
        ALLOCATOR = 10,
        EXTENSION = 11,
        TRANSACTION = 12,
        MEMORY_TAG_COUNT = 13
    };
    enum class destroy_buffer_condition : uint8_t
    {
        BLOCK = 0,
        EVICTION = 1,
        UNPIN = 2
    };

    struct buffer_pool_reservation_t {
        memory_tag tag;
        uint64_t size{0};
        buffer_pool_t& pool;

        buffer_pool_reservation_t(memory_tag tag, buffer_pool_t& pool);
        buffer_pool_reservation_t(const buffer_pool_reservation_t&) = delete;
        buffer_pool_reservation_t& operator=(const buffer_pool_reservation_t&) = delete;
        buffer_pool_reservation_t(buffer_pool_reservation_t&&) noexcept;
        buffer_pool_reservation_t& operator=(buffer_pool_reservation_t&&) noexcept;
        ~buffer_pool_reservation_t();

        void resize(uint64_t new_size);
        void merge(buffer_pool_reservation_t src);
    };

    struct temp_buffer_pool_reservation_t : buffer_pool_reservation_t {
        temp_buffer_pool_reservation_t(memory_tag tag, buffer_pool_t& pool, uint64_t size)
            : buffer_pool_reservation_t(tag, pool) {
            resize(size);
        }
        temp_buffer_pool_reservation_t(temp_buffer_pool_reservation_t&&) = default;
        ~temp_buffer_pool_reservation_t() { resize(0); }
    };

    class block_handle_t : public std::enable_shared_from_this<block_handle_t> {
    public:
        block_handle_t(block_manager_t& block_manager, uint64_t block_id, memory_tag tag);
        block_handle_t(block_manager_t& block_manager,
                       uint64_t block_id,
                       memory_tag tag,
                       std::unique_ptr<file_buffer_t> buffer,
                       destroy_buffer_condition destroy_buffer_condition,
                       uint64_t block_size,
                       buffer_pool_reservation_t&& reservation);
        ~block_handle_t();

        uint64_t block_id() const { return block_id_; }

        uint64_t eviction_sequence_number() const { return eviction_seq_num_; }

        uint64_t next_eviction_sequence_number() { return ++eviction_seq_num_; }

        int32_t readers() const { return readers_; }
        int32_t decrement_readers() { return --readers_; }

        bool is_swizzled() const { return !unswizzled_; }

        void set_swizzled(const char* unswizzler) { unswizzled_ = unswizzler; }

        memory_tag get_memory_tag() const { return tag_; }

        void set_destroy_buffer_condition(destroy_buffer_condition destroy_buffer_upon) {
            destroy_condition_ = destroy_buffer_upon;
        }

        bool must_add_to_eviction_queue() const { return destroy_condition_ != destroy_buffer_condition::UNPIN; }

        // Reloadable iff a disk copy exists (block_id < MAXIMUM_BLOCK): load() can re-read it via the block
        // manager. Managed in-memory blocks (block_id >= MAXIMUM_BLOCK) have no backing store -- load() returns
        // {} for them, so they must never be unloaded/evicted (a later re-pin would deref a null buffer).
        // Same condition load() uses.
        bool is_reloadable() const { return block_id_ < MAXIMUM_BLOCK; }

        // Spill state. Deliberately SEPARATE from block_id_/is_reloadable(): that expression means
        // four different things around the tree ("has a disk copy", "is shared/read-only", "already
        // written through", "is a real .otbx id, free it on compact"), and widening it to cover
        // temporary copies would silently flip all four. Worse, a temp id would then reach
        // single_file_block_manager_t::block_location, where (2^62 + N) * block_size overflows onto
        // exactly real block N — corruption that reads back with a valid checksum.
        //
        // So a spilled block keeps its transient identity and gains a slot in the pool's scratch
        // file instead.
        bool has_temp_copy() const { return temp_slot_ != INVALID_TEMP_SLOT; }
        uint64_t temp_slot() const { return temp_slot_; }
        uint64_t temp_size() const { return temp_size_; }
        // `bytes` is what was written to the scratch file (the whole allocation); `user_size` is
        // the logical size the buffer was created with. construct_manager_buffer() derives the
        // allocation from the logical size and asserts they agree, so both have to be remembered.
        void set_temp_copy(uint64_t slot, uint64_t bytes, uint64_t user_size) {
            temp_slot_ = slot;
            temp_size_ = bytes;
            temp_user_size_ = user_size;
        }
        void clear_temp_copy() {
            temp_slot_ = INVALID_TEMP_SLOT;
            temp_size_ = 0;
            temp_user_size_ = 0;
        }

        // A resident transient buffer nobody is reading can be written to the scratch file and its
        // memory reclaimed. can_unload() covers the disk-backed case; this covers the case that used
        // to have no answer at all and left the pool with nothing to evict.
        bool can_spill() const {
            return state_ == block_state::LOADED && readers_ == 0 && buffer_ != nullptr && !is_reloadable();
        }

        uint64_t memory_usage() const { return memory_usage_; }

        bool is_unloaded() const { return state_ == block_state::UNLOADED; }

        void set_eviction_queue_index(uint64_t index) {
            // can only be set once
            assert(eviction_queue_idx_ == INVALID_INDEX);
            // Any buffer type can be queued now that a transient one can be spilled; the queue is
            // chosen by eviction_queue_for_handle, which already routes all three types.
            assert(buffer_type() != file_buffer_type::BLOCK || is_reloadable());
            eviction_queue_idx_ = index;
        }

        uint64_t eviction_queue_index() const { return eviction_queue_idx_; }

        file_buffer_type buffer_type() const { return buffer_type_; }

        block_state state() const { return state_; }

        int64_t LRU_timestamp() const { return lru_timestamp_msec_; }

        void set_LRU_timestamp(int64_t timestamp_msec) { lru_timestamp_msec_ = timestamp_msec; }

        std::unique_lock<std::mutex> get_lock() { return std::unique_lock(lock_); }

        std::unique_ptr<file_buffer_t>& get_buffer(std::unique_lock<std::mutex>& l);

        void change_memory_usage(std::unique_lock<std::mutex>& l, int64_t delta);
        buffer_pool_reservation_t& memory_usage(std::unique_lock<std::mutex>& l);
        void merge_memory_reservation(std::unique_lock<std::mutex>&, buffer_pool_reservation_t reservation);
        void resize_memory(std::unique_lock<std::mutex>&, uint64_t alloc_size);

        void resize_buffer(std::unique_lock<std::mutex>&, uint64_t block_size, int64_t memory_delta);
        // Reload-from-disk path (block_id < MAXIMUM_BLOCK) calls block_manager.read(), which can fail with
        // data_corruption/io_error -- forwarded here instead of throwing. Already-LOADED and
        // managed-in-memory ({}) fast paths return a valid handle with no error.
        [[nodiscard]] core::result_wrapper_t<buffer_handle_t> load(std::unique_ptr<file_buffer_t> buffer = nullptr);
        buffer_handle_t load_from_buffer(std::unique_lock<std::mutex>& l,
                                         std::byte* data,
                                         std::unique_ptr<file_buffer_t> reusable_buffer,
                                         buffer_pool_reservation_t reservation);
        std::unique_ptr<file_buffer_t> unload_and_take_block(std::unique_lock<std::mutex>&);
        void unload(std::unique_lock<std::mutex>&);

        bool can_unload() const;

        block_manager_t& block_manager;

    private:
        std::mutex lock_;
        std::atomic<block_state> state_;
        std::atomic<int32_t> readers_;
        uint64_t block_id_;
        const memory_tag tag_;
        const file_buffer_type buffer_type_;
        std::unique_ptr<file_buffer_t> buffer_;
        std::atomic<uint64_t> eviction_seq_num_;
        std::atomic<int64_t> lru_timestamp_msec_;
        std::atomic<destroy_buffer_condition> destroy_condition_;
        std::atomic<uint64_t> memory_usage_;
        uint64_t temp_slot_{INVALID_TEMP_SLOT};
        uint64_t temp_size_{0};
        uint64_t temp_user_size_{0};
        buffer_pool_reservation_t memory_charge_;
        const char* unswizzled_;
        std::atomic<uint64_t> eviction_queue_idx_;
    };

} //namespace components::table::storage
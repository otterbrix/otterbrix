#pragma once

#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>

#include <core/result_wrapper.hpp>

#include "buffer_handle.hpp"
#include "file_buffer.hpp"

namespace components::table::storage {

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

        uint64_t memory_usage() const { return memory_usage_; }

        bool is_unloaded() const { return state_ == block_state::UNLOADED; }

        void set_eviction_queue_index(uint64_t index) {
            // can only be set once
            assert(eviction_queue_idx_ == INVALID_INDEX);
            assert(buffer_type() == file_buffer_type::MANAGED_BUFFER);
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
        buffer_pool_reservation_t memory_charge_;
        const char* unswizzled_;
        std::atomic<uint64_t> eviction_queue_idx_;
    };

} //namespace components::table::storage
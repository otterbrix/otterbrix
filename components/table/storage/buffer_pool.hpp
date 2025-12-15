#pragma once

#include "block_handle.hpp"

#include <array>
#include <queue>
#include <thread>

namespace components::table::storage {

    struct temp_buffer_pool_reservation_t;

    struct buffer_eviction_node_t {
        buffer_eviction_node_t() = default;
        buffer_eviction_node_t(std::weak_ptr<block_handle_t> handle, uint64_t eviction_seq_num);

        std::weak_ptr<block_handle_t> handle;
        uint64_t handle_sequence_number;

        bool can_unload(block_handle_t& handle);
        std::shared_ptr<block_handle_t> try_get_block_handle();
    };

    struct eviction_queue_t {
        explicit eviction_queue_t(const file_buffer_type file_buffer_type)
            : buffer_type(file_buffer_type)
            , evict_queue_insertions_(0)
            , total_dead_nodes_(0) {}

        bool add_to_eviction_queue(buffer_eviction_node_t&& node);
        bool try_dequeue_with_lock(buffer_eviction_node_t& node);
        void purge();
        template<typename FN>
        void iterate_unloadable_blocks(FN fn);

        void increment_dead_nodes() { total_dead_nodes_++; }
        void decrement_dead_nodes() { total_dead_nodes_--; }

    private:
        void purge_iteration(uint64_t purge_size);

    public:
        const file_buffer_type buffer_type;
        std::queue<buffer_eviction_node_t> q;

    private:
        constexpr static uint64_t INSERT_INTERVAL = 4096;
        constexpr static uint64_t PURGE_SIZE_MULTIPLIER = 2;
        constexpr static uint64_t EARLY_OUT_MULTIPLIER = 4;
        constexpr static uint64_t ALIVE_NODE_MULTIPLIER = 4;

        std::atomic<uint64_t> evict_queue_insertions_;
        std::atomic<uint64_t> total_dead_nodes_;
        std::mutex purge_lock_;
        std::vector<buffer_eviction_node_t> purge_nodes_;
    };

    inline uint64_t estimated_CPU_id() { return std::hash<std::thread::id>()(std::this_thread::get_id()); }

    class buffer_pool_t {
        friend class block_handle_t;
        friend class block_manager_t;
        friend class buffer_manager_t;
        friend class standard_buffer_manager_t;

    public:
        buffer_pool_t(std::pmr::memory_resource* resource,
                      uint64_t maximum_memory,
                      bool track_eviction_timestamps,
                      uint64_t allocator_bulk_deallocation_flush_threshold);

        void set_limit(uint64_t limit);

        void set_allocator_bulk_dealloc_flush_threashold(uint64_t threshold);
        uint64_t get_allocator_bulk_dealloc_flush_threashold();

        void update_used_memory(memory_tag tag, int64_t size);

        uint64_t used_memory() const;

        uint64_t max_memory() const;

        uint64_t query_max_memory() const;

    protected:
        struct eviction_result {
            bool success;
            temp_buffer_pool_reservation_t reservation;
        };
        eviction_result evict_blocks(memory_tag tag,
                                     uint64_t extra_memory,
                                     uint64_t memory_limit,
                                     std::unique_ptr<file_buffer_t>* buffer = nullptr);
        eviction_result evict_blocks_internal(eviction_queue_t& queue,
                                              memory_tag tag,
                                              uint64_t extra_memory,
                                              uint64_t memory_limit,
                                              std::unique_ptr<file_buffer_t>* buffer = nullptr);

        uint64_t purge_aged_blocks(uint32_t max_age_sec);
        uint64_t purge_aged_blocks_internal(eviction_queue_t& queue, uint32_t max_age_sec, int64_t now, int64_t limit);
        void purge_queue(const block_handle_t& handle);
        bool add_to_eviction_queue(std::shared_ptr<block_handle_t>& handle);
        eviction_queue_t& eviction_queue_for_handle(const block_handle_t& handle);
        void increment_dead_nodes(const block_handle_t& handle);

        static constexpr uint64_t BLOCK_QUEUE_SIZE = 1;
        static constexpr uint64_t MANAGED_BUFFER_QUEUE_SIZE = 6;
        static constexpr uint64_t TINY_BUFFER_QUEUE_SIZE = 1;
        const std::array<uint64_t, FILE_BUFFER_TYPE_COUNT> eviction_queue_sizes;

        enum class memory_usage_caches
        {
            FLUSH,
            NO_FLUSH,
        };

        struct memory_usage_t {
            static constexpr uint64_t MEMORY_USAGE_CACHE_COUNT = 64;
            static constexpr uint64_t MEMORY_USAGE_CACHE_THRESHOLD = 32 << 10;
            static constexpr uint64_t TOTAL_MEMORY_USAGE_INDEX = static_cast<uint64_t>(memory_tag::MEMORY_TAG_COUNT);
            using memory_usage_counters_t =
                std::array<std::atomic<int64_t>, static_cast<uint64_t>(memory_tag::MEMORY_TAG_COUNT) + 1>;

            memory_usage_counters_t memory_usage;
            std::array<memory_usage_counters_t, MEMORY_USAGE_CACHE_COUNT> memory_usage_caches_array;

            memory_usage_t();

            uint64_t used_memory(memory_usage_caches cache) { return used_memory(TOTAL_MEMORY_USAGE_INDEX, cache); }

            uint64_t used_memory(memory_tag tag, memory_usage_caches cache) {
                return used_memory(static_cast<uint64_t>(tag), cache);
            }

            uint64_t used_memory(uint64_t index, memory_usage_caches cache) {
                if (cache == memory_usage_caches::NO_FLUSH) {
                    auto used_memory = memory_usage[index].load(std::memory_order_relaxed);
                    return used_memory > 0 ? static_cast<uint64_t>(used_memory) : 0;
                }
                int64_t cached = 0;
                for (auto& cache_elem : memory_usage_caches_array) {
                    cached += cache_elem[index].exchange(0, std::memory_order_relaxed);
                }
                auto used_memory = memory_usage[index].fetch_add(cached, std::memory_order_relaxed) + cached;
                return used_memory > 0 ? static_cast<uint64_t>(used_memory) : 0;
            }

            void update_used_memory(memory_tag tag, int64_t size);
        };

        std::pmr::memory_resource* resource;
        std::mutex limit_lock;
        std::atomic<uint64_t> maximum_memory;
        std::atomic<uint64_t> allocator_bulk_deallocation_flush_threshold;
        bool track_eviction_timestamps;
        std::vector<std::unique_ptr<eviction_queue_t>> queues;
        mutable memory_usage_t memory_usage;
    };

} // namespace components::table::storage
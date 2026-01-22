#include "buffer_pool.hpp"
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace components::table::storage {

    buffer_eviction_node_t::buffer_eviction_node_t(std::weak_ptr<block_handle_t> handle, uint64_t eviction_seq_num)
        : handle(std::move(handle))
        , handle_sequence_number(eviction_seq_num) {
        assert(!this->handle.expired());
    }

    bool buffer_eviction_node_t::can_unload(block_handle_t& handle) {
        if (handle_sequence_number != handle.eviction_sequence_number()) {
            return false;
        }
        return handle.can_unload();
    }

    std::shared_ptr<block_handle_t> buffer_eviction_node_t::try_get_block_handle() {
        auto handle_ptr = handle.lock();
        if (!handle_ptr) {
            return nullptr;
        }
        if (!can_unload(*handle_ptr)) {
            return nullptr;
        }
        return handle_ptr;
    }

    bool eviction_queue_t::add_to_eviction_queue(buffer_eviction_node_t&& node) {
        q.push(std::move(node));
        return ++evict_queue_insertions_ % INSERT_INTERVAL == 0;
    }

    bool eviction_queue_t::try_dequeue_with_lock(buffer_eviction_node_t& node) {
        std::lock_guard lock(purge_lock_);
        if (q.empty()) {
            return false;
        }
        node = std::move(q.front());
        q.pop();
        return true;
    }

    void eviction_queue_t::purge() {
        if (!purge_lock_.try_lock()) {
            return;
        }
        std::lock_guard lock{purge_lock_, std::adopt_lock};

        uint64_t purge_size = INSERT_INTERVAL * PURGE_SIZE_MULTIPLIER;
        uint64_t approx_q_size = q.size();

        if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
            return;
        }

        uint64_t max_purges = approx_q_size / purge_size;
        while (max_purges != 0) {
            purge_iteration(purge_size);

            approx_q_size = q.size();

            if (approx_q_size < purge_size * EARLY_OUT_MULTIPLIER) {
                break;
            }

            uint64_t approx_dead_nodes = total_dead_nodes_;
            approx_dead_nodes = approx_dead_nodes > approx_q_size ? approx_q_size : approx_dead_nodes;
            uint64_t approx_alive_nodes = approx_q_size - approx_dead_nodes;

            if (approx_alive_nodes * (ALIVE_NODE_MULTIPLIER - 1) > approx_dead_nodes) {
                break;
            }

            max_purges--;
        }
    }

    void eviction_queue_t::purge_iteration(uint64_t purge_size) {
        uint64_t previous_purge_size = purge_nodes_.size();
        if (purge_size < previous_purge_size / 2 || purge_size > previous_purge_size) {
            purge_nodes_.resize(purge_size);
        }

        uint64_t actually_dequeued = purge_size;
        auto it = purge_nodes_.begin();
        for (size_t i = 0; i < purge_size; i++) {
            if (q.empty()) {
                actually_dequeued = i;
                break;
            }
            *it = std::move(q.front());
            q.pop();
            ++it;
        }

        uint64_t alive_nodes = 0;
        for (uint64_t i = 0; i < actually_dequeued; i++) {
            auto& node = purge_nodes_[i];
            auto handle = node.try_get_block_handle();
            if (handle) {
                q.push(std::move(node));
                alive_nodes++;
            }
        }

        total_dead_nodes_ -= actually_dequeued - alive_nodes;
    }

    template<typename FN>
    void eviction_queue_t::iterate_unloadable_blocks(FN fn) {
        for (;;) {
            buffer_eviction_node_t node;
            if (q.empty()) {
                if (!try_dequeue_with_lock(node)) {
                    return;
                }
            }
            node = std::move(q.front());
            q.pop();

            auto handle = node.try_get_block_handle();
            if (!handle) {
                decrement_dead_nodes();
                continue;
            }

            auto lock = handle->get_lock();
            if (!node.can_unload(*handle)) {
                decrement_dead_nodes();
                continue;
            }

            if (!fn(node, handle, lock)) {
                break;
            }
        }
    }

    buffer_pool_t::buffer_pool_t(std::pmr::memory_resource* resource,
                                 uint64_t maximum_memory,
                                 bool track_eviction_timestamps,
                                 uint64_t allocator_bulk_deallocation_flush_threshold)
        : eviction_queue_sizes({BLOCK_QUEUE_SIZE, MANAGED_BUFFER_QUEUE_SIZE, TINY_BUFFER_QUEUE_SIZE})
        , resource(resource)
        , maximum_memory(maximum_memory)
        , allocator_bulk_deallocation_flush_threshold(allocator_bulk_deallocation_flush_threshold)
        , track_eviction_timestamps(track_eviction_timestamps) {
        for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
            const auto type = static_cast<file_buffer_type>(type_idx + 1);
            const auto& type_queue_size = eviction_queue_sizes[type_idx];
            for (uint64_t queue_idx = 0; queue_idx < type_queue_size; queue_idx++) {
                queues.push_back(std::make_unique<eviction_queue_t>(type));
            }
        }
    }

    void buffer_pool_t::set_limit(uint64_t limit) {
        std::lock_guard l_lock(limit_lock);
        if (!evict_blocks(memory_tag::EXTENSION, 0, limit).success) {
            throw std::runtime_error("Failed to change memory limit to " + std::to_string(limit) +
                                     ": could not free up enough memory for the new limit");
        }
        uint64_t old_limit = maximum_memory;
        maximum_memory = limit;
        if (!evict_blocks(memory_tag::EXTENSION, 0, limit).success) {
            maximum_memory = old_limit;
            throw std::runtime_error("Failed to change memory limit to " + std::to_string(limit) +
                                     ": could not free up enough memory for the new limit");
        }
    }

    void buffer_pool_t::set_allocator_bulk_dealloc_flush_threashold(uint64_t threshold) {
        allocator_bulk_deallocation_flush_threshold = threshold;
    }

    uint64_t buffer_pool_t::get_allocator_bulk_dealloc_flush_threashold() {
        return allocator_bulk_deallocation_flush_threshold;
    }

    void buffer_pool_t::update_used_memory(memory_tag tag, int64_t size) { memory_usage.update_used_memory(tag, size); }

    uint64_t buffer_pool_t::used_memory() const { return memory_usage.used_memory(memory_usage_caches::FLUSH); }

    uint64_t buffer_pool_t::max_memory() const { return maximum_memory; }

    uint64_t buffer_pool_t::query_max_memory() const { return max_memory(); }

    buffer_pool_t::eviction_result buffer_pool_t::evict_blocks(memory_tag tag,
                                                               uint64_t extra_memory,
                                                               uint64_t memory_limit,
                                                               std::unique_ptr<file_buffer_t>* buffer) {
        for (auto& queue : queues) {
            auto block_result = evict_blocks_internal(*queue, tag, extra_memory, memory_limit, buffer);
            if (block_result.success || queue.get() == queues.back().get()) {
                return block_result;
            }
        }
        throw std::runtime_error(
            "Exited buffer_pool_t::evict_blocks_internal without obtaining buffer_pool_t::eviction_result");
    }

    buffer_pool_t::eviction_result buffer_pool_t::evict_blocks_internal(eviction_queue_t& queue,
                                                                        memory_tag tag,
                                                                        uint64_t extra_memory,
                                                                        uint64_t memory_limit,
                                                                        std::unique_ptr<file_buffer_t>* buffer) {
        temp_buffer_pool_reservation_t r(tag, *this, extra_memory);
        bool found = false;

        if (memory_usage.used_memory(memory_usage_caches::NO_FLUSH) <= memory_limit) {
            return {true, std::move(r)};
        }

        queue.iterate_unloadable_blocks([&](buffer_eviction_node_t&,
                                            const std::shared_ptr<block_handle_t>& handle,
                                            std::unique_lock<std::mutex>& lock) {
            if (buffer && handle->get_buffer(lock)->allocation_size() == extra_memory) {
                *buffer = handle->unload_and_take_block(lock);
                found = true;
                return false;
            }

            handle->unload(lock);

            if (memory_usage.used_memory(memory_usage_caches::NO_FLUSH) <= memory_limit) {
                found = true;
                return false;
            }

            return true;
        });

        if (!found) {
            r.resize(0);
        }

        return {found, std::move(r)};
    }

    uint64_t buffer_pool_t::purge_aged_blocks(uint32_t max_age_sec) {
        int64_t now = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
                          .time_since_epoch()
                          .count();
        int64_t limit = now - (static_cast<int64_t>(max_age_sec) * 1000);
        uint64_t purged_bytes = 0;
        for (auto& queue : queues) {
            purged_bytes += purge_aged_blocks_internal(*queue, max_age_sec, now, limit);
        }
        return purged_bytes;
    }

    uint64_t buffer_pool_t::purge_aged_blocks_internal(eviction_queue_t& queue, uint32_t, int64_t now, int64_t limit) {
        uint64_t purged_bytes = 0;
        queue.iterate_unloadable_blocks([&](buffer_eviction_node_t&,
                                            const std::shared_ptr<block_handle_t>& handle,
                                            std::unique_lock<std::mutex>& lock) {
            auto lru_timestamp_msec = handle->LRU_timestamp();
            bool is_fresh = lru_timestamp_msec >= limit && lru_timestamp_msec <= now;
            purged_bytes += handle->memory_usage();
            handle->unload(lock);
            return !is_fresh;
        });
        return purged_bytes;
    }

    void buffer_pool_t::purge_queue(const block_handle_t& handle) { eviction_queue_for_handle(handle).purge(); }

    bool buffer_pool_t::add_to_eviction_queue(std::shared_ptr<block_handle_t>& handle) {
        auto& queue = eviction_queue_for_handle(*handle);
        assert(handle->readers() == 0);
        auto ts = handle->next_eviction_sequence_number();
        if (track_eviction_timestamps) {
            handle->set_LRU_timestamp(
                std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
                    .time_since_epoch()
                    .count());
        }

        if (ts != 1) {
            queue.increment_dead_nodes();
        }

        return queue.add_to_eviction_queue(buffer_eviction_node_t(std::weak_ptr(handle), ts));
    }

    eviction_queue_t& buffer_pool_t::eviction_queue_for_handle(const block_handle_t& handle) {
        const auto& handle_buffer_type = handle.buffer_type();
        uint64_t queue_index = 0;
        for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
            const auto queue_buffer_type = static_cast<file_buffer_type>(type_idx + 1);
            if (handle_buffer_type == queue_buffer_type) {
                break;
            }
        const auto& type_queue_size = eviction_queue_sizes[type_idx];
            queue_index += type_queue_size;
        }

        const auto& queue_size = eviction_queue_sizes[static_cast<size_t>(static_cast<uint8_t>(handle_buffer_type) - 1)];
        auto eviction_queue_idx = handle.eviction_queue_index();
        if (eviction_queue_idx < queue_size) {
            queue_index += queue_size - eviction_queue_idx - 1;
        }

        assert(queues[queue_index]->buffer_type == handle_buffer_type);
        return *queues[queue_index];
    }

    void buffer_pool_t::increment_dead_nodes(const block_handle_t& handle) {
        eviction_queue_for_handle(handle).increment_dead_nodes();
    }

    buffer_pool_t::memory_usage_t::memory_usage_t() {
        for (auto& v : memory_usage) {
            v = 0;
        }
        for (auto& cache : memory_usage_caches_array) {
            for (auto& v : cache) {
                v = 0;
            }
        }
    }

    void buffer_pool_t::memory_usage_t::update_used_memory(memory_tag tag, int64_t size) {
        auto tag_idx = static_cast<uint64_t>(tag);
        if (static_cast<uint64_t>(std::abs(size)) < MEMORY_USAGE_CACHE_THRESHOLD) {
            auto cache_idx = estimated_CPU_id() % MEMORY_USAGE_CACHE_COUNT;
            auto& cache = memory_usage_caches_array[cache_idx];
            auto new_tag_size = cache[tag_idx].fetch_add(size, std::memory_order_relaxed) + size;
            if (static_cast<uint64_t>(std::abs(new_tag_size)) >= MEMORY_USAGE_CACHE_THRESHOLD) {
                auto tag_size = cache[tag_idx].exchange(0, std::memory_order_relaxed);
                memory_usage[tag_idx].fetch_add(tag_size, std::memory_order_relaxed);
            }
            auto new_total_size = cache[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size, std::memory_order_relaxed) + size;
            if (static_cast<uint64_t>(std::abs(new_total_size)) >= MEMORY_USAGE_CACHE_THRESHOLD) {
                auto total_size = cache[TOTAL_MEMORY_USAGE_INDEX].exchange(0, std::memory_order_relaxed);
                memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(total_size, std::memory_order_relaxed);
            }
        } else {
            memory_usage[tag_idx].fetch_add(size, std::memory_order_relaxed);
            memory_usage[TOTAL_MEMORY_USAGE_INDEX].fetch_add(size, std::memory_order_relaxed);
        }
    }

} // namespace components::table::storage
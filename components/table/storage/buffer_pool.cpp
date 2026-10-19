#include "buffer_pool.hpp"
#include <algorithm>
#include <cstdlib>
#include <new>
#include <stdexcept>
#include <string>

namespace components::table::storage {

    buffer_eviction_node_t::buffer_eviction_node_t(std::weak_ptr<block_handle_t> handle, uint64_t eviction_seq_num)
        : handle(std::move(handle))
        , handle_sequence_number(eviction_seq_num) {
        assert(!this->handle.expired());
    }

    bool buffer_eviction_node_t::is_current() const {
        auto handle_ptr = handle.lock();
        return handle_ptr && is_current(*handle_ptr);
    }

    eviction_queue_t::~eviction_queue_t() {
        buffer_eviction_node_t* ptr = nullptr;
        while (q.pop(ptr)) {
            destroy_node(ptr);
        }
    }

    void eviction_queue_t::enqueue(buffer_eviction_node_t&& node) {
        auto* ptr = static_cast<buffer_eviction_node_t*>(
            resource_->allocate(sizeof(buffer_eviction_node_t), alignof(buffer_eviction_node_t)));
        new (ptr) buffer_eviction_node_t(std::move(node));
        q.push(ptr);
    }

    void eviction_queue_t::destroy_node(buffer_eviction_node_t* node) {
        node->~buffer_eviction_node_t();
        resource_->deallocate(node, sizeof(buffer_eviction_node_t), alignof(buffer_eviction_node_t));
    }

    bool eviction_queue_t::add_to_eviction_queue(buffer_eviction_node_t&& node, bool replaces_queued) {
        if (replaces_queued) {
            dead_nodes_.fetch_add(1, std::memory_order_relaxed);
        } else {
            live_nodes_.fetch_add(1, std::memory_order_relaxed);
        }
        enqueue(std::move(node));
        // 1M pin/unpin cycles, Release, arm64 macOS, mean of 9: R=4 handles 49.7 ns/cycle (41.9 with the
        // former purge of every 4096th insert over a ~32768-node queue), R=4096 42.0 ns (42.0).
        return dead_nodes_.load(std::memory_order_relaxed) > live_nodes_.load(std::memory_order_relaxed);
    }

    bool eviction_queue_t::try_dequeue(buffer_eviction_node_t& node) {
        buffer_eviction_node_t* ptr = nullptr;
        if (!q.pop(ptr)) {
            return false;
        }
        node = std::move(*ptr);
        destroy_node(ptr);
        return true;
    }

    bool eviction_queue_t::try_dequeue_with_lock(buffer_eviction_node_t& node) {
        std::lock_guard lock(purge_lock_);
        return try_dequeue(node);
    }

    void eviction_queue_t::purge() {
        if (!purge_lock_.try_lock()) {
            return;
        }
        std::lock_guard lock{purge_lock_, std::adopt_lock};

        for (int batch = 0; batch < 2 && dead_nodes_.load(std::memory_order_relaxed) >
                                             live_nodes_.load(std::memory_order_relaxed);
             ++batch) {
            purge_batch();
        }
    }

    void eviction_queue_t::purge_batch() {
        const int64_t queued =
            live_nodes_.load(std::memory_order_relaxed) + dead_nodes_.load(std::memory_order_relaxed);
        const int64_t batch = std::min(queued, PURGE_BATCH);
        for (int64_t i = 0; i < batch; i++) {
            buffer_eviction_node_t* ptr = nullptr;
            if (!q.pop(ptr)) {
                break;
            }
            if (ptr->is_current()) {
                purge_nodes_.push_back(ptr);
            } else {
                destroy_node(ptr);
                dead_nodes_.fetch_sub(1, std::memory_order_relaxed);
            }
        }
        for (auto* ptr : purge_nodes_) {
            q.push(ptr);
        }
        purge_nodes_.clear();
    }

    template<typename FN>
    void eviction_queue_t::iterate_unloadable_blocks(FN fn) {
        for (;;) {
            buffer_eviction_node_t node;
            if (!try_dequeue(node)) {
                // retry under purge_lock_: a concurrent purge may briefly
                // hold alive nodes outside the queue (in purge_nodes_)
                if (!try_dequeue_with_lock(node)) {
                    return;
                }
            }

            auto handle = node.handle.lock();
            if (!handle) {
                dead_nodes_.fetch_sub(1, std::memory_order_relaxed);
                continue;
            }

            auto lock = handle->get_lock();
            if (!node.is_current(*handle)) {
                dead_nodes_.fetch_sub(1, std::memory_order_relaxed);
                continue;
            }
            [[maybe_unused]] const bool was_queued = handle->exchange_queued(lock, false);
            assert(was_queued);
            live_nodes_.fetch_sub(1, std::memory_order_relaxed);
            // A candidate is either already backed by disk (can_unload) or transient and idle, in
            // which case the pool writes it to its scratch file first (can_spill). Before the spill
            // path existed only the first kind could ever be queued, which is why the queues were
            // empty exactly when a bulk load needed them.
            if (!handle->can_unload() && !handle->can_spill()) {
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
        , spill_file_(resource)
        , allocator_bulk_deallocation_flush_threshold(allocator_bulk_deallocation_flush_threshold)
        , track_eviction_timestamps(track_eviction_timestamps) {
        for (uint8_t type_idx = 0; type_idx < FILE_BUFFER_TYPE_COUNT; type_idx++) {
            const auto type = static_cast<file_buffer_type>(type_idx + 1);
            const auto& type_queue_size = eviction_queue_sizes[type_idx];
            for (uint64_t queue_idx = 0; queue_idx < type_queue_size; queue_idx++) {
                queues.push_back(std::make_unique<eviction_queue_t>(type, resource));
            }
        }
    }

    core::result_wrapper_t<bool> buffer_pool_t::set_limit(uint64_t limit) {
        std::lock_guard l_lock(limit_lock);
        if (!evict_blocks(memory_tag::EXTENSION, 0, limit).success) {
            return core::error_t(core::error_code_t::out_of_memory,
                                 std::pmr::string{"Failed to change memory limit to " + std::to_string(limit) +
                                                      ": could not free up enough memory for the new limit",
                                                  resource});
        }
        uint64_t old_limit = maximum_memory;
        maximum_memory = limit;
        if (!evict_blocks(memory_tag::EXTENSION, 0, limit).success) {
            maximum_memory = old_limit;
            return core::error_t(core::error_code_t::out_of_memory,
                                 std::pmr::string{"Failed to change memory limit to " + std::to_string(limit) +
                                                      ": could not free up enough memory for the new limit",
                                                  resource});
        }
        return true;
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
        // Unreachable: the loop above always returns on the last queue (`queue.get() == queues.back().get()`),
        // and `queues` is non-empty (the ctor pushes one queue per file_buffer_type). Programming invariant,
        // not a runtime error -- assert instead of throwing.
        assert(false && "Exited buffer_pool_t::evict_blocks_internal without obtaining buffer_pool_t::eviction_result");
        return evict_blocks_internal(*queues.back(), tag, extra_memory, memory_limit, buffer);
    }

    buffer_pool_t::eviction_result buffer_pool_t::evict_blocks_internal(eviction_queue_t& queue,
                                                                        memory_tag tag,
                                                                        uint64_t extra_memory,
                                                                        uint64_t memory_limit,
                                                                        std::unique_ptr<file_buffer_t>* buffer) {
        temp_buffer_pool_reservation_t r(tag, *this, extra_memory);
        bool found = false;

        // FLUSH, not NO_FLUSH. update_used_memory accumulates small deltas in per-thread caches and
        // only folds them into the base counter past a threshold, so the NO_FLUSH reading does not
        // move as this loop frees memory. Reading it after every unload therefore never saw the
        // limit satisfied: the pass emptied the whole queue and still reported failure. Take one
        // accurate reading here and count what this pass frees locally.
        const uint64_t used_now = memory_usage.used_memory(memory_usage_caches::FLUSH);
        if (used_now <= memory_limit) {
            return {true, std::move(r)};
        }
        const uint64_t to_free = used_now - memory_limit;
        uint64_t freed = 0;

        queue.iterate_unloadable_blocks([&](buffer_eviction_node_t&,
                                            const std::shared_ptr<block_handle_t>& handle,
                                            std::unique_lock<std::mutex>& lock) {
            // Transient buffer: its bytes exist nowhere else, so they go to the scratch file
            // BEFORE the memory is taken away. A failed write leaves the block untouched and
            // resident — losing rows to make room is never the right trade — and eviction simply
            // moves on to the next candidate.
            if (!handle->is_reloadable() && !handle->has_temp_copy()) {
                auto& file_buffer = handle->get_buffer(lock);
                auto slot = spill_file_.write(file_buffer->internal_buffer(), file_buffer->allocation_size());
                if (slot.has_error()) {
                    return true;
                }
                handle->set_temp_copy(slot.value(), file_buffer->allocation_size(), file_buffer->size());
            }

            const uint64_t reclaimed = handle->get_buffer(lock)->allocation_size();

            if (buffer && reclaimed == extra_memory) {
                *buffer = handle->unload_and_take_block(lock);
                found = true;
                return false;
            }

            handle->unload(lock);
            freed += reclaimed;

            if (freed >= to_free) {
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

    eviction_queue_t* buffer_pool_t::add_to_eviction_queue(std::shared_ptr<block_handle_t>& handle,
                                                           std::unique_lock<std::mutex>& lock) {
        auto& queue = eviction_queue_for_handle(*handle);
        assert(handle->readers() == 0);
        const bool replaces_queued = handle->exchange_queued(lock, true);
        auto ts = handle->next_eviction_sequence_number();
        if (track_eviction_timestamps) {
            handle->set_LRU_timestamp(
                std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now())
                    .time_since_epoch()
                    .count());
        }

        if (!queue.add_to_eviction_queue(buffer_eviction_node_t(std::weak_ptr(handle), ts), replaces_queued)) {
            return nullptr;
        }
        return &queue;
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

        const auto& queue_size =
            eviction_queue_sizes[static_cast<size_t>(static_cast<uint8_t>(handle_buffer_type) - 1)];
        auto eviction_queue_idx = handle.eviction_queue_index();
        if (eviction_queue_idx < queue_size) {
            queue_index += queue_size - eviction_queue_idx - 1;
        }

        assert(queues[queue_index]->buffer_type == handle_buffer_type);
        return *queues[queue_index];
    }

    void buffer_pool_t::retire_queued_node(const block_handle_t& handle) {
        eviction_queue_for_handle(handle).retire_queued_node();
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
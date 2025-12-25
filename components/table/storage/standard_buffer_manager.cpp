#include "standard_buffer_manager.hpp"

#include "buffer_handle.hpp"
#include "buffer_pool.hpp"
#include "in_memory_block_manager.hpp"

namespace components::table::storage {

    standard_buffer_manager_t::standard_buffer_manager_t(std::pmr::memory_resource* resource,
                                                         core::filesystem::local_file_system_t& fs,
                                                         buffer_pool_t& buffer_pool)
        : resource_(resource)
        , fs_(fs)
        , buffer_pool_(buffer_pool)
        , temp_id_(MAXIMUM_BLOCK) {
        temp_block_manager_ = std::make_unique<in_memory_block_manager_t>(*this, DEFAULT_BLOCK_ALLOC_SIZE);
        for (uint64_t i = 0; i < static_cast<uint64_t>(memory_tag::MEMORY_TAG_COUNT); i++) {
            evicted_data_per_tag_[i] = 0;
        }
    }

    std::unique_ptr<file_buffer_t>
    standard_buffer_manager_t::construct_manager_buffer(uint64_t size,
                                                        std::unique_ptr<file_buffer_t>&& source,
                                                        file_buffer_type type) {
        std::unique_ptr<file_buffer_t> result;
        if (type == file_buffer_type::BLOCK) {
            throw std::logic_error("construct_manager_buffer cannot be used to construct blocks");
        }
        if (source) {
            auto tmp = std::move(source);
            assert(tmp->allocation_size() == buffer_manager_t::allocation_size(size));
            result = std::make_unique<file_buffer_t>(*tmp, type);
        } else {
            result = std::make_unique<file_buffer_t>(resource(), type, size);
        }
        return result;
    }

    buffer_pool_t& standard_buffer_manager_t::buffer_pool() const { return buffer_pool_; }

    uint64_t standard_buffer_manager_t::block_allocation_size() const {
        return temp_block_manager_->block_allocation_size();
    }

    uint64_t standard_buffer_manager_t::block_size() const { return temp_block_manager_->block_size(); }

    temp_buffer_pool_reservation_t
    standard_buffer_manager_t::evict_blocks_or_throw(memory_tag tag,
                                                     uint64_t memory_delta,
                                                     std::unique_ptr<file_buffer_t>* buffer) {
        auto r = buffer_pool_.evict_blocks(tag, memory_delta, buffer_pool_.maximum_memory, buffer);
        if (!r.success) {
            throw std::runtime_error("standard_buffer_manager_t out of memory");
        }
        return std::move(r.reservation);
    }

    std::shared_ptr<block_handle_t> standard_buffer_manager_t::register_transient_memory(uint64_t size,
                                                                                         uint64_t block_size) {
        assert(size <= block_size);

        if (size < block_size) {
            return register_small_memory(memory_tag::IN_MEMORY_TABLE, size);
        }

        auto buffer_handle = allocate(memory_tag::IN_MEMORY_TABLE, size, false);
        return buffer_handle.block_handle();
    }

    std::shared_ptr<block_handle_t> standard_buffer_manager_t::register_small_memory(uint64_t size) {
        return buffer_manager_t::register_small_memory(size);
    }

    std::shared_ptr<block_handle_t> standard_buffer_manager_t::register_small_memory(memory_tag tag, uint64_t size) {
        assert(size < block_size());
        auto reservation = evict_blocks_or_throw(tag, size, nullptr);

        auto buffer = construct_manager_buffer(size, nullptr, file_buffer_type::TINY_BUFFER);

        auto result = std::make_shared<block_handle_t>(*temp_block_manager_,
                                                       ++temp_id_,
                                                       tag,
                                                       std::move(buffer),
                                                       destroy_buffer_condition::BLOCK,
                                                       size,
                                                       std::move(reservation));
        return result;
    }

    std::shared_ptr<block_handle_t>
    standard_buffer_manager_t::register_memory(memory_tag tag, uint64_t block_size, bool can_destroy) {
        auto alloc_size = allocation_size(block_size);

        std::unique_ptr<file_buffer_t> reusable_buffer;
        auto res = evict_blocks_or_throw(tag, alloc_size, &reusable_buffer);

        auto buffer = construct_manager_buffer(block_size, std::move(reusable_buffer));
        destroy_buffer_condition destroy_buffer_condition =
            can_destroy ? destroy_buffer_condition::EVICTION : destroy_buffer_condition::BLOCK;
        return std::make_shared<block_handle_t>(*temp_block_manager_,
                                                ++temp_id_,
                                                tag,
                                                std::move(buffer),
                                                destroy_buffer_condition,
                                                alloc_size,
                                                std::move(res));
    }

    buffer_handle_t standard_buffer_manager_t::allocate(memory_tag tag, uint64_t block_size, bool can_destroy) {
        auto block = register_memory(tag, block_size, can_destroy);
        return pin(block);
    }

    void standard_buffer_manager_t::reallocate(std::shared_ptr<block_handle_t>& handle, uint64_t block_size) {
        assert(block_size >= this->block_size());
        auto lock = handle->get_lock();

        auto handle_memory_usage = handle->memory_usage();
        assert(handle->state() == block_state::LOADED);
        assert(handle_memory_usage == handle->get_buffer(lock)->allocation_size());
        assert(handle_memory_usage == handle->memory_usage(lock).size);

        auto req = handle->get_buffer(lock)->calculate_memory(block_size);
        int64_t memory_delta = static_cast<int64_t>(req.alloc_size) - static_cast<int64_t>(handle_memory_usage);

        if (memory_delta == 0) {
            return;
        } else if (memory_delta > 0) {
            lock.unlock();
            auto reservation =
                evict_blocks_or_throw(handle->get_memory_tag(), static_cast<uint64_t>(memory_delta), nullptr);
            lock.lock();

            handle->merge_memory_reservation(lock, std::move(reservation));
        } else {
            handle->resize_memory(lock, req.alloc_size);
        }

        handle->resize_buffer(lock, block_size, memory_delta);
    }

    void standard_buffer_manager_t::batch_read(std::vector<std::shared_ptr<block_handle_t>>& handles,
                                               const std::map<uint64_t, uint64_t>& load_map,
                                               uint64_t first_block,
                                               uint64_t last_block) {
        auto& block_manager = handles[0]->block_manager;
        uint64_t block_count = last_block - first_block + 1;

        auto intermediate_buffer = allocate(memory_tag::BASE_TABLE, block_count * block_manager.block_size());
        block_manager.read_blocks(intermediate_buffer.file_buffer(), first_block, block_count);

        for (uint64_t block_idx = 0; block_idx < block_count; block_idx++) {
            uint64_t block_id = first_block + block_idx;
            auto entry = load_map.find(block_id);
            assert(entry != load_map.end());
            auto& handle = handles[entry->second];

            uint64_t required_memory = handle->memory_usage();
            std::unique_ptr<file_buffer_t> reusable_buffer;
            auto reservation = evict_blocks_or_throw(handle->get_memory_tag(), required_memory, &reusable_buffer);
            buffer_handle_t buf;
            {
                auto lock = handle->get_lock();
                if (handle->state() == block_state::LOADED) {
                    reservation.resize(0);
                    continue;
                }
                auto block_ptr = intermediate_buffer.file_buffer().internal_buffer() +
                                 block_idx * block_manager.block_allocation_size();
                buf = handle->load_from_buffer(lock, block_ptr, std::move(reusable_buffer), std::move(reservation));
            }
        }
    }

    void standard_buffer_manager_t::prefetch(std::vector<std::shared_ptr<block_handle_t>>& handles) {
        std::map<uint64_t, uint64_t> to_be_loaded;
        for (uint64_t block_idx = 0; block_idx < handles.size(); block_idx++) {
            auto& handle = handles[block_idx];
            if (handle->state() != block_state::LOADED) {
                to_be_loaded.insert(std::make_pair(handle->block_id(), block_idx));
            }
        }
        if (to_be_loaded.empty()) {
            return;
        }
        uint64_t first_block = std::numeric_limits<uint64_t>::max();
        uint64_t previous_block_id = std::numeric_limits<uint64_t>::max();
        for (auto& entry : to_be_loaded) {
            if (previous_block_id == std::numeric_limits<uint64_t>::max()) {
                first_block = entry.first;
                previous_block_id = first_block;
            } else if (previous_block_id + 1 == entry.first) {
                previous_block_id = entry.first;
            } else {
                batch_read(handles, to_be_loaded, first_block, previous_block_id);
                first_block = entry.first;
                previous_block_id = entry.first;
            }
        }
        batch_read(handles, to_be_loaded, first_block, previous_block_id);
    }

    buffer_handle_t standard_buffer_manager_t::pin(std::shared_ptr<block_handle_t>& handle) {
        buffer_handle_t buf;

        uint64_t required_memory;
        {
            auto lock = handle->get_lock();
            if (handle->state() == block_state::LOADED) {
                buf = handle->load();
            }
            required_memory = handle->memory_usage();
        }

        if (buf.is_valid()) {
            return buf;
        } else {
            std::unique_ptr<file_buffer_t> reusable_buffer;
            auto reservation = evict_blocks_or_throw(handle->get_memory_tag(), required_memory, &reusable_buffer);

            auto lock = handle->get_lock();
            if (handle->state() == block_state::LOADED) {
                reservation.resize(0);
                buf = handle->load();
            } else {
                assert(handle->readers() == 0);
                buf = handle->load(std::move(reusable_buffer));
                auto& memory_charge = handle->memory_usage(lock);
                memory_charge = std::move(reservation);
                int64_t delta = static_cast<int64_t>(handle->get_buffer(lock)->allocation_size()) -
                                static_cast<int64_t>(handle->memory_usage());
                if (delta) {
                    handle->change_memory_usage(lock, delta);
                }
                assert(handle->memory_usage() == handle->get_buffer(lock)->allocation_size());
            }
        }

        assert(buf.is_valid());
        return buf;
    }

    void standard_buffer_manager_t::purge_queue(const block_handle_t& handle) { buffer_pool_.purge_queue(handle); }

    void standard_buffer_manager_t::add_to_eviction_queue(std::shared_ptr<block_handle_t>& handle) {
        buffer_pool_.add_to_eviction_queue(handle);
    }

    void standard_buffer_manager_t::unpin(std::shared_ptr<block_handle_t>& handle) {
        bool purge = false;
        {
            auto lock = handle->get_lock();
            if (!handle->get_buffer(lock) || handle->buffer_type() == file_buffer_type::TINY_BUFFER) {
                return;
            }
            assert(handle->readers() > 0);
            auto new_readers = handle->decrement_readers();
            if (new_readers == 0) {
                if (handle->must_add_to_eviction_queue()) {
                    purge = buffer_pool_.add_to_eviction_queue(handle);
                } else {
                    handle->unload(lock);
                }
            }
        }

        if (purge) {
            purge_queue(*handle);
        }
    }

    void standard_buffer_manager_t::set_memory_limit(uint64_t limit) { buffer_pool_.set_limit(limit); }

    std::vector<memory_info_t> standard_buffer_manager_t::get_memory_usage_info() const {
        std::vector<memory_info_t> result;
        for (uint64_t k = 0; k < static_cast<uint64_t>(memory_tag::MEMORY_TAG_COUNT); k++) {
            memory_info_t info;
            info.tag = memory_tag(k);
            info.size = buffer_pool_.memory_usage.used_memory(memory_tag(k), buffer_pool_t::memory_usage_caches::FLUSH);
            info.evicted_data = evicted_data_per_tag_[k].load();
            result.push_back(info);
        }
        return result;
    }

    void standard_buffer_manager_t::reserve_memory(uint64_t size) {
        if (size == 0) {
            return;
        }
        auto reservation = evict_blocks_or_throw(memory_tag::EXTENSION, size, nullptr);
        reservation.size = 0;
    }

    void standard_buffer_manager_t::free_reserved_memory(uint64_t size) {
        if (size == 0) {
            return;
        }
        buffer_pool_.memory_usage.update_used_memory(memory_tag::EXTENSION, -static_cast<int64_t>(size));
    }

} // namespace components::table::storage
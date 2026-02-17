#include "block_handle.hpp"
#include "block_manager.hpp"
#include "buffer_handle.hpp"
#include "buffer_manager.hpp"
#include "buffer_pool.hpp"

#include <cstring>

namespace components::table::storage {

    buffer_pool_reservation_t::buffer_pool_reservation_t(memory_tag tag, buffer_pool_t& pool)
        : tag(tag)
        , pool(pool) {}

    buffer_pool_reservation_t::buffer_pool_reservation_t(buffer_pool_reservation_t&& src) noexcept
        : tag(src.tag)
        , pool(src.pool) {
        size = src.size;
        src.size = 0;
    }

    buffer_pool_reservation_t& buffer_pool_reservation_t::operator=(buffer_pool_reservation_t&& src) noexcept {
        tag = src.tag;
        size = src.size;
        src.size = 0;
        return *this;
    }

    buffer_pool_reservation_t::~buffer_pool_reservation_t() { assert(size == 0); }

    void buffer_pool_reservation_t::resize(uint64_t new_size) {
        auto delta = static_cast<int64_t>(new_size) - static_cast<int64_t>(size);
        pool.update_used_memory(tag, delta);
        size = new_size;
    }

    void buffer_pool_reservation_t::merge(buffer_pool_reservation_t src) {
        size += src.size;
        src.size = 0;
    }

    block_handle_t::block_handle_t(block_manager_t& block_manager, uint64_t block_id, memory_tag tag)
        : block_manager(block_manager)
        , readers_(0)
        , block_id_(block_id)
        , tag_(tag)
        , buffer_type_(file_buffer_type::BLOCK)
        , buffer_(nullptr)
        , eviction_seq_num_(0)
        , destroy_condition_(destroy_buffer_condition::BLOCK)
        , memory_charge_(tag, block_manager.buffer_manager.buffer_pool())
        , unswizzled_(nullptr)
        , eviction_queue_idx_(INVALID_INDEX) {
        eviction_seq_num_ = 0;
        state_ = block_state::UNLOADED;
        memory_usage_ = block_manager.block_allocation_size();
    }

    block_handle_t::block_handle_t(block_manager_t& block_manager,
                                   uint64_t block_id,
                                   memory_tag tag,
                                   std::unique_ptr<file_buffer_t> buffer,
                                   destroy_buffer_condition destroy_buffer_upon,
                                   uint64_t block_size,
                                   buffer_pool_reservation_t&& reservation)
        : block_manager(block_manager)
        , readers_(0)
        , block_id_(block_id)
        , tag_(tag)
        , buffer_type_(buffer->buffer_type())
        , eviction_seq_num_(0)
        , destroy_condition_(destroy_buffer_upon)
        , memory_charge_(tag, block_manager.buffer_manager.buffer_pool())
        , unswizzled_(nullptr)
        , eviction_queue_idx_(INVALID_INDEX) {
        buffer_ = std::move(buffer);
        state_ = block_state::LOADED;
        memory_usage_ = block_size;
        memory_charge_ = std::move(reservation);
    }

    block_handle_t::~block_handle_t() {
        unswizzled_ = nullptr;
        assert(!buffer_ || buffer_->buffer_type() == buffer_type_);
        if (buffer_ && buffer_type_ != file_buffer_type::TINY_BUFFER) {
            auto& buffer_manager = block_manager.buffer_manager;
            buffer_manager.buffer_pool().increment_dead_nodes(*this);
        }

        if (buffer_ && state_ == block_state::LOADED) {
            assert(memory_charge_.size > 0);
            buffer_.reset();
            memory_charge_.resize(0);
        } else {
            assert(memory_charge_.size == 0);
        }

        block_manager.unregister_block(*this);
    }

    std::unique_ptr<block_t>
    allocate_block(block_manager_t& block_manager, std::unique_ptr<file_buffer_t> reusable_buffer, uint64_t block_id) {
        if (reusable_buffer) {
            if (reusable_buffer->buffer_type() == file_buffer_type::BLOCK) {
                auto& block = reinterpret_cast<block_t&>(*reusable_buffer);
                block.id = block_id;
                return std::unique_ptr<block_t>(static_cast<block_t*>(reusable_buffer.release()));
            }
            auto block = block_manager.create_block(block_id, reusable_buffer.get());
            reusable_buffer.reset();
            return block;
        } else {
            return block_manager.create_block(block_id, nullptr);
        }
    }

    void block_handle_t::change_memory_usage(std::unique_lock<std::mutex>&, int64_t delta) {
        assert(delta < 0);
        memory_usage_ += static_cast<uint64_t>(delta);
        memory_charge_.resize(memory_usage_);
    }

    std::unique_ptr<file_buffer_t>& block_handle_t::get_buffer(std::unique_lock<std::mutex>&) { return buffer_; }

    buffer_pool_reservation_t& block_handle_t::memory_usage(std::unique_lock<std::mutex>&) { return memory_charge_; }

    void block_handle_t::merge_memory_reservation(std::unique_lock<std::mutex>&,
                                                  buffer_pool_reservation_t reservation) {
        memory_charge_.merge(std::move(reservation));
    }

    void block_handle_t::resize_memory(std::unique_lock<std::mutex>&, uint64_t alloc_size) {
        memory_charge_.resize(alloc_size);
    }

    void block_handle_t::resize_buffer(std::unique_lock<std::mutex>&, uint64_t block_size, int64_t memory_delta) {
        assert(buffer_);
        buffer_->resize(block_size);
        memory_usage_ = static_cast<uint64_t>(static_cast<int64_t>(memory_usage_.load()) + memory_delta);
        assert(memory_usage_ == buffer_->allocation_size());
    }

    buffer_handle_t block_handle_t::load_from_buffer(std::unique_lock<std::mutex>&,
                                                     std::byte* data,
                                                     std::unique_ptr<file_buffer_t> reusable_buffer,
                                                     buffer_pool_reservation_t reservation) {
        assert(state_ != block_state::LOADED);
        assert(readers_ == 0);
        auto block = allocate_block(block_manager, std::move(reusable_buffer), block_id_);
        std::memcpy(block->internal_buffer(), data, block->allocation_size());
        buffer_ = std::move(block);
        state_ = block_state::LOADED;
        readers_ = 1;
        memory_charge_ = std::move(reservation);
        return buffer_handle_t(this, buffer_.get());
    }

    buffer_handle_t block_handle_t::load(std::unique_ptr<file_buffer_t> reusable_buffer) {
        if (state_ == block_state::LOADED) {
            assert(buffer_);
            ++readers_;
            return buffer_handle_t(this, buffer_.get());
        }

        if (block_id_ < MAXIMUM_BLOCK) {
            auto block = allocate_block(block_manager, std::move(reusable_buffer), block_id_);
            block_manager.read(*block);
            buffer_ = std::move(block);
        } else {
            return {};
        }
        state_ = block_state::LOADED;
        readers_ = 1;
        return buffer_handle_t(this, buffer_.get());
    }

    std::unique_ptr<file_buffer_t> block_handle_t::unload_and_take_block(std::unique_lock<std::mutex>&) {
        if (state_ == block_state::UNLOADED) {
            return nullptr;
        }
        assert(!unswizzled_);
        assert(can_unload());

        memory_charge_.resize(0);
        state_ = block_state::UNLOADED;
        return std::move(buffer_);
    }

    void block_handle_t::unload(std::unique_lock<std::mutex>& lock) {
        auto block = unload_and_take_block(lock);
        block.reset();
    }

    bool block_handle_t::can_unload() const {
        if (state_ == block_state::UNLOADED) {
            return false;
        }
        if (readers_ > 0) {
            return false;
        }
        return true;
    }

} // namespace components::table::storage
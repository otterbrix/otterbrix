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
        // Serialize the buffer_/state_/memory_charge_ teardown against a concurrent unload() /
        // unload_and_take_block() running under lock_ on the eviction path of the OTHER disk-agent thread.
        // Without this lock, the dtor's buffer_.reset() / memory_charge_.resize(0) can race a concurrent
        // unload(), double-freeing the block buffer and corrupting the shared pmr pool freelist (BUG A).
        // Deadlock-safe: no code path holds lock_ while dropping the last shared_ptr that triggers this dtor
        // (the eviction / pin / reallocate paths all keep a strong reference while lock_ is held, so the
        // refcount is >= 1 there and the dtor cannot run concurrently with the same handle's lock holder).
        {
            std::unique_lock<std::mutex> lock(lock_);
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

    core::result_wrapper_t<buffer_handle_t> block_handle_t::load(std::unique_ptr<file_buffer_t> reusable_buffer) {
        if (state_ == block_state::LOADED) {
            assert(buffer_);
            ++readers_;
            return buffer_handle_t(this, buffer_.get());
        }

        if (has_temp_copy()) {
            // Spilled: rebuild a buffer of THIS handle's type (never a block_t — the destructor and
            // the eviction queues both key off buffer_type_) and read the scratch slot back into it.
            // temp_user_size_ is the buffer's own size() as recorded at spill time. Rebuilding from
            // it reproduces the identical allocation: size() is (allocation - header), and the
            // allocation is already sector-aligned, so aligning it up again is a no-op.
            //
            // reusable_buffer is deliberately not consumed here: pin() sizes it against
            // memory_usage_ (a user size) while construct_manager_buffer compares allocations, so
            // adopting it would compare two different units. Letting it go costs one allocation on
            // a path that has just done disk I/O.
            auto restored =
                block_manager.buffer_manager.construct_manager_buffer(temp_user_size_, nullptr, buffer_type_);
            if (!block_manager.buffer_manager.buffer_pool().read_temporary(temp_slot_,
                                                                           restored->internal_buffer(),
                                                                           temp_size_)) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"block_handle_t: spilled buffer could not be read back",
                                                      block_manager.buffer_manager.resource()});
            }
            // The slot is released once the bytes are back in memory: the buffer may now be written
            // to, so the copy on disk is stale from this moment on.
            block_manager.buffer_manager.buffer_pool().release_temporary(temp_slot_, temp_size_);
            clear_temp_copy();
            buffer_ = std::move(restored);
        } else if (block_id_ < MAXIMUM_BLOCK) {
            auto block = allocate_block(block_manager, std::move(reusable_buffer), block_id_);
            // Disk reload: surface a checksum/IO failure as a value (data_corruption/io_error) rather than
            // throwing. The block stays UNLOADED on error.
            auto read_result = block_manager.read(*block);
            if (read_result.has_error()) {
                return read_result.convert_error<buffer_handle_t>();
            }
            buffer_ = std::move(block);
        } else {
            return buffer_handle_t{};
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
        // Either the bytes are already on disk, or they were just written to the pool's scratch
        // file. Dropping a buffer that is in neither state loses rows.
        assert(can_unload() || has_temp_copy());

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
        if (!is_reloadable()) {
            // No disk copy (managed in-memory block): load() would return {} -> can never reload.
            // Never unload such a block; keep it resident. (Hard safety net for evict + purge paths.)
            return false;
        }
        return true;
    }

} // namespace components::table::storage
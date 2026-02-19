#include "metadata_manager.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

#include "buffer_manager.hpp"

namespace components::table::storage {

    metadata_manager_t::metadata_manager_t(block_manager_t& block_manager)
        : block_manager_(block_manager)
        , sub_block_size_(block_manager.block_allocation_size() / META_SUB_BLOCKS_PER_BLOCK) {}

    meta_block_pointer_t metadata_manager_t::allocate_handle() {
        std::lock_guard lock(lock_);

        // find block with free sub-blocks
        for (auto& mb : blocks_) {
            if (mb.next_free_sub_block < META_SUB_BLOCKS_PER_BLOCK) {
                uint32_t sub_idx = mb.next_free_sub_block;
                mb.next_free_sub_block++;
                mb.dirty = true;
                // block_pointer = block_id * 64 + sub_block_index
                uint64_t bp = mb.block_id * META_SUB_BLOCKS_PER_BLOCK + sub_idx;
                return meta_block_pointer_t(bp, 0);
            }
        }

        // allocate new block
        uint64_t new_block_id = block_manager_.free_block_id();
        auto resource = block_manager_.buffer_manager.resource();
        auto block = std::make_unique<block_t>(resource, new_block_id,
                                                static_cast<uint64_t>(block_manager_.block_size()));
        block->clear();

        metadata_block_t mb;
        mb.block_id = new_block_id;
        mb.block = std::move(block);
        mb.next_free_sub_block = 1; // sub-block 0 is being allocated
        mb.dirty = true;
        blocks_.push_back(std::move(mb));

        uint64_t bp = new_block_id * META_SUB_BLOCKS_PER_BLOCK + 0;
        return meta_block_pointer_t(bp, 0);
    }

    std::byte* metadata_manager_t::pin(meta_block_pointer_t pointer) {
        uint64_t block_id = pointer.block_id();
        uint32_t sub_idx = pointer.GetBlockIndex();

        std::lock_guard lock(lock_);
        for (auto& mb : blocks_) {
            if (mb.block_id == block_id) {
                auto* base = mb.block->buffer();
                return base + sub_idx * sub_block_size_;
            }
        }

        // block not loaded yet — load from disk
        auto resource = block_manager_.buffer_manager.resource();
        auto block = std::make_unique<block_t>(resource, block_id,
                                                static_cast<uint64_t>(block_manager_.block_size()));
        block_manager_.read(*block);

        metadata_block_t mb;
        mb.block_id = block_id;
        mb.block = std::move(block);
        mb.next_free_sub_block = META_SUB_BLOCKS_PER_BLOCK; // all sub-blocks assumed used
        mb.dirty = false;

        auto* base = mb.block->buffer();
        auto* result = base + sub_idx * sub_block_size_;
        blocks_.push_back(std::move(mb));
        return result;
    }

    void metadata_manager_t::flush() {
        std::lock_guard lock(lock_);
        for (auto& mb : blocks_) {
            if (mb.dirty) {
                block_manager_.write(*mb.block);
                mb.dirty = false;
            }
        }
    }

} // namespace components::table::storage

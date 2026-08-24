#include "metadata_manager.hpp"

#include <cassert>
#include <cstring>
#include <stdexcept>

#include "buffer_manager.hpp"

namespace components::table::storage {

    metadata_manager_t::metadata_manager_t(block_manager_t& block_manager)
        : block_manager_(block_manager)
        // Sub-blocks are carved out of what pin() can actually reach — block_size(), the
        // ALLOCATION minus the block header — not out of the allocation itself. Dividing
        // the allocation gave 262144/64 = 4096, so the 64 sub-blocks spanned 262144 bytes
        // inside a 262136-byte region and sub-block 63 ended 8 bytes past the buffer, on
        // top of the neighbouring pool allocation's free-list pointer: a non-deterministic
        // SIGSEGV inside an unrelated do_allocate on any large checkpoint.
        // The floor keeps every sub-block base 8-byte aligned for the uint64_t chain
        // header that metadata_writer_t / metadata_reader_t put at its start.
        , sub_block_size_((block_manager.block_size() / META_SUB_BLOCKS_PER_BLOCK) &
                          ~(static_cast<uint64_t>(sizeof(uint64_t)) - 1)) {}

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
        auto block =
            std::make_unique<block_t>(resource, new_block_id, static_cast<uint64_t>(block_manager_.block_size()));
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
        auto block = std::make_unique<block_t>(resource, block_id, static_cast<uint64_t>(block_manager_.block_size()));
        // read() returns a value (data_corruption/io_error). pin can run inside the table_storage_t DISK ctor
        // on the agent thread (bootstrap_disk_inner_sync, noexcept), so it MUST NOT throw: record the sticky
        // error and return nullptr; metadata_reader_t checks has_error()/null and unwinds.
        if (auto read_result = block_manager_.read(*block); read_result.has_error()) {
            error_ = read_result.error();
            return nullptr;
        }

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

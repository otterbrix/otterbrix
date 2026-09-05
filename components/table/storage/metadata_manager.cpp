#include "metadata_manager.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <set>
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

    void metadata_manager_t::reserve(uint64_t sub_blocks) {
        std::lock_guard lock(lock_);
        uint64_t available = 0;
        for (const auto& mb : blocks_) {
            if (mb.next_free_sub_block < META_SUB_BLOCKS_PER_BLOCK) {
                available += META_SUB_BLOCKS_PER_BLOCK - mb.next_free_sub_block;
            }
        }
        auto* resource = block_manager_.buffer_manager.resource();
        while (available < sub_blocks) {
            uint64_t new_block_id = block_manager_.free_block_id();
            auto block =
                std::make_unique<block_t>(resource, new_block_id, static_cast<uint64_t>(block_manager_.block_size()));
            block->clear();

            metadata_block_t mb;
            mb.block_id = new_block_id;
            mb.block = std::move(block);
            mb.next_free_sub_block = 0;
            // NOT dirty: a reserved block that no allocate_handle ever touches holds nothing and
            // must not cost a 256 KiB write of zeros. allocate_handle sets dirty when it hands a
            // sub-block out.
            mb.dirty = false;
            blocks_.push_back(std::move(mb));
            available += META_SUB_BLOCKS_PER_BLOCK;
        }
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

    core::result_wrapper_t<bool> metadata_manager_t::chain_blocks(meta_block_pointer_t start,
                                                                  std::pmr::vector<uint64_t>& out) {
        auto* resource = block_manager_.buffer_manager.resource();
        // Sub-block pointers already visited. The chain is disk-fed, so a cycle is possible
        // and must terminate the walk with an error instead of spinning forever.
        std::pmr::set<uint64_t> visited(resource);
        auto pointer = start;
        while (pointer.is_valid()) {
            if (!visited.insert(pointer.block_pointer).second) {
                return core::error_t(core::error_code_t::data_corruption,
                                     std::pmr::string{"metadata chain contains a cycle at sub-block " +
                                                          std::to_string(pointer.block_pointer),
                                                      resource});
            }
            const uint64_t block_id = pointer.block_id();
            if (std::find(out.begin(), out.end(), block_id) == out.end()) {
                out.push_back(block_id);
            }
            auto* data = pin(pointer);
            if (data == nullptr) {
                return has_error() ? core::result_wrapper_t<bool>(core::error_t(error_))
                                   : core::result_wrapper_t<bool>(core::error_t(
                                         core::error_code_t::data_corruption,
                                         std::pmr::string{"metadata chain sub-block " +
                                                              std::to_string(pointer.block_pointer) +
                                                              " could not be pinned",
                                                          resource}));
            }
            // 12-byte sub-block header written by metadata_writer_t: {uint64 next_block_pointer,
            // uint32 next_offset}. memcpy, not a reinterpret_cast load: the pointer is a
            // sub-block base inside a block buffer and the fields are disk bytes.
            uint64_t next_bp = 0;
            uint32_t next_off = 0;
            std::memcpy(&next_bp, data, sizeof(next_bp));
            std::memcpy(&next_off, data + sizeof(next_bp), sizeof(next_off));
            if (next_bp == INVALID_INDEX) {
                break;
            }
            pointer = meta_block_pointer_t(next_bp, next_off);
        }
        return true;
    }

    core::result_wrapper_t<bool> metadata_manager_t::flush() {
        std::lock_guard lock(lock_);
        for (auto& mb : blocks_) {
            if (mb.dirty) {
                // The metadata chain IS the root. A dropped write here (which is what this used
                // to do) leaves header.meta_block pointing at a sub-block chain that was never
                // laid down, and the next open follows it into whatever the file held before.
                // The block stays dirty on failure so a retry would try it again.
                auto written = block_manager_.write(*mb.block);
                if (written.has_error()) {
                    if (!error_.contains_error()) {
                        error_ = written.error();
                    }
                    return written;
                }
                mb.dirty = false;
            }
        }
        return true;
    }

} // namespace components::table::storage

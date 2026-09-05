#include "partial_block_manager.hpp"

#include <cstring>

#include "buffer_manager.hpp"

namespace components::table::storage {

    partial_block_manager_t::partial_block_manager_t(block_manager_t& block_manager, double full_threshold)
        : block_manager_(block_manager)
        , full_threshold_(full_threshold) {}

    partial_block_allocation_t partial_block_manager_t::get_block_allocation(uint64_t segment_size) {
        auto block_alloc_size = block_manager_.block_size();

        // if segment is large enough (> threshold of block), give it a dedicated block
        if (segment_size > static_cast<uint64_t>(static_cast<double>(block_alloc_size) * full_threshold_)) {
            uint64_t block_id = block_manager_.free_block_id();
            return {block_id, 0, segment_size};
        }

        // try to fit into an existing partial block
        for (auto& pb : partial_blocks_) {
            // Place every segment at an 8-byte-aligned offset. The offset is persisted in the
            // data pointer and dereferenced after reload with the segment's own element type:
            // uint64_t* for validity bitmaps (validity_scan/_partial, validity_fetch_row),
            // int32_t* for string dictionary offsets, and the raw T* that fixed_size_scan hands
            // to the result vector. Byte-granular packing (e.g. a validity bitmap right after a
            // 4-byte CONSTANT INT32 segment) made every one of those reads misaligned — UB that
            // -fsanitize=alignment flags on a plain scan of the reloaded table, and the file
            // keeps handing out the bad offset forever. The ≤7 padding bytes stay zeroed
            // (write_to_block memsets fresh buffers) and are never addressed.
            uint64_t aligned_offset = align_value<uint64_t>(pb.used_bytes);
            if (aligned_offset + segment_size <= pb.block_capacity) {
                pb.used_bytes = static_cast<uint32_t>(aligned_offset + segment_size);
                return {pb.block_id, static_cast<uint32_t>(aligned_offset), segment_size};
            }
        }

        // allocate new partial block
        uint64_t block_id = block_manager_.free_block_id();
        partial_block_t pb;
        pb.block_id = block_id;
        pb.used_bytes = static_cast<uint32_t>(segment_size);
        pb.block_capacity = block_alloc_size;
        partial_blocks_.push_back(pb);

        return {block_id, 0, segment_size};
    }

    void partial_block_manager_t::register_partial_block(uint64_t block_id, uint32_t used_size) {
        // used_size may be reconstructed from a persisted value and is NOT rounded here:
        // get_block_allocation aligns the offset at placement time, so the 8-byte-aligned-offset
        // invariant holds no matter what fill level this block is re-adopted with.
        auto block_alloc_size = block_manager_.block_size();
        partial_block_t pb;
        pb.block_id = block_id;
        pb.used_bytes = used_size;
        pb.block_capacity = block_alloc_size;
        partial_blocks_.push_back(pb);
    }

    void partial_block_manager_t::write_to_block(uint64_t block_id, uint32_t offset, const void* data, uint64_t size) {
        auto it = block_buffers_.find(block_id);
        if (it == block_buffers_.end()) {
            auto block = std::make_unique<block_t>(block_manager_.buffer_manager.resource(),
                                                   block_id,
                                                   static_cast<uint64_t>(block_manager_.block_size()));
            std::memset(block->buffer(), 0, static_cast<size_t>(block_manager_.block_size()));
            it = block_buffers_.emplace(block_id, std::move(block)).first;
        }
        std::memcpy(it->second->buffer() + offset, data, size);
    }

    core::result_wrapper_t<bool> partial_block_manager_t::flush_partial_blocks() {
        // Write all accumulated block buffers to disk. The FIRST failure ends the flush and is
        // returned: continuing would pile more unobserved writes on top of a file that already
        // has a hole in it, and the caller's checkpoint is over either way. The buffers are
        // still cleared, because the segments they belong to have already been re-pointed at
        // these block ids — holding them would only pretend the round can be resumed.
        core::result_wrapper_t<bool> result = true;
        for (auto& [block_id, block] : block_buffers_) {
            auto written = block_manager_.write(*block, block_id);
            if (written.has_error()) {
                result = written.error();
                break;
            }
        }
        block_buffers_.clear();
        partial_blocks_.clear();
        return result;
    }

} // namespace components::table::storage

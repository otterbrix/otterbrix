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
            // Place every segment at an 8-byte-aligned offset: the offset is dereferenced after
            // reload as uint64_t*/int32_t*/T*, and byte-granular packing made those reads
            // misaligned UB (caught by -fsanitize=alignment). Padding bytes stay zeroed
            // (write_to_block memsets fresh buffers).
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
        // First failure ends the flush and is returned; buffers are still cleared regardless,
        // since their segments are already re-pointed at these block ids (the round is over
        // either way).
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

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
            uint64_t remaining = pb.block_capacity - pb.used_bytes;
            if (remaining >= segment_size) {
                uint32_t offset = pb.used_bytes;
                pb.used_bytes += static_cast<uint32_t>(segment_size);
                return {pb.block_id, offset, segment_size};
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
            auto block = std::make_unique<block_t>(
                block_manager_.buffer_manager.resource(),
                block_id,
                static_cast<uint64_t>(block_manager_.block_size()));
            std::memset(block->buffer(), 0, static_cast<size_t>(block_manager_.block_size()));
            it = block_buffers_.emplace(block_id, std::move(block)).first;
        }
        std::memcpy(it->second->buffer() + offset, data, size);
    }

    void partial_block_manager_t::flush_partial_blocks() {
        // write all accumulated block buffers to disk
        for (auto& [block_id, block] : block_buffers_) {
            block_manager_.write(*block, block_id);
        }
        block_buffers_.clear();
        partial_blocks_.clear();
    }

} // namespace components::table::storage

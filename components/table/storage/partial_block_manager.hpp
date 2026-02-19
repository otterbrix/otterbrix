#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "block_manager.hpp"

namespace components::table::storage {

    struct partial_block_allocation_t {
        uint64_t block_id;
        uint32_t offset_in_block;
        uint64_t size;
    };

    class partial_block_manager_t {
    public:
        explicit partial_block_manager_t(block_manager_t& block_manager, double full_threshold = 0.8);

        partial_block_allocation_t get_block_allocation(uint64_t segment_size);

        void register_partial_block(uint64_t block_id, uint32_t used_size);

        // Write segment data into a managed block buffer (does NOT write to disk yet)
        void write_to_block(uint64_t block_id, uint32_t offset, const void* data, uint64_t size);

        // Flush all managed block buffers to disk, then clear
        void flush_partial_blocks();

    private:
        struct partial_block_t {
            uint64_t block_id;
            uint32_t used_bytes;
            uint64_t block_capacity;
        };

        block_manager_t& block_manager_;
        double full_threshold_;
        std::vector<partial_block_t> partial_blocks_;
        std::unordered_map<uint64_t, std::unique_ptr<block_t>> block_buffers_;
    };

} // namespace components::table::storage

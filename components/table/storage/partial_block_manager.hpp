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
        // A segment larger than FULL_THRESHOLD of the block payload gets a DEDICATED whole block
        // (offset 0, never shared). At/below it, the segment is PACKED into a shared partial block
        // at a (possibly non-zero) offset alongside other columns' segments. This is the single
        // source of truth for the write-side "dedicated vs shared" decision (used by both the
        // checkpoint flush path and the B2 write-through transition).
        //
        // Invariant: every offset handed out is 8-byte aligned. Offsets are persisted in the data
        // pointers and dereferenced after restart with typed pointers up to uint64_t wide (validity
        // bitmaps, string dictionary offsets, fixed-size scans), so byte-granular placement would
        // be permanent UB baked into the file. Enforced in get_block_allocation.
        static constexpr double FULL_THRESHOLD = 0.8;

        explicit partial_block_manager_t(block_manager_t& block_manager, double full_threshold = FULL_THRESHOLD);

        partial_block_allocation_t get_block_allocation(uint64_t segment_size);

        void register_partial_block(uint64_t block_id, uint32_t used_size);

        // Write segment data into a managed block buffer (does NOT write to disk yet)
        void write_to_block(uint64_t block_id, uint32_t offset, const void* data, uint64_t size);

        // Flush all managed block buffers to disk, then clear. Returns io_error when any of
        // those writes failed. This is the DATA half of the checkpoint's block writes — every
        // column segment in the system reaches the file through here — and it used to be a
        // `void` over a `void` over a discarded bool, which is why a failed data-block write
        // was invisible all the way up to a committed header.
        [[nodiscard]] core::result_wrapper_t<bool> flush_partial_blocks();

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

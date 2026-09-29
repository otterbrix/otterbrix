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
        // (offset 0, never shared); at or below it the segment is PACKED into a shared partial block
        // alongside other columns' segments. Single source of truth for the write-side "dedicated vs
        // shared" decision, used by the checkpoint flush path and the write-through transition alike.
        //
        // Invariant: every offset handed out is 8-byte aligned (enforced in get_block_allocation) —
        // offsets are dereferenced after restart with typed pointers up to uint64_t wide.
        static constexpr double FULL_THRESHOLD = 0.8;
        // Tails kept open after a flush (reuse_tails only): one for the row-group-sized fixed
        // segments, one for the string segments, so neither evicts the other every row group.
        // Unbounded, an 8-text-column table reached 120 open tails (30 MiB of buffers); 2 costs
        // +2% file on that table and nothing on single-text-column ones.
        static constexpr uint64_t MAX_OPEN_TAILS = 2;
        // An open tail below this is dropped at flush: no segment of the write-through fits it.
        static constexpr uint64_t MIN_REUSABLE_TAIL = 4096;

        // reuse_tails: a partial block survives flush_partial_blocks() and keeps taking segments
        // across later rounds (each flush writes only what was appended). Legal only while no
        // durable root names the block -- the owner must seal() before a checkpoint can reference
        // a block written through here.
        explicit partial_block_manager_t(block_manager_t& block_manager,
                                         double full_threshold = FULL_THRESHOLD,
                                         bool reuse_tails = false);

        partial_block_allocation_t get_block_allocation(uint64_t segment_size);

        void register_partial_block(uint64_t block_id, uint32_t used_size);

        // Write segment data into a managed block buffer (does NOT write to disk yet)
        void write_to_block(uint64_t block_id, uint32_t offset, const void* data, uint64_t size);

        // Flush all managed block buffers to disk, then clear (reuse_tails: keep the open tails).
        // Returns io_error on failure: every column segment reaches the file through here, so a
        // `void` would leave a failed data-block write invisible up to a committed header.
        [[nodiscard]] core::result_wrapper_t<bool> flush_partial_blocks();

        // Flushes, then forgets every open tail: the next allocation starts a fresh block.
        [[nodiscard]] core::result_wrapper_t<bool> seal();

#ifdef DEV_MODE
        // Fault seam: the next seal() in the process answers io_error without flushing. One-shot and
        // process-wide, like single_file_block_manager_t::dev_set_file_interposer; every append
        // flushes its own writes, so nothing but a seam can make a seal fail in a test.
        static void dev_refuse_next_seal();
#endif

    private:
        struct partial_block_t {
            uint64_t block_id;
            uint32_t used_bytes;
            uint64_t block_capacity;
            uint32_t flushed_bytes; // bytes of this image already on disk under block_id
        };

        [[nodiscard]] core::result_wrapper_t<bool> flush_tail(partial_block_t& pb, block_t& block);
        void keep_reusable_tails();

        block_manager_t& block_manager_;
        double full_threshold_;
        bool reuse_tails_;
        std::vector<partial_block_t> partial_blocks_;
        std::unordered_map<uint64_t, std::unique_ptr<block_t>> block_buffers_;
    };

} // namespace components::table::storage

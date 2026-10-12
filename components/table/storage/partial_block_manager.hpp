#pragma once

#include <cstdint>
#include <memory>
#include <memory_resource>
#include <vector>

#include "block_manager.hpp"

namespace components::table::storage {

    struct partial_block_allocation_t {
        uint64_t block_id;
        uint32_t offset_in_block;
        uint64_t size;
    };

    // What a caller wants done with the live segment whose bytes it placed, once the block holding
    // them is on the file: adopted at the flush after that block's write landed, destroyed with a
    // block that is never written (a refused write, the packer's death). DuckDB v1.5.6 keeps the same
    // list on PartialBlockForCheckpoint::segments and converts them in Flush().
    class placement_t {
    public:
        virtual ~placement_t() = default;
        // `at` is where the bytes went (the answer place() gave the caller). False: the live segment
        // is not the one placed any more (unwound, replaced, truncated), nothing names `at`.
        [[nodiscard]] bool adopt(const partial_block_allocation_t& at) { return adopt_impl(at); }
        // Whether the segment placed is still there to adopt: a tail whose every placement since
        // its last write died is rolled back instead of written.
        [[nodiscard]] bool alive() const { return alive_impl(); }

    private:
        virtual bool adopt_impl(const partial_block_allocation_t& at) = 0;
        virtual bool alive_impl() const = 0;
    };

    class partial_block_manager_t {
    public:
        // A segment larger than FULL_THRESHOLD of the block payload gets a DEDICATED whole block
        // (offset 0, never shared); at or below it the segment is PACKED into a shared partial block
        // alongside other columns' segments. Single source of truth for the write-side "dedicated vs
        // shared" decision, used by the checkpoint flush path and the write-through transition alike.
        //
        // Invariant: every offset handed out is 8-byte aligned (partial_block_t::next_offset) —
        // offsets are dereferenced after restart with typed pointers up to uint64_t wide.
        static constexpr double FULL_THRESHOLD = 0.8;
        // Tails kept open after a flush (for_appends only): one for the row-group-sized fixed
        // segments, one for the string segments, so neither evicts the other every row group.
        // Unbounded, an 8-text-column table reached 120 open tails (30 MiB of buffers); 2 costs
        // +2% file on that table and nothing on single-text-column ones.
        static constexpr uint64_t MAX_OPEN_TAILS = 2;
        // A tail with less room than this is written and dropped as soon as it gets there: no
        // segment of the write-through fits it.
        static constexpr uint64_t MIN_REUSABLE_TAIL = 4096;

        // A partial block survives flush_partial_blocks() and keeps taking segments across later
        // rounds (each flush writes only what was appended). Legal only while no durable root names
        // the block -- the owner must seal() before a checkpoint can reference a block written here.
        static partial_block_manager_t for_appends(block_manager_t& block_manager);
        // Every flush writes its blocks whole and forgets them.
        static partial_block_manager_t for_checkpoint(block_manager_t& block_manager);

        // A packer that dies with tails nothing of which reached the file gives their ids back.
        ~partial_block_manager_t();
        partial_block_manager_t(partial_block_manager_t&&) noexcept = default;

        // Copies `size` (> 0) bytes of `data` into a block image and answers where they went: a
        // dedicated block above FULL_THRESHOLD, else the first open tail with room, else a fresh tail.
        // The image is written and dropped as soon as nothing else can land in it (a dedicated block
        // at once, a tail once its room is below MIN_REUSABLE_TAIL); only the open tails stay
        // buffered until flush_partial_blocks(). Rejected: holding every image of one append until
        // its flush -- compact then held a second copy of the table (76 images, 19 MB, on 300k x 16
        // INT32). A resident copy of a grown tail that cannot be pinned is an error: that copy would
        // keep serving zeros.
        // `placement` is adopted at the next flush_partial_blocks()/seal(), never inside place():
        // the caller still holds the placed segment and a pin of its block here.
        [[nodiscard]] core::result_wrapper_t<partial_block_allocation_t>
        place(const void* data, uint64_t size, std::unique_ptr<placement_t> placement = nullptr);

        // Writes the open tails (for_appends: what was appended since the last flush, and keeps
        // them; for_checkpoint: whole, and forgets them). Returns io_error on failure: every column
        // segment reaches the file through here, so a `void` would leave a failed data-block write
        // invisible up to a committed header. Adopts the placements of every block on the file; a
        // refused write destroys the placements of the tails not written, which stay transient.
        [[nodiscard]] core::result_wrapper_t<bool> flush_partial_blocks();

        // Flushes, then forgets every open tail: the next placement starts a fresh block.
        [[nodiscard]] core::error_t seal();

#ifdef DEV_MODE
        // Fault seam: the next seal() in the process answers io_error without flushing. One-shot and
        // process-wide, like single_file_block_manager_t::dev_set_file_interposer; every append
        // flushes its own writes, so nothing but a seam can make a seal fail in a test.
        static void dev_refuse_next_seal();
#endif

    private:
        enum class tails_t : uint8_t
        {
            kept,
            dropped
        };

        struct placed_t {
            partial_block_allocation_t at;
            std::unique_ptr<placement_t> placement;
        };

        // An open tail and its image: the only block images that outlive place().
        struct partial_block_t {
            uint64_t block_id;
            uint32_t used_bytes;
            uint64_t block_capacity;
            uint32_t flushed_bytes; // bytes of this image already on disk under block_id
            std::unique_ptr<block_t> image;
            uint32_t crc; // CRC32C of payload [0, flushed_bytes), grown per flush
            // The placements of the bytes in [flushed_bytes, used_bytes), and how many placements
            // without one (overflow records, checkpoint copies) landed there too.
            std::pmr::vector<placed_t> placements;
            uint32_t plain_since_flush;

            // Where the next segment goes: every offset is 8-byte aligned.
            uint64_t next_offset() const;
            uint64_t free_space() const;
        };

        partial_block_manager_t(block_manager_t& block_manager, tails_t tails);

        std::unique_ptr<block_t> fresh_image(uint64_t block_id);
        [[nodiscard]] core::error_t flush_tail(partial_block_t& pb);
        void keep_reusable_tails();
        void ready(std::pmr::vector<placed_t>& placements);
        // Every placement of the tail since its last write died (a refused append unwound their
        // segments) and nothing else landed there: the bytes are not worth a write.
        static bool dead_since_flush(const partial_block_t& pb);

        block_manager_t& block_manager_;
        tails_t tails_;
        std::vector<partial_block_t> partial_blocks_;
        std::pmr::vector<placed_t> ready_;
    };

} // namespace components::table::storage

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include <core/result_wrapper.hpp>

#include "block_manager.hpp"
#include "file_buffer.hpp"

namespace components::table::storage {

    // A single 256KB block is divided into 64 sub-blocks of ~4KB each.
    static constexpr uint32_t META_SUB_BLOCKS_PER_BLOCK = 64;

    class metadata_manager_t {
    public:
        explicit metadata_manager_t(block_manager_t& block_manager);

        // Allocate a sub-block handle, returns meta_block_pointer_t
        meta_block_pointer_t allocate_handle();

        // Pre-allocate BLOCKS until this manager can hand out `sub_blocks` more sub-blocks
        // without calling block_manager_.free_block_id() again.
        //
        // The one caller is single_file_block_manager_t::serialize_free_list, and the reason is
        // narrow and important: the free list it publishes is a snapshot of the very pool
        // free_block_id draws from, so a chain block allocated MID-WRITE is an id the published
        // list already calls free. Reserving the whole chain up front moves every one of those
        // allocations to BEFORE the snapshot, which is what takes them out of it.
        //
        // Reserved-but-unused sub-blocks cost nothing on disk: a block only becomes dirty when
        // allocate_handle actually hands one of its sub-blocks out.
        void reserve(uint64_t sub_blocks);

        // Pin a sub-block and return a pointer to its data, or nullptr if loading the backing block from
        // disk failed (data_corruption/io_error). On the load path pin runs inside the table_storage_t DISK
        // ctor (reachable on the agent thread via bootstrap_disk_inner_sync, noexcept), so it MUST NOT throw:
        // instead it records a sticky error_t here that metadata_reader_t propagates. The write path never
        // triggers a disk read in pin, so has_error() stays clear there.
        std::byte* pin(meta_block_pointer_t pointer);

        bool has_error() const noexcept { return error_.contains_error(); }
        const core::error_t& error() const noexcept { return error_; }

        // Get the size of a single sub-block
        uint64_t sub_block_size() const { return sub_block_size_; }

        // A7.3. Follow a metadata sub-block chain from `start` and collect the underlying
        // BLOCK ids, deduplicated: one block backs META_SUB_BLOCKS_PER_BLOCK sub-blocks, so a
        // long chain can live in a single block and a short one can span several.
        //
        // ONE implementation, on purpose. The A7.3 reclaim needs the chain blocks of the
        // superseded root, and the test-side reachability walker needs the same thing to judge
        // the reclaim; two walkers would be two notions of "the chain", free to disagree about
        // the very thing under test. block_reachability_walker.hpp calls this.
        //
        // Every byte followed here came off the disk, so a cycle or an unreadable block is
        // CORRUPT INPUT, not a violated invariant: both return data_corruption / io_error
        // rather than asserting (an assert vanishes under NDEBUG and turns a corrupt chain
        // into an unbounded loop or a wild read).
        [[nodiscard]] core::result_wrapper_t<bool> chain_blocks(meta_block_pointer_t start,
                                                                std::pmr::vector<uint64_t>& out);

        // Flush all dirty metadata blocks to disk. Returns io_error when any of those block
        // writes failed: this is the write half of the pin()/read sticky-error pattern above,
        // and it feeds the checkpoint's result channel. The error is ALSO latched into the same
        // error_ so a caller that only checks has_error() still sees it.
        [[nodiscard]] core::result_wrapper_t<bool> flush();

        block_manager_t& block_manager() { return block_manager_; }

    private:
        struct metadata_block_t {
            uint64_t block_id;
            std::unique_ptr<block_t> block;
            uint32_t next_free_sub_block;
            bool dirty;
        };

        block_manager_t& block_manager_;
        uint64_t sub_block_size_;
        std::mutex lock_;
        std::vector<metadata_block_t> blocks_;
        // Sticky load error: set by pin() when block_manager_.read() fails, surfaced to metadata_reader_t.
        core::error_t error_{core::error_t::no_error()};
    };

} // namespace components::table::storage

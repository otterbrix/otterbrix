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

        // Pre-allocate BLOCKS until this manager can hand out `sub_blocks` more without calling
        // free_block_id() again. Caller: serialize_free_list, whose published list is a snapshot
        // of the same pool free_block_id draws from — reserving up front moves any mid-write
        // chain-block allocation to before that snapshot. Unused sub-blocks cost nothing on disk.
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

        // Follow a metadata sub-block chain from `start` and collect the underlying BLOCK ids,
        // deduplicated (one block backs META_SUB_BLOCKS_PER_BLOCK sub-blocks). ONE implementation
        // on purpose, shared by the superseded-root reclaim and the test-side reachability walker
        // (block_reachability_walker.hpp), so they can't disagree about what "the chain" is.
        // Every byte here came off disk, so a cycle or unreadable block is data_corruption /
        // io_error, not an assert that vanishes under NDEBUG.
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

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

        meta_block_pointer_t allocate_handle();

        // Called before serialize_free_list snapshots the pool, so a mid-write chain-block
        // allocation can't land after the snapshot.
        void reserve(uint64_t sub_blocks);

        // nullptr on a failed disk load (data_corruption/io_error). Must not throw (this runs inside
        // a noexcept ctor on the load path), so it records a sticky error_t that metadata_reader_t propagates.
        std::byte* pin(meta_block_pointer_t pointer);

        bool has_error() const noexcept { return error_.contains_error(); }
        const core::error_t& error() const noexcept { return error_; }

        uint64_t sub_block_size() const { return sub_block_size_; }

        // Deduplicated (one block backs META_SUB_BLOCKS_PER_BLOCK sub-blocks). A cycle or unreadable
        // block is data_corruption/io_error, not an assert that would vanish under NDEBUG. ONE
        // implementation on purpose, shared with block_reachability_walker.hpp so prod and test
        // cannot disagree about what "the chain" is.
        [[nodiscard]] core::result_wrapper_t<bool> chain_blocks(meta_block_pointer_t start,
                                                                std::pmr::vector<uint64_t>& out);

        // Returns io_error when a block write fails, also latched into error_ so a caller that
        // only checks has_error() still sees it.
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

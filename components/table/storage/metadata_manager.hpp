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

        // Flush all dirty metadata blocks to disk
        void flush();

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

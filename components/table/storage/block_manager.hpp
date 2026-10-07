#pragma once

#include <memory>
#include <memory_resource>
#include <optional>
#include <unordered_map>
#include <vector>

#include <core/result_wrapper.hpp>

#include "file_buffer.hpp"

namespace components::table::storage {
    class block_handle_t;
    class buffer_handle_t;
    class buffer_manager_t;

    class block_manager_t {
    public:
        block_manager_t() = delete;
        block_manager_t(buffer_manager_t& buffer_manager, uint64_t block_alloc_size);
        virtual ~block_manager_t() = default;

        buffer_manager_t& buffer_manager;

        virtual std::unique_ptr<block_t> convert_block(uint64_t block_id, file_buffer_t& source_buffer) = 0;
        virtual std::unique_ptr<block_t> create_block(uint64_t block_id, file_buffer_t* source_buffer) = 0;

        virtual uint64_t free_block_id() = 0;
        virtual uint64_t peek_free_block_id() = 0;
        virtual bool is_root_block(meta_block_pointer_t root) = 0;
        virtual void mark_as_free(uint64_t block_id) = 0;
        virtual void mark_as_used(uint64_t block_id) = 0;
        virtual void mark_as_modified(uint64_t block_id) = 0;
        virtual void increase_block_ref_count(uint64_t block_id) = 0;
        virtual uint64_t meta_block() = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> read(block_t& block) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t block_count) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> write(file_buffer_t& block, uint64_t block_id) = 0;
        [[nodiscard]] core::result_wrapper_t<bool> write(block_t& block) { return write(block, block.id); }
        // Rewrites the checksum slot and payload bytes [offset, offset+length) of a block whose
        // other bytes are already on disk unchanged. A manager without positional writes writes
        // the whole block, which is the same bytes. `covered_crc` is the caller's CRC32C of payload
        // [0, offset+length), kept incrementally; without it the manager computes it.
        [[nodiscard]] core::error_t write_range(file_buffer_t& block,
                                                uint64_t block_id,
                                                uint64_t offset,
                                                uint64_t length,
                                                std::optional<uint32_t> covered_crc = std::nullopt) {
            return write_range_impl(block, block_id, offset, length, covered_crc);
        }
        // First write of a block whose payload beyond `length` is zero: a block past the end of the
        // file is written as the prefix over a sparse extension; a reused id (old bytes on disk
        // past the prefix) is written whole.
        [[nodiscard]] core::error_t write_prefix(file_buffer_t& block, uint64_t block_id, uint64_t length) {
            return write_prefix_impl(block, block_id, length);
        }

        virtual void adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& /*block_ids*/) {}
        // Frees root N once root N+1 is fully written; otherwise checkpointing an unchanged table grows the file.
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t>
        reclaim_superseded_root(const std::pmr::vector<uint64_t>& /*new_root_data_blocks*/) {
            return uint64_t{0};
        }

        // Sticky once latched; a degraded manager never promotes pending_free_ again.
        virtual bool degraded() const { return false; }

        virtual uint64_t total_blocks() = 0;
        virtual uint64_t free_blocks() = 0;
        virtual bool is_remote() { return false; }
        // Data and metadata blocks must reach the DEVICE before the root naming them becomes durable.
        [[nodiscard]] virtual core::result_wrapper_t<bool> file_sync() = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> truncate();

        std::shared_ptr<block_handle_t> register_block(uint64_t block_id);

        // Identity-checked: a stale handle destroyed after re-registration can't drop the fresh slot.
        void unregister_block(block_handle_t& block);
        void unregister_block(uint64_t id);

        bool registry_alive(uint64_t id);

        std::pmr::vector<uint64_t> live_registry_ids();

#ifdef DEV_MODE
        std::pmr::vector<uint64_t> dev_live_registry_ids() { return live_registry_ids(); }
#endif

        uint64_t block_allocation_size() const { return block_alloc_size_; }
        uint64_t block_size() const { return block_alloc_size_ - DEFAULT_BLOCK_HEADER_SIZE; }

        // DISK-FED: an unvalidated too-small header size would wrap block_size()'s subtraction to ~1.8e19.
        [[nodiscard]] core::result_wrapper_t<bool> set_block_allocation_size(uint64_t block_alloc_size);

    private:
        virtual core::error_t write_range_impl(file_buffer_t& block,
                                               uint64_t block_id,
                                               uint64_t offset,
                                               uint64_t length,
                                               std::optional<uint32_t> covered_crc);
        virtual core::error_t write_prefix_impl(file_buffer_t& block, uint64_t block_id, uint64_t length);

        // NO LOCK: exactly one block manager is reachable from exactly one disk agent thread, by construction.
        std::pmr::unordered_map<uint64_t, std::weak_ptr<block_handle_t>> blocks_;
        uint64_t block_alloc_size_;
    };

} // namespace components::table::storage
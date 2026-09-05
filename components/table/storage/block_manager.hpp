#pragma once

#include <memory>
#include <memory_resource>
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
        // Returns true on success, or core::error_code_t::data_corruption (checksum mismatch on disk reload) /
        // io_error. Disk reload makes the checksum path reachable on the agent thread, so the failure must
        // surface via result_wrapper_t rather than a throw.
        [[nodiscard]] virtual core::result_wrapper_t<bool> read(block_t& block) = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t block_count) = 0;
        // NOT `void`: a discarded bool here would make a block that never reached the file
        // indistinguishable from one that did, and the header swap that follows would make the
        // hole durable. The disk implementation also latches the first failure so write_header()
        // refuses to commit over it.
        [[nodiscard]] virtual core::result_wrapper_t<bool> write(file_buffer_t& block, uint64_t block_id) = 0;
        [[nodiscard]] core::result_wrapper_t<bool> write(block_t& block) { return write(block, block.id); }

        // Reclaiming the SUPERSEDED root: shadow paging leaves root N standing while root N+1 is
        // built, so without taking it down its blocks stay allocated forever and a checkpoint of
        // an unchanged table extends the file every round. Only the single-file (disk) manager
        // has a root at all; the transient one keeps the no-op defaults below.
        //
        // `adopt_durable_root_data_blocks` is how the LOADER states what the durable root
        // references, collected out of the same row_group_pointer_t stream load_from_disk builds
        // the table from, so it cannot drift from what a reload would actually read back.
        virtual void adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& /*block_ids*/) {}
        // Frees root N now that every block of root N+1 is written. Returns how many ids were
        // reclaimed, or an error if root N's chains cannot be read (corrupt input the checkpoint
        // must not commit on top of).
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t>
        reclaim_superseded_root(const std::pmr::vector<uint64_t>& /*new_root_data_blocks*/) {
            return uint64_t{0};
        }

        // Has this manager latched an unrecoverable failure (a write/fsync that missed the
        // device, or a corrupt free list)? Both latches are sticky and block write_header, so a
        // degraded manager never promotes pending_free_ again. Callers use this to stop
        // rebuilding, not to paper over the failure.
        virtual bool degraded() const { return false; }

        virtual uint64_t total_blocks() = 0;
        virtual uint64_t free_blocks() = 0;
        virtual bool is_remote() { return false; }
        // The pre-header barrier: data and metadata blocks must be on the DEVICE before the root
        // that names them becomes durable. A `virtual void` over a dropped bool would make the
        // barrier decorative -- the header could commit over blocks still in the page cache.
        [[nodiscard]] virtual core::result_wrapper_t<bool> file_sync() = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> truncate();

        std::shared_ptr<block_handle_t> register_block(uint64_t block_id);

        // Drops THIS HANDLE's registry slot. Identity-checked: a stale handle destroyed after
        // its id was re-registered to a fresh handle leaves the fresh one's slot alone (see the
        // long note at the definition).
        void unregister_block(block_handle_t& block);
        // Drops the slot for an ID, whoever holds it: the deliberate ABA break used by
        // data_table_t::compact and reclaim_superseded_root when returning an id to the free pool.
        void unregister_block(uint64_t id);

        // Does this id have a LIVE block_handle_t right now? Handing it out again would overwrite
        // live table state with a valid CRC. NOT DEV_MODE-only: free_block_id draws from an
        // on-disk free list, so this guards a path fed by untrusted bytes.
        bool registry_alive(uint64_t id);

        // Every registry id whose block_handle_t is still alive. A production API: serialize_free_list
        // must name the blocks whose ONLY owner is the live in-memory tree, since a restart has
        // no other way to ever find them again.
        std::pmr::vector<uint64_t> live_registry_ids();

#ifdef DEV_MODE
        // Block-reachability walker: the registry ids whose block_handle_t is still
        // alive (the weak_ptr locks). Same answer as live_registry_ids(); kept as a separate
        // name so test call sites read as diagnostics, not as production dependencies.
        std::pmr::vector<uint64_t> dev_live_registry_ids() { return live_registry_ids(); }
#endif

        uint64_t block_allocation_size() const { return block_alloc_size_; }
        uint64_t block_size() const { return block_alloc_size_ - DEFAULT_BLOCK_HEADER_SIZE; }

        // Adopt a block allocation size read off an existing file's header
        // (load_existing_database). DISK-FED, so it must validate: block_size() is an unsigned
        // subtraction, and a header claiming a too-small size would wrap it to ~1.8e19. Reported,
        // never thrown; leaves the current size untouched on refusal.
        [[nodiscard]] core::result_wrapper_t<bool> set_block_allocation_size(uint64_t block_alloc_size);

    private:
        // NO LOCK HERE — same ownership argument as data_table_t's row_groups_: one
        // table_storage_t owns exactly ONE block manager, reachable from exactly one disk agent
        // thread. A caller reaching this registry from another thread has smuggled a table or
        // handle across a mailbox boundary — a defect a mutex would hide, not fix.
        std::pmr::unordered_map<uint64_t, std::weak_ptr<block_handle_t>> blocks_;
        uint64_t block_alloc_size_;
    };

} // namespace components::table::storage
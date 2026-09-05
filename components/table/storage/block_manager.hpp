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
        // NOT `void`: a `void` here, all the way down through file_buffer_t::write into a
        // discarded bool of file_handle_t::write, makes a block that never reached the file
        // indistinguishable from one that did, and the header swap that follows makes the hole
        // durable. Every write answers, and the disk implementation ALSO latches the first
        // failure so write_header() refuses to commit a root over it.
        [[nodiscard]] virtual core::result_wrapper_t<bool> write(file_buffer_t& block, uint64_t block_id) = 0;
        [[nodiscard]] core::result_wrapper_t<bool> write(block_t& block) { return write(block, block.id); }

        // --- Reclaiming the SUPERSEDED root ---
        //
        // Shadow paging leaves root N standing while root N+1 is built, so without taking root N
        // down its chains and packed data blocks stay allocated forever and a checkpoint of an
        // UNCHANGED table extends the file every round. Only the single-file (disk) manager has a
        // root at all; the transient one keeps the no-op defaults, so data_table_t can call these
        // unconditionally.
        //
        // `adopt_durable_root_data_blocks` is how the LOADER states what the durable root
        // references: data_table_t::load_from_disk collects the ids out of the very
        // row_group_pointer_t stream it builds the table from, so the manager's idea of "root N's
        // data" cannot drift from what the loader would actually read back.
        virtual void adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& /*block_ids*/) {}
        // Frees root N now that every block of root N+1 is written. Returns how many ids were
        // reclaimed, or an error if root N's chains cannot be read (that is corrupt input, and
        // the checkpoint must not commit on top of state it cannot account for).
        [[nodiscard]] virtual core::result_wrapper_t<uint64_t>
        reclaim_superseded_root(const std::pmr::vector<uint64_t>& /*new_root_data_blocks*/) {
            return uint64_t{0};
        }

        // Has this manager latched a failure it cannot recover from (a write/fsync that did not reach
        // the device, or a free list proven corrupt)? Both latches are sticky and both make
        // write_header refuse to commit, so a degraded manager never promotes pending_free_ again —
        // rebuilding a table on top of one costs a full extra copy of it EVERY round, for the life of
        // the process. Callers use this to stop rebuilding, not to paper over the failure.
        virtual bool degraded() const { return false; }

        virtual uint64_t total_blocks() = 0;
        virtual uint64_t free_blocks() = 0;
        virtual bool is_remote() { return false; }
        // The pre-header barrier: data and metadata blocks must be on the DEVICE before the
        // root that names them becomes durable. A `virtual void` over a dropped
        // `handle_->sync()` bool would make the barrier decorative — the header could commit
        // over blocks that never left the page cache. An unobserved barrier is the same as no
        // barrier, so this one reports.
        [[nodiscard]] virtual core::result_wrapper_t<bool> file_sync() = 0;
        [[nodiscard]] virtual core::result_wrapper_t<bool> truncate();

        std::shared_ptr<block_handle_t> register_block(uint64_t block_id);

        // Drops THIS HANDLE's registry slot. Identity-checked: a stale handle destroyed after
        // its id was re-registered to a fresh handle leaves the fresh one's slot alone. See the
        // long note at the definition — the id-only erase was how the reclaim came to free a
        // block a live segment was still reading.
        void unregister_block(block_handle_t& block);
        // Drops the slot for an ID, whoever holds it. The deliberate ABA break in
        // data_table_t::compact and reclaim_superseded_root: the id is being returned to the
        // free pool, so no handle may be resurrected for it by a later register_block.
        void unregister_block(uint64_t id);

        // Does this id have a LIVE block_handle_t in the registry right now? A block that does is live
        // table state, and handing it out again would overwrite it with a valid CRC. NOT a DEV_MODE
        // hook: free_block_id draws from a free list deserialized out of the .otbx, so this guards a
        // path fed by untrusted bytes and must exist in the build where corruption costs something.
        bool registry_alive(uint64_t id);

        // Every registry id whose block_handle_t is still alive — registry_alive(), enumerated. A
        // production API, not a diagnostic: serialize_free_list must name the blocks whose ONLY owner
        // is the live in-memory tree (no root's pointer stream references them), because a restart
        // has no other way to ever find them again. See the note there.
        std::pmr::vector<uint64_t> live_registry_ids();

#ifdef DEV_MODE
        // Block-reachability walker: the registry ids whose block_handle_t is still
        // alive (the weak_ptr locks). Same answer as live_registry_ids(); kept as a separate
        // name so test call sites read as diagnostics, not as production dependencies.
        std::pmr::vector<uint64_t> dev_live_registry_ids() { return live_registry_ids(); }
#endif

        uint64_t block_allocation_size() const { return block_alloc_size_; }
        uint64_t block_size() const { return block_alloc_size_ - DEFAULT_BLOCK_HEADER_SIZE; }

        // Adopt a block allocation size — in practice the one the header sector of an existing file
        // carries (single_file_block_manager_t::load_existing_database is the only caller), which
        // makes this a DISK-FED path that must validate: block_size() is an UNSIGNED subtraction, so
        // a header claiming 4 wraps it to ~1.8e19 and every buffer bound derived from it becomes
        // meaningless. Reported, never thrown — this runs on the open path, where an exception makes
        // the database permanently unopenable (rules 2/6). Returns data_corruption for a size that
        // cannot address this file layout, leaving the current size untouched.
        [[nodiscard]] core::result_wrapper_t<bool> set_block_allocation_size(uint64_t block_alloc_size);

    private:
        // NO LOCK HERE (rule 12) — the same ownership argument data_table_t records for its own
        // row_groups_: one table_storage_t owns exactly ONE block manager and lives in exactly one
        // disk agent's storages_ map (oids route by pool_idx_for_oid), actor-zeta resumes an actor on
        // at most one thread, nothing beneath data_table_t is handed to another actor, buffer-pool
        // eviction runs INLINE on the allocating thread, and the manager-side *_sync paths run before
        // the schedulers start. This registry is agent-local state, so a caller reaching it from
        // another thread has smuggled a table or a block_handle_t across a mailbox boundary — a
        // DEFECT IN THAT CALLER that a mutex would hide instead of fix.
        std::pmr::unordered_map<uint64_t, std::weak_ptr<block_handle_t>> blocks_;
        uint64_t block_alloc_size_;
    };

} // namespace components::table::storage
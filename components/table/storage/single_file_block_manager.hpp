#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "block_manager.hpp"
#include "buffer_manager.hpp"

namespace core::filesystem {
    class local_file_system_t;
    struct file_handle_t;
} // namespace core::filesystem

namespace components::table::storage {

    static constexpr uint64_t BLOCK_START = 3 * SECTOR_SIZE; // 12288

    struct main_header_t {
        static constexpr uint32_t MAGIC_NUMBER = 0x5842544F; // "OTBX" little-endian
        // The on-disk layout is PRE-RELEASE: the format changes in place and the version stays 0.
        // A stale file from an earlier build of this branch is only caught by ACCIDENT (those
        // builds left checksum dead at 0, failing CRC) — wipe stale test/dev directories rather
        // than relying on it. The equality check stays as the mechanism the FIRST version bump needs.
        static constexpr uint32_t CURRENT_VERSION = 0;

        uint32_t magic;
        uint32_t version;
        uint64_t flags;
        uint8_t padding[SECTOR_SIZE - sizeof(uint32_t) - sizeof(uint32_t) - sizeof(uint64_t)];

        void initialize() {
            std::memset(this, 0, sizeof(*this));
            magic = MAGIC_NUMBER;
            version = CURRENT_VERSION;
            flags = 0;
        }

        // EXACT match, not <=: the metadata layout is version-specific, so an older file is
        // unreadable rather than degraded. There is no compatibility path by design.
        bool validate() const { return magic == MAGIC_NUMBER && version == CURRENT_VERSION; }
        bool magic_ok() const { return magic == MAGIC_NUMBER; }
    };
    static_assert(sizeof(main_header_t) == SECTOR_SIZE, "main_header_t must be SECTOR_SIZE");

    struct database_header_t {
        uint64_t iteration;
        uint64_t meta_block;
        uint64_t free_list;
        uint64_t block_count;
        uint64_t block_alloc_size;
        uint64_t checksum;
        uint8_t padding[SECTOR_SIZE - 6 * sizeof(uint64_t)];

        void initialize() {
            std::memset(this, 0, sizeof(*this));
            iteration = 0;
            meta_block = INVALID_INDEX;
            free_list = INVALID_INDEX;
            block_count = 0;
            block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE;
            checksum = 0;
        }

        // Self-validating: a CRC32c over EVERY byte of the sector except the 8-byte checksum slot
        // (write_header() zeroes it first). Catches an unwritten slot (would otherwise read back
        // as zeros = a tied "iteration 0" pointing at a real block id), garbage, and bit rot. Does
        // NOT catch a TORN write: every differing byte lives in the same 512-byte hardware sector,
        // so a torn write still reassembles into one byte-exact generation and passes — the
        // two-slot layout is what survives a tear, not this checksum.
        //
        // Defined out of line so the CRC dependency (absl) stays in the .cpp.
        [[nodiscard]] uint64_t compute_checksum() const;
        // A slot read from disk is DISK BYTES: never assert on this, branch on it. Callers on
        // the open path must treat a false here as "this slot does not exist".
        [[nodiscard]] bool checksum_ok() const { return checksum == compute_checksum(); }
    };
    static_assert(sizeof(database_header_t) == SECTOR_SIZE, "database_header_t must be SECTOR_SIZE");

    class single_file_block_manager_t : public block_manager_t {
    public:
        single_file_block_manager_t(buffer_manager_t& buffer_manager,
                                    core::filesystem::local_file_system_t& fs,
                                    const std::string& path,
                                    uint64_t block_alloc_size = DEFAULT_BLOCK_ALLOC_SIZE);
        ~single_file_block_manager_t() override;

        // Return io_error / data_corruption on file create/open/header failure. Called only on the
        // single-threaded bootstrap/load path (load_storage_disk_sync), which reports the error
        // and refuses the file rather than mutating it — recovery is the two-slot root
        // reconciliation inside load_existing_database itself.
        //
        // A MANAGER WHOSE LOAD WAS REFUSED IS NOT FIT FOR USE — destroy it. A refusal partway
        // through can land after some header fields are already adopted (needed to interpret the
        // rest of the file), but the allocator pool stays clean either way: deserialize_free_list
        // installs all-or-nothing.
        [[nodiscard]] core::result_wrapper_t<bool> create_new_database();
        [[nodiscard]] core::result_wrapper_t<bool> load_existing_database();

        std::unique_ptr<block_t> convert_block(uint64_t block_id, file_buffer_t& source_buffer) override;
        std::unique_ptr<block_t> create_block(uint64_t block_id, file_buffer_t* source_buffer) override;

        uint64_t free_block_id() override;
        uint64_t peek_free_block_id() override;
        bool is_root_block(meta_block_pointer_t root) override;
        void mark_as_free(uint64_t block_id) override;
        void mark_as_used(uint64_t block_id) override;
        void mark_as_modified(uint64_t block_id) override;
        void increase_block_ref_count(uint64_t block_id) override;
        uint64_t meta_block() override;
        void set_meta_block(uint64_t block) { meta_block_ = block; }
        [[nodiscard]] core::result_wrapper_t<bool> read(block_t& block) override;
        [[nodiscard]] core::result_wrapper_t<bool>
        read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t block_count) override;
        [[nodiscard]] core::result_wrapper_t<bool> write(file_buffer_t& block, uint64_t block_id) override;

        void adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& block_ids) override;
        [[nodiscard]] core::result_wrapper_t<uint64_t>
        reclaim_superseded_root(const std::pmr::vector<uint64_t>& new_root_data_blocks) override;

        // Give back what a round that did NOT commit took, and nothing else. Returns how many ids
        // were released (0 after a committed round, or when an INDETERMINATE header write
        // refuses on purpose).
        //
        // PRECONDITION -- the whole safety argument: the round's header must NOT have become the
        // durable root (a failure before any header slot was written, or
        // reconcile_failed_header_write case 2, whose read-back PROVES the previous root still
        // stands). The indeterminate case is refused structurally (durable_root_indeterminate_).
        // Harmless after a SUCCESSFUL round: promote_durable_root already cleared
        // issued_since_root_.
        uint64_t roll_back_uncommitted_round();

        bool degraded() const override {
            return durability_error_.contains_error() || allocation_error_.contains_error();
        }

        uint64_t total_blocks() override;
        uint64_t free_blocks() override;
        [[nodiscard]] core::result_wrapper_t<bool> file_sync() override;
        [[nodiscard]] core::result_wrapper_t<bool> truncate() override;

        // Writes AND fsyncs the one header slot this checkpoint owns. Returns io_error when
        // either fails: this is the single point of durability of a checkpoint, so a caller
        // that ignores the answer is a caller that reports a checkpoint which never happened.
        [[nodiscard]] core::result_wrapper_t<bool> write_header(const database_header_t& header);

        // Writes the free list into metadata blocks, so it can fail exactly like any other
        // block write: returning the pointer unconditionally would hand table_storage_t::checkpoint
        // a root pointer to a chain that was never laid down.
        //
        // What it persists is reusable_ ∪ pending_free_ — see the justification at the definition.
        [[nodiscard]] core::result_wrapper_t<meta_block_pointer_t> serialize_free_list();
        // Loads the free list OF THE DURABLE ROOT into reusable_ (never pending_free_, since
        // nothing is in flight at load time), and only as a whole -- a list refused for ANY id
        // installs NOTHING. PRECONDITION: max_block_ must already hold the header's block_count
        // (load_existing_database installs it first), since the guard measures the file's extent
        // through it -- a free list is a header's own statement and can't be interpreted against
        // an unadopted extent.
        [[nodiscard]] core::result_wrapper_t<bool> deserialize_free_list(meta_block_pointer_t pointer);

        // Allocation error channel: free_block_id() can't return an error (uint64_t-returning
        // virtual), so a corrupt free list is latched here instead, dropping the offending id and
        // making the next write_header() refuse to commit. Sticky: a corrupt free list does not
        // heal.
        [[nodiscard]] bool has_allocation_error() const { return allocation_error_.contains_error(); }
        [[nodiscard]] const core::error_t& allocation_error() const { return allocation_error_; }

        // Durability latch: every block write/fsync failure is recorded here, and write_header()
        // refuses to commit while it is set. Sticky, because the hole does not heal -- a later
        // round only re-writes segments it considers dirty, and the failed segment is already
        // re-pointed at its never-written block. Loud, not fatal: reads/writes continue,
        // every checkpoint reports the error.
        [[nodiscard]] bool has_durability_error() const { return durability_error_.contains_error(); }
        [[nodiscard]] const core::error_t& durability_error() const { return durability_error_; }

        core::filesystem::file_handle_t& handle() const { return *handle_; }

#ifdef DEV_MODE
        // Fault-injection seam: a test installs an interposer that wraps the freshly
        // opened database file handle (programmable write failures, torn sectors, crash
        // simulation via an undo journal). Plain virtual interface, NOT std::function
        //; process-wide, DEV_MODE-only, read once per open.
        struct file_handle_interposer_t {
            virtual ~file_handle_interposer_t() = default;
            virtual std::unique_ptr<core::filesystem::file_handle_t>
            wrap(std::unique_ptr<core::filesystem::file_handle_t> inner) = 0;
        };
        static void dev_set_file_interposer(file_handle_interposer_t* interposer); // nullptr = off

        // Block-reachability walker hooks: every id issued/freed, in call order. Test-only
        // diagnostics -- the walker classifies each as durable-root-reachable / registry-live /
        // free-listed, and an id in none of the three is an accounting hole.
        const std::pmr::vector<uint64_t>& dev_issued_ids() const { return dev_issued_; }
        const std::pmr::vector<uint64_t>& dev_freed_ids() const { return dev_freed_; }
        // The whole free pool, both halves — "every id this manager considers unreferenced by
        // the root it is building". That is what the walker needs to explain an issued id.
        std::set<uint64_t> dev_free_list_snapshot() {
            std::set<uint64_t> all = reusable_;
            all.insert(pending_free_.begin(), pending_free_.end());
            return all;
        }
        // The shadow-paging gates need the two halves apart: reusable_ is what free_block_id may draw from
        // RIGHT NOW, pending_free_ is what the in-flight checkpoint released and the durable
        // root still points at.
        std::set<uint64_t> dev_reusable_snapshot() { return reusable_; }
        std::set<uint64_t> dev_pending_free_snapshot() { return pending_free_; }
        // The durable root's OWN data blocks (adopted by the loader, replaced at commit). The
        // walker needs the COMPLETE named set to tell "free-listed but the root reads it" (fatal)
        // from "free-listed because only the live tree holds it".
        std::set<uint64_t> dev_durable_root_data_snapshot() { return durable_root_data_; }
        void dev_reset_tracking() {
            dev_issued_.clear();
            dev_freed_.clear();
        }
#endif

    private:
        uint64_t block_location(uint64_t block_id) const;
        [[nodiscard]] core::result_wrapper_t<bool> checksum_and_write(file_buffer_t& buffer, uint64_t block_id);
        bool verify_checksum(file_buffer_t& buffer);
        // `reason` completes the sentence "refused block N: ", and is a std::string because
        // the file-extent case has to NAME the extent it measured against.
        void latch_allocation_error(uint64_t block_id, const std::string& reason);
        // A walk of the SUPERSEDED root's chains failed, so this manager can no longer say which
        // blocks root N owns. Latches into allocation_error_ (first failure wins). `which_chain`
        // completes "its ... chain". See the long note at the definition.
        core::error_t latch_reclaim_failure(const core::error_t& cause, const char* which_chain);
        // First failure wins; returns the latched error so the caller can propagate it.
        core::error_t latch_durability_error(core::error_t error);
        // After a header write or its fsync reported failure, READ THE SLOTS BACK and let the
        // disk decide what the durable root is. See the long note at the definition.
        [[nodiscard]] core::result_wrapper_t<bool>
        reconcile_failed_header_write(uint64_t next_iteration, bool write_ok, bool sync_ok);
        // Promotion point: pending_free_ -> reusable_, and the new root's chain
        // pointers + data blocks become THE durable root's. Called ONLY where the new root has
        // just become the durable one (write_header's success path, and the read-back branch of
        // reconcile_failed_header_write that proves the new header reached the device).
        void promote_durable_root(uint64_t meta_block, uint64_t free_list);

        core::filesystem::local_file_system_t& fs_;
        std::string path_;
        std::unique_ptr<core::filesystem::file_handle_t> handle_;

        // NO LOCK HERE, same reason as block_manager_t's registry: one table_storage_t
        // owns one block manager on exactly one disk agent thread, and the invariants span
        // several calls (snapshot-then-write, allocate-then-register) that no per-call lock could
        // cover anyway. A caller from another thread has smuggled state across a mailbox boundary.
        //
        // Shadow paging, half two: the free list is SPLIT because "free" is a statement about a
        // ROOT, and two roots are live at once during a checkpoint.
        //   reusable_     — free under the CURRENT DURABLE root; THE ONLY POOL free_block_id draws
        //                   from.
        //   pending_free_ — released by the IN-FLIGHT checkpoint, but the durable root still names
        //                   these, so issuing one would let the checkpoint's own metadata
        //                   overwrite a block the recoverable root still reads.
        // Disjoint (mark_as_free files into pending_free_ only when not already reusable); they
        // merge in exactly one place, promote_durable_root(), once write_header's slot write and
        // fsync have both succeeded.
        std::set<uint64_t> reusable_;
        std::set<uint64_t> pending_free_;
        std::set<uint64_t> used_blocks_;
        std::set<uint64_t> modified_blocks_;
        uint64_t max_block_{0};
        uint64_t iteration_{0};
        uint64_t meta_block_{INVALID_INDEX};

        // What root N is, kept separately from what root N+1 is becoming: meta_block_ is
        // overwritten the moment the new chain is written, so root N's own chains/data must be
        // remembered here BEFORE that happens. They move to match only when pending_free_ is
        // promoted, i.e. once the new header is proven on the device.
        uint64_t durable_meta_block_{INVALID_INDEX};
        uint64_t durable_free_list_{INVALID_INDEX};
        std::set<uint64_t> durable_root_data_;
        // The data blocks of the root UNDER CONSTRUCTION, handed over by data_table_t as it
        // writes them. Becomes durable_root_data_ on the same event.
        std::set<uint64_t> pending_root_data_;
        // Every id issued since the durable root was committed. Root N was built out of earlier
        // rounds' allocations only, so an id in here can never belong to it -- the cheap, exact
        // form of "minus the blocks of root N+1".
        std::set<uint64_t> issued_since_root_;
        // Set by the two reconcile_failed_header_write branches that cannot say which root is on
        // the device. Sticky, and it makes roll_back_uncommitted_round's precondition STRUCTURAL:
        // if the failed round's header MIGHT have landed, its blocks might be named by the durable
        // root, and giving them back is exactly the corruption shadow paging prevents.
        bool durable_root_indeterminate_{false};
        core::error_t allocation_error_{core::error_t::no_error()};
        core::error_t durability_error_{core::error_t::no_error()};

#ifdef DEV_MODE
        // Walker hook journals (see dev_issued_ids/dev_freed_ids above). Agent-local like the
        // sets they mirror.
        std::pmr::vector<uint64_t> dev_issued_;
        std::pmr::vector<uint64_t> dev_freed_;
#endif
    };

} // namespace components::table::storage

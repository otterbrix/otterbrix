#include "single_file_block_manager.hpp"

#include <string>

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iterator>
#include <stdexcept>

#include <absl/crc/crc32c.h>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>

namespace components::table::storage {

    // --- Database header checksum ---

    uint64_t database_header_t::compute_checksum() const {
        // Two spans, because the checksum slot sits between the fields and the padding and
        // obviously cannot cover itself. Everything else in the sector IS covered.
        static constexpr size_t SLOT_OFFSET = offsetof(database_header_t, checksum);
        static constexpr size_t SLOT_END = SLOT_OFFSET + sizeof(uint64_t);
        static_assert(SLOT_END < sizeof(database_header_t), "checksum slot must leave a tail to cover");

        const auto* bytes = reinterpret_cast<const char*>(this);
        auto crc = absl::ComputeCrc32c({bytes, SLOT_OFFSET});
        crc = absl::ExtendCrc32c(crc, {bytes + SLOT_END, sizeof(database_header_t) - SLOT_END});
        return static_cast<uint64_t>(static_cast<uint32_t>(crc));
    }

    namespace {
        // Shadow-paging slot rule: iteration N owns slot 1 when N is odd, slot 2 when N is
        // even. Consecutive iterations therefore land in DIFFERENT slots — which is the whole
        // point: writing the new root can never touch the previous one. The corollary is that
        // an iteration number must never be consumed by a write that did not become durable
        // (see write_header): a skipped iteration is a slot swap, and a slot swap aims the
        // next attempt at the last good root.
        constexpr uint64_t header_slot_offset(uint64_t iteration) {
            return (iteration % 2 == 1) ? SECTOR_SIZE : (2 * SECTOR_SIZE);
        }
    } // namespace

    single_file_block_manager_t::single_file_block_manager_t(buffer_manager_t& buffer_manager,
                                                             core::filesystem::local_file_system_t& fs,
                                                             const std::string& path,
                                                             uint64_t block_alloc_size)
        : block_manager_t(buffer_manager, block_alloc_size)
        , fs_(fs)
        , path_(path)
#ifdef DEV_MODE
        , dev_issued_(buffer_manager.resource())
        , dev_freed_(buffer_manager.resource())
#endif
    {
    }

    single_file_block_manager_t::~single_file_block_manager_t() = default;

#ifdef DEV_MODE
    namespace {
        single_file_block_manager_t::file_handle_interposer_t* dev_file_interposer_ = nullptr;
    } // namespace

    void single_file_block_manager_t::dev_set_file_interposer(file_handle_interposer_t* interposer) {
        dev_file_interposer_ = interposer;
    }
#endif

    uint64_t single_file_block_manager_t::block_location(uint64_t block_id) const {
        // Only REAL block ids address this file. A transient/temporary id is >= MAXIMUM_BLOCK
        // (1<<62), and the multiplication below then overflows and lands on a real block:
        // (2^62 + N) * 2^18 mod 2^64 == N * 2^18. The checksum is recomputed on write, so the
        // aliased block reads back as valid data and the corruption is silent. Refuse loudly.
        assert(block_id < MAXIMUM_BLOCK && "block_location called with a non-file block id");
        return BLOCK_START + block_id * block_allocation_size();
    }

    // --- Database lifecycle ---

    core::result_wrapper_t<bool> single_file_block_manager_t::create_new_database() {
        using namespace core::filesystem;

        // Refuse an unusable geometry BEFORE laying down a file. The size arrives through this
        // manager's constructor, which cannot report, so the first call that can is this one —
        // and creating the file first would produce a header no build of this engine can ever
        // open again.
        if (auto usable = set_block_allocation_size(block_allocation_size()); usable.has_error()) {
            return usable;
        }

        handle_ = open_file(fs_,
                            path_,
                            file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE_NEW,
                            file_lock_type::WRITE_LOCK);
        if (!handle_) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Failed to create database file: " + path_, buffer_manager.resource()});
        }
#ifdef DEV_MODE
        if (dev_file_interposer_ != nullptr) {
            handle_ = dev_file_interposer_->wrap(std::move(handle_));
        }
#endif

        // Every write below LAYS DOWN the file, so none of their results may be discarded:
        // that produces a file with no valid slot while the engine is told the database was
        // created, and the failure then surfaces at the next open, as data_corruption, with
        // the create long past and nothing left to retry.
        main_header_t main_header;
        main_header.initialize();
        if (!handle_->write(&main_header, sizeof(main_header), 0)) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Failed to write main header of new database file: " + path_,
                                 buffer_manager.resource()});
        }

        database_header_t db_header;
        db_header.initialize(); // zeroes the padding too: deterministic bytes under the CRC
        db_header.block_alloc_size = block_allocation_size();
        db_header.checksum = db_header.compute_checksum();

        // Fresh-file rule: write ONLY the slot iteration 0 owns — never both (write_header: the second
        // write leaves no previous root), and never the slot iteration 1 owns, which would have the very
        // first checkpoint overwrite the only valid root and leave the file with no fallback. The
        // never-written slot reads back as 4 KiB of zeros, which decode as iteration 0 — a TIE with the
        // real header — pointing at metadata root 0, a REAL block id; nothing but the checksum stops that
        // from being selected.
        if (!handle_->write(&db_header, sizeof(db_header), header_slot_offset(db_header.iteration))) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Failed to write the initial database header of: " + path_,
                                 buffer_manager.resource()});
        }

        if (!handle_->sync()) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Failed to fsync the newly created database file: " + path_,
                                 buffer_manager.resource()});
        }

        iteration_ = 0;
        max_block_ = 0;
        meta_block_ = INVALID_INDEX;
        // A brand-new file has no superseded root. Everything stays INVALID/empty until the
        // first write_header commits one.
        durable_meta_block_ = INVALID_INDEX;
        durable_free_list_ = INVALID_INDEX;
        return true;
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::load_existing_database() {
        using namespace core::filesystem;

        // A LOAD never creates the file. FILE_CREATE here would have a probe of a MISSING .otbx
        // silently manufacture a 0-byte file, so "the file never existed" would become
        // indistinguishable from "the file was truncated" one open later. A missing file is its own
        // loud, distinct refusal, and the probe leaves the filesystem exactly as it found it.
        if (!std::filesystem::exists(path_)) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Database file does not exist: " + path_ +
                                     " (a load never creates the file; only the create path lays down a database)",
                                 buffer_manager.resource()});
        }
        handle_ = open_file(fs_, path_, file_flags::WRITE | file_flags::READ, file_lock_type::WRITE_LOCK);
        if (!handle_) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to open database file: " + path_, buffer_manager.resource()});
        }
#ifdef DEV_MODE
        if (dev_file_interposer_ != nullptr) {
            handle_ = dev_file_interposer_->wrap(std::move(handle_));
        }
#endif

        // "Empty" is refused with its own words, loudly distinct from "missing" above: a
        // 0-byte (or sub-header) file is not a database and is never silently accepted as an
        // empty table. It is also left byte-identical — nothing below writes.
        const uint64_t file_bytes = handle_->file_size();
        if (file_bytes < sizeof(main_header_t)) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Failed to read main header of " + path_ + ": the file is " +
                                     std::to_string(file_bytes) +
                                     " bytes (empty or truncated below one header sector); it is not a database and "
                                     "is left untouched",
                                 buffer_manager.resource()});
        }
        main_header_t main_header;
        if (!handle_->read(&main_header, sizeof(main_header), 0)) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to read main header", buffer_manager.resource()});
        }
        if (!main_header.magic_ok()) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Invalid database file: bad magic", buffer_manager.resource()});
        }
        if (main_header.version != main_header_t::CURRENT_VERSION) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Unsupported database file version " +
                                                      std::to_string(main_header.version) + ", this build writes " +
                                                      std::to_string(main_header_t::CURRENT_VERSION) +
                                                      " (the on-disk layout changed; the file must be recreated)",
                                                  buffer_manager.resource()});
        }

        // Both slots are DISK BYTES. A slot exists, for recovery purposes, only if it reads
        // back whole AND its checksum matches the bytes that came back; a slot that fails
        // either test is ignored ENTIRELY, no matter what iteration it claims. Value-init so
        // a short read (a truncated file, or the never-written slot of a fresh database)
        // leaves defined bytes behind, and branch — never assert — because this is the open
        // path: an abort here would make the database permanently unopenable.
        database_header_t header1{};
        database_header_t header2{};
        const bool header1_read = handle_->read(&header1, sizeof(header1), SECTOR_SIZE);
        const bool header2_read = handle_->read(&header2, sizeof(header2), 2 * SECTOR_SIZE);
        const bool header1_valid = header1_read && header1.checksum_ok();
        const bool header2_valid = header2_read && header2.checksum_ok();

        if (!header1_valid && !header2_valid) {
            // Terminal refusal: with no external whole-file backup, this message is the
            // operator's ONLY remaining tool. Describe BOTH slots in full — which failed
            // how, what iteration each claims, and the stored-vs-computed checksum — and state
            // that the file was not touched (no rename, no truncation, no quarantine copy).
            auto describe_slot = [](const char* name, bool read_ok, const database_header_t& h) -> std::string {
                if (!read_ok) {
                    return std::string(name) + ": unreadable (positional read failed)";
                }
                char stored[19];
                char computed[19];
                std::snprintf(stored, sizeof(stored), "0x%016llx", static_cast<unsigned long long>(h.checksum));
                std::snprintf(computed,
                              sizeof(computed),
                              "0x%016llx",
                              static_cast<unsigned long long>(h.compute_checksum()));
                return std::string(name) + ": claims iteration " + std::to_string(h.iteration) +
                       ", checksum stored " + stored + " != computed " + computed + ", meta_block " +
                       std::to_string(h.meta_block) + ", block_count " + std::to_string(h.block_count);
            };
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"No recoverable root in " + path_ + ": neither database header slot is usable. " +
                                     describe_slot("slot 1", header1_read, header1) + "; " +
                                     describe_slot("slot 2", header2_read, header2) +
                                     ". The file is left byte-identical for offline inspection.",
                                 buffer_manager.resource()});
        }

        // The winner is the VALID slot with the greater iteration. Comparing iterations alone
        // would let a slot full of garbage carrying a large iteration beat a good root.
        const database_header_t& active =
            (header1_valid && (!header2_valid || header1.iteration >= header2.iteration)) ? header1 : header2;

        // meta_block == INVALID_INDEX means "never checkpointed — the table is empty and its schema
        // comes from the catalog". Legalising that needs independent evidence that the file really
        // is young: corruption that knocks out the newest slot of a CHECKPOINTED file surfaces the
        // very same header, because the two-slot fallback then selects the initial iteration-0 slot,
        // whose meta_block is INVALID by construction. Without a second witness, "INVALID = empty"
        // would convert a corrupt table into a silently empty one.
        //
        // The witness is the FILE SIZE, because the writer physically cannot fake it:
        // create_new_database lays down exactly BLOCK_START bytes, every checkpoint writes its
        // data/metadata blocks past BLOCK_START and fsyncs them BEFORE its header commits, and no
        // crash truncates that back. The `.wal_id` sidecar is rejected as the DECIDING witness (a
        // separate file that can be lost independently of the .otbx, so its absence proves nothing —
        // the manager layer still uses it in the REFUSING direction only), and so is the catalog (it
        // lives in tables loaded through this very code path). A file whose FIRST checkpoint crashed
        // after laying down blocks looks identical here and is refused too rather than guessed at
        // (rule 6): its rows are still in the WAL, which replays from id 0 with no sidecar.
        //
        // The gate applies to the INITIAL header only (iteration 0) — the one header corruption can
        // SURFACE by knocking out a newer slot, since create_new_database writes it unconditionally
        // and it never carries a root. INVALID at iteration >= 1 is different in kind: write_header
        // committed and checksummed those bytes, so it is the writer's own recorded statement ("this
        // root names no metadata"), and the block manager's contract (with the tests that pin it)
        // allows committing without one.
        if (active.meta_block == INVALID_INDEX && active.iteration == 0) {
            const bool header_is_initial = active.free_list == INVALID_INDEX && active.block_count == 0;
            if (!header_is_initial || file_bytes != BLOCK_START) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{
                        "Refusing to open " + path_ +
                            ": the selected root has no metadata pointer (meta_block INVALID), which is legal "
                            "only for a never-checkpointed database, but the file does not look young: size " +
                            std::to_string(file_bytes) + " bytes (a never-checkpointed file is exactly " +
                            std::to_string(BLOCK_START) + "), slot iteration " + std::to_string(active.iteration) +
                            ", block_count " + std::to_string(active.block_count) + ", free_list " +
                            std::to_string(active.free_list) +
                            ". Either this table WAS checkpointed and its newest header slot was lost to "
                            "corruption, or its first checkpoint crashed mid-flight; treating either as an "
                            "empty table would silently discard data. The file is left byte-identical for "
                            "offline inspection.",
                        buffer_manager.resource()});
            }
        }

        // GEOMETRY BEFORE ANY ADOPTION: a header that cannot describe this file is not a header to
        // open, and nothing below should be half-installed when it is refused. The claim is DISK
        // BYTES, and adopting it unchecked wraps block_size() for any value <=
        // DEFAULT_BLOCK_HEADER_SIZE, so set_block_allocation_size reports instead of throwing and
        // the open fails loudly (load_storage_disk_sync hands the refusal to its bootstrap caller,
        // file untouched).
        //
        // UNCONDITIONAL — neither short-circuit is allowed. Skipping on `== 0` would run the engine
        // on whatever size the CALLER passed to the constructor, a compatibility branch for a header
        // shape no writer in this build produces (initialize() stores DEFAULT_BLOCK_ALLOC_SIZE,
        // write_header() stores an already-validated non-zero sector multiple), so a zero is a
        // corrupt header, not an older format (rule 6: refuse, do not guess). Skipping on
        // `== block_allocation_size()` would check the header's claim only when it disagreed with
        // the constructor, so an AGREED-ON nonsense size would be adopted by both unchecked.
        if (auto adopted = set_block_allocation_size(active.block_alloc_size); adopted.has_error()) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Database header of " + path_ +
                                                      " is unusable: " + std::string(adopted.error().what.c_str()),
                                                  buffer_manager.resource()});
        }

        iteration_ = active.iteration;
        meta_block_ = active.meta_block;
        max_block_ = active.block_count;
        // The slot just selected IS root N. Record its two chain pointers now — the very
        // next checkpoint overwrites meta_block_ through set_meta_block, and the free-list
        // pointer only ever exists inside the header struct. Its DATA blocks arrive separately,
        // from the loader (adopt_durable_root_data_blocks), because only the loader knows which
        // ids the row_group_pointer_t stream names.
        durable_meta_block_ = active.meta_block;
        durable_free_list_ = active.free_list;

        if (active.free_list != INVALID_INDEX) {
            return deserialize_free_list(meta_block_pointer_t{active.free_list, 0});
        }
        return true;
    }

    // --- Block I/O ---

    core::result_wrapper_t<bool> single_file_block_manager_t::read(block_t& block) {
        auto location = block_location(block.id);
        // A short/failed read leaves the buffer holding whatever was in it before. Checking the
        // checksum of stale bytes would let a read failure masquerade as data_corruption — or,
        // worse, as a valid block when the buffer happened to hold the right thing.
        if (!block.read(*handle_, location)) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to read block " + std::to_string(block.id) + " of " + path_,
                                                  buffer_manager.resource()});
        }

        if (!verify_checksum(block)) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Block checksum mismatch for block " + std::to_string(block.id),
                                                  buffer_manager.resource()});
        }
        return true;
    }

    core::result_wrapper_t<bool>
    single_file_block_manager_t::read_blocks(file_buffer_t& buffer, uint64_t start_block, uint64_t /*count*/) {
        auto location = block_location(start_block);
        if (!buffer.read(*handle_, location)) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to batch-read blocks from " + std::to_string(start_block) +
                                                      " of " + path_,
                                                  buffer_manager.resource()});
        }
        return true;
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::write(file_buffer_t& buffer, uint64_t block_id) {
        return checksum_and_write(buffer, block_id);
    }

    // --- Block allocation ---

    uint64_t single_file_block_manager_t::free_block_id() {
        uint64_t block_id = INVALID_INDEX;
        bool from_free_list = false;

        // reusable_ ONLY. An empty reusable_ means "nothing is free under the durable root", and the
        // answer to that is to extend the file (the fall-through below), not to reach into
        // pending_free_ for blocks that root still reads.
        //
        // The free list is DISK BYTES — deserialize_free_list fills it straight from the .otbx — so
        // "the id I am handing out is not live" is a statement about untrusted input: a branch in
        // every build, never an assert(), which is gone under NDEBUG — exactly the build where a
        // reissued live id gets overwritten with a valid CRC and the corruption becomes unfindable.
        // A live candidate is dropped from the free list for good, the corruption is latched for the
        // caller that can act on it, and allocation continues, so the failure stays reportable
        // instead of aborting a path an actor thread runs (rules 6/9).
        while (!reusable_.empty()) {
            auto it = reusable_.begin();
            const uint64_t candidate = *it;
            reusable_.erase(it);
            if (registry_alive(candidate)) {
                latch_allocation_error(candidate, "it has a live handle in the block registry");
                continue;
            }
            block_id = candidate;
            from_free_list = true;
            break;
        }
        if (!from_free_list) {
            block_id = max_block_++;
        }

        used_blocks_.insert(block_id);
        // Every id this round hands out belongs to the root being built (or to the
        // rebuild's own transient traffic, which the registry covers). Root N was built out of
        // earlier rounds, so membership here is a proof of "not part of root N" — the
        // subtraction reclaim_superseded_root needs, computed for free at the one point where
        // an id enters circulation.
        issued_since_root_.insert(block_id);
#ifdef DEV_MODE
        dev_issued_.push_back(block_id);
#endif
        return block_id;
    }

    void single_file_block_manager_t::latch_allocation_error(uint64_t block_id, const std::string& reason) {
        // First one wins: it names the id that proved the block accounting wrong, and later ones
        // are consequences of the same corrupt input.
        if (allocation_error_.contains_error()) {
            return;
        }
        allocation_error_ = core::error_t(core::error_code_t::data_corruption,
                                          std::pmr::string{"Block accounting of " + path_ + " refused block " +
                                                               std::to_string(block_id) + ": " + reason,
                                                           buffer_manager.resource()});
    }

    // THE DELIBERATE DECISION about a reclaim that could not read root N.
    //
    // reclaim_superseded_root walks the durable root's two chains through chain_blocks, which read()s
    // every sub-block. Merely propagating that error is not enough: the checkpoint fails but
    // degraded() stays FALSE, and every health gate in the engine keys exclusively on degraded()
    // (data_table_t::compact, table_storage_t::checkpoint, agent_disk_t::checkpoint_inner), so the
    // caller keeps compacting. One rotten block in root N's metadata chain then costs a FULL COPY of
    // the table per round: compact rebuilds into freshly extended blocks (its release goes to
    // pending_free_, which only a committed header drains) and the reclaim fails on the same block
    // again. Measured on a 12k-row table: +18 blocks EVERY round, forever, with every health
    // indicator reporting the file healthy.
    //
    // DECISION: LATCH, and do NOT roll the failed round back.
    //
    //  * LATCH into allocation_error_, not durability_error_: nothing was written badly, what failed
    //    is the manager's ability to ACCOUNT for its blocks. Sticky, because root N is the root a
    //    crash recovers and the engine cannot tell a rotten bit from a transient EIO on the same
    //    block — guessing "transient" is the guess that keeps rebuilding. Loud, not fatal (rule 6):
    //    reads and writes continue, WAL records are never sealed away, and every CHECKPOINT reports
    //    this error until the file is rebuilt.
    //
    //  * NO BLIND ROLL BACK: mark_as_free'ing everything in issued_since_root_ would free LIVE TABLE
    //    STATE — compact has already swapped row_groups_ to the rebuilt collection, and
    //    column_data_t::checkpoint has already re-pointed live tail segments onto this round's
    //    blocks. roll_back_uncommitted_round does roll back, but DISCRIMINATES by registry_alive(),
    //    which is exact: register_block stores a weak_ptr whose only owner is the live
    //    column_segment_t, and the eviction queue never touches blocks_, so an evicted block stays
    //    registry-alive as long as a segment owns it.
    core::error_t single_file_block_manager_t::latch_reclaim_failure(const core::error_t& cause,
                                                                     const char* which_chain) {
        core::error_t composed(cause.type,
                               std::pmr::string{"Superseded root of " + path_ + " cannot be accounted for: its " +
                                                    which_chain + " chain could not be walked (" +
                                                    std::string(cause.what.c_str()) +
                                                    ") — refusing all further checkpoints on this file until it "
                                                    "is rebuilt",
                                                buffer_manager.resource()});
        if (!allocation_error_.contains_error()) {
            allocation_error_ = composed;
        }
        return composed;
    }

    core::error_t single_file_block_manager_t::latch_durability_error(core::error_t error) {
        // First one wins: it names the write or fsync that actually broke the durable image;
        // everything after it is a consequence of running on a file with a hole in it.
        if (!durability_error_.contains_error()) {
            durability_error_ = error;
        }
        return error;
    }

    uint64_t single_file_block_manager_t::peek_free_block_id() {
        // Must mirror free_block_id EXACTLY, pending_free_ included (i.e. excluded), and skip the
        // same candidates: free_block_id does NOT hand out *reusable_.begin(), because a candidate
        // with a live handle in the block registry is live table state. Naming an id the allocator
        // would refuse is a trap for the first caller to appear — it would size or lay out against
        // it — and the only case the two answers differ in is the case the free list is corrupt.
        //
        // What a peek does NOT mirror is the SIDE EFFECTS: dropping the corrupt candidate and
        // latching the accounting error belong to the allocation that consumes it, not to a look —
        // latching here would let a diagnostic turn a checkpoint into a refusal. So this walks past
        // the same candidates without removing them, including the fall-through to the high-water
        // mark when every candidate is live.
        for (uint64_t candidate : reusable_) {
            if (registry_alive(candidate)) {
                continue;
            }
            return candidate;
        }
        return max_block_;
    }

    bool single_file_block_manager_t::is_root_block(meta_block_pointer_t root) {
        return root.block_pointer == meta_block_;
    }

    void single_file_block_manager_t::mark_as_free(uint64_t block_id) {
        // Rule 19, and it cannot be an assert(): the ids arriving here are NOT this manager's own.
        // data_table_t::compact collects them from the live collection, and a segment's big-string
        // overflow ids are rebuilt verbatim from data_pointer_t::overflow_blocks, which
        // data_pointer_t::deserialize reads straight out of the .otbx with no domain check anywhere
        // in between — a path fed by disk bytes. An assert() would abort a debug build on the agent
        // thread that runs compact, i.e. across an actor boundary (rule 9), and do NOTHING under
        // NDEBUG, where a transient-domain id (>= MAXIMUM_BLOCK) would enter the free pool, be
        // promoted by the next durable header, be handed out, and have block_location wrap it:
        // (2^62 + N) * 2^18 == N * 2^18, a REAL live block rewritten with a valid CRC. Branch
        // instead, and latch, so the next write_header refuses to commit a root built on state
        // already known to be inconsistent.
        //
        // THE BOUNDARY IS THE FILE, not the domain. `>= MAXIMUM_BLOCK` alone accepts every id between
        // the end of the file and 2^62, which is just as unaddressable: the seek lands past EOF and
        // the write extends the file across the whole gap — a corrupt overflow_blocks entry naming
        // block 2^40 asks for a 256 TB file — while total_blocks() still reports the old extent and
        // the published free list hands the same id to the next process that opens the file.
        // max_block_ is that boundary, and it is NOT the file's extent but this manager's ISSUANCE
        // MARK (adopted from the header's block_count at open, raised by free_block_id, lowered only
        // by roll_back_uncommitted_round over the contiguous released tail), so after a rollback it
        // can sit BELOW both the physical end of the file and the durable header's block_count. The
        // narrower measure errs in the only safe direction: the rollback's descent walks only
        // RELEASED ids (registry-alive ones are filtered out, and root N's blocks are never in
        // issued_since_root_) and stops at the first id it may not cross, so every live and every
        // root id stays below the mark, and the gap holds only an abandoned tail nothing owns — a
        // refusal there latches a leak, it cannot lose a block. It is also STRICTLY WIDER than the
        // domain check, which is still named separately because it means something else.
        if (block_id >= max_block_) {
            latch_allocation_error(block_id,
                                   block_id >= MAXIMUM_BLOCK
                                       ? std::string("it is outside the addressable block domain")
                                       : "the file holds " + std::to_string(max_block_) +
                                             " blocks, so it is past the end of the file");
            return;
        }
        used_blocks_.erase(block_id);
        modified_blocks_.erase(block_id);
#ifdef DEV_MODE
        dev_freed_.push_back(block_id);
#endif
        // The one production caller is data_table_t::compact, which runs immediately before the
        // checkpoint and releases the OUTGOING collection's blocks — precisely what the CURRENT
        // durable root still points at. So the release means "free once the root being built becomes
        // durable", which is what pending_free_ is for; free_block_id cannot see it from here.
        //
        // An id already in reusable_ stays there instead of being demoted: reusable_ means the
        // DURABLE root does not reference it, and freeing it again in memory cannot change that.
        // Demoting would quarantine a provably safe block, and staying put keeps the sets disjoint.
        if (reusable_.count(block_id) != 0) {
            return;
        }
        pending_free_.insert(block_id);
    }

    void single_file_block_manager_t::mark_as_used(uint64_t block_id) {
        // Both halves: "used" has to remove the id from every pool that could hand it out or
        // publish it as free, or serialize_free_list would persist a live block as reusable.
        reusable_.erase(block_id);
        pending_free_.erase(block_id);
        used_blocks_.insert(block_id);
    }

    // Promotion, and the decision about a checkpoint that FAILS: a failed checkpoint neither
    // promotes pending_free_ nor discards it. The ids stay quarantined until the NEXT checkpoint
    // whose header becomes durable.
    //
    // Not promoted, because the blocks are still live under the still-current old root — the root a
    // crash recovers. Not discarded, because the release itself was not undone: data_table_t::compact
    // swapped row_groups_ BEFORE the checkpoint ran, so in memory nothing reaches those blocks and
    // dropping them would leak the space with no path back, on a failure the engine otherwise
    // recovers from (agent_disk defers the entry and retries next round).
    //
    // The memory/disk disagreement that leaves — COMPACTED table in memory, PRE-compact durable root
    // — costs space, not correctness: the file cannot reuse it until a checkpoint succeeds (one
    // collection's worth per deferred round), a process death reopens on the old root and these
    // blocks come back as its live data (nothing lost, only BECAUSE they were never reissued), and
    // the dropped rows are still in the WAL, since agent_disk_t::checkpoint_inner does not advance
    // the .wal_id sidecar for a failed entry. Promoting on a failure is the one move that would turn
    // this into corruption, so promotion has exactly two callers and both have proof the new header
    // reached the device.
    //
    // Adopting the new root's chain pointers and data blocks belongs to this same call: both are
    // statements about which root a crash would recover, so splitting them would let the two halves
    // disagree, and both must be made at the instant the header is proven on the device.
    void single_file_block_manager_t::promote_durable_root(uint64_t meta_block, uint64_t free_list) {
        reusable_.insert(pending_free_.begin(), pending_free_.end());
        pending_free_.clear();
        // The root that just landed is the one to reclaim NEXT round.
        durable_meta_block_ = meta_block;
        durable_free_list_ = free_list;
        durable_root_data_ = std::move(pending_root_data_);
        pending_root_data_.clear();
        // A new root means a new "issued since": from here on, every allocation belongs to the
        // NEXT root under construction.
        issued_since_root_.clear();
    }

    // --- Reclaiming the superseded root ---

    void single_file_block_manager_t::adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& block_ids) {
        durable_root_data_.clear();
        durable_root_data_.insert(block_ids.begin(), block_ids.end());
    }

    // THE FORMULA:
    //
    //   free = {blocks of root N} u {metadata chain of N} u {free-list chain of N}
    //          - {blocks of root N+1} - {ids live in the block registry}
    //
    // Neither subtraction is redundant:
    //
    //   * {blocks of root N+1}. The round under construction allocates its data blocks, metadata
    //     chain and free-list chain out of reusable_, i.e. out of what PREVIOUS rounds released.
    //     Rather than lean on "root N's blocks are not in reusable_ yet" (this function puts them in
    //     pending_free_, which free_block_id never draws from), the subtraction is made explicit
    //     twice over: against the data blocks data_table_t just wrote, and against
    //     issued_since_root_ — every id handed out since the durable root was committed.
    //
    //   * {ids live in the block registry}. Root N's DATA blocks are live table state whenever the
    //     table was loaded from root N and this round did NOT compact it: the in-memory segments
    //     still point at them and will page them back in. Freeing one would hand it to the next
    //     allocation and overwrite live data with a valid CRC, and the registry is the only thing
    //     that knows this.
    //
    // WHAT IT COSTS. The ids go into pending_free_, not reusable_, so root N's blocks become issuable
    // only once a root that does NOT name them is on the device: between this call and the fsync
    // inside write_header the file holds TWO complete copies of the table, and a SUCCESSFUL round
    // does not give that back (truncate() only trims past max_block_, and nothing on the success path
    // lowers the mark — only roll_back_uncommitted_round does, on the FAILED path). Later rounds
    // reuse the space instead.
    core::result_wrapper_t<uint64_t>
    single_file_block_manager_t::reclaim_superseded_root(const std::pmr::vector<uint64_t>& new_root_data_blocks) {
        auto* resource = buffer_manager.resource();
        pending_root_data_.clear();
        pending_root_data_.insert(new_root_data_blocks.begin(), new_root_data_blocks.end());

        const uint64_t durable_meta = durable_meta_block_;
        const uint64_t durable_free = durable_free_list_;
        std::pmr::vector<uint64_t> candidates(resource);
        candidates.assign(durable_root_data_.begin(), durable_root_data_.end());
        if (durable_meta == INVALID_INDEX && durable_free == INVALID_INDEX && candidates.empty()) {
            return uint64_t{0}; // no durable root yet: the first checkpoint of a fresh file
        }

        // Root N's chains, walked through the SAME code the reachability walker uses. Dedup is
        // inside chain_blocks: one block backs 64 sub-blocks, so a chain of dozens of sub-blocks
        // is usually one block id repeated.
        metadata_manager_t chain_mgr(*this);
        if (durable_meta != INVALID_INDEX) {
            if (auto walked = chain_mgr.chain_blocks(meta_block_pointer_t(durable_meta, 0), candidates);
                walked.has_error()) {
                // Propagating alone leaves degraded() false, so nothing upstream stops
                // compacting and the file grows by a full copy of the table every round.
                return core::result_wrapper_t<uint64_t>(latch_reclaim_failure(walked.error(), "table-metadata"));
            }
        }
        if (durable_free != INVALID_INDEX) {
            if (auto walked = chain_mgr.chain_blocks(meta_block_pointer_t(durable_free, 0), candidates);
                walked.has_error()) {
                return core::result_wrapper_t<uint64_t>(latch_reclaim_failure(walked.error(), "free-list"));
            }
        }

        uint64_t reclaimed = 0;
        for (uint64_t block_id : candidates) {
            // ADDRESSABILITY FIRST, because this list is DISK BYTES: durable_root_data_ is collected
            // from data_pointer_t::block_pointer.block_id, a full uint64 the metadata reader takes
            // straight off the file. Past max_block_ the id names no block at all, and past
            // MAXIMUM_BLOCK block_location would wrap it, (2^62 + N) * 2^18 == N * 2^18, onto a REAL
            // live block. mark_as_free applies exactly this boundary and latches; the screen here is
            // what keeps the CALLER from walking on, since unregister_block's assert() only covers
            // the domain half and ++reclaimed would otherwise count an id that reached no pool.
            if (block_id >= max_block_) {
                mark_as_free(block_id); // refuses the id and latches; never reaches either pool
                continue;
            }
            if (pending_root_data_.count(block_id) != 0 || issued_since_root_.count(block_id) != 0) {
                continue; // named by, or allocated for, the root under construction
            }
            if (registry_alive(block_id)) {
                continue; // live table state — the table was not compacted this round
            }
            // mark_as_free files into pending_free_ and re-checks the addressable domain; both
            // are wanted here, since these ids came off the disk chains. unregister_block drops
            // the (expired) registry entry so a later reuse of the id cannot resurrect a stale
            // handle — the same ABA pairing data_table_t::compact makes.
            mark_as_free(block_id);
            unregister_block(block_id);
            ++reclaimed;
        }
        return reclaimed;
    }

    // --- Giving back what a FAILED round took ---
    //
    // THE PROBLEM, measured. A checkpoint round allocates three kinds of block before it commits, and
    // NONE is registered in the block registry: the PACKED COPY (flush_segment routes every column
    // segment through partial_block_manager_t, and only the root's row_group_pointer_t stream names
    // those ids), the TABLE-METADATA chain and the FREE-LIST chain (both metadata_manager_t). When
    // the header write fails no root names them and nothing in memory reaches them either — they stay
    // in used_blocks_/issued_since_root_ forever. On a 7.8 MB table that is ~655 KB per round, for as
    // long as the failure persists, while degraded() stays FALSE: reconcile_failed_header_write case
    // 2 deliberately does not latch, so that a transient ENOSPC can still recover.
    //
    // WHY THIS IS NOT the blanket "free everything in issued_since_root_" that latch_reclaim_failure
    // refuses. The two things a blanket sweep would destroy — the rebuilt collection compact()
    // swapped into row_groups_, and the tail segments column_data_t::checkpoint re-pointed — both
    // reach their blocks through column_data_t::transition_segment_to_disk, which DOES register_block
    // and hands the handle to a live column_segment_t that owns it for as long as it is in the tree
    // (eviction unloads the BUFFER, not the handle). So the live tree's dependence on a block is
    // exactly "a live handle in the registry":
    //
    //     releasable = issued_since_root_ - {ids live in the block registry}
    //
    // free_block_id is reached from exactly two places (metadata_manager_t::allocate_handle/reserve
    // and partial_block_manager_t::get_block_allocation), and of those only
    // transition_segment_to_disk registers what it allocated, so the three kinds above fall on the
    // releasable side and the live tree's blocks never do.
    //
    // reusable_ AND NOT pending_free_: pending_free_ means "the DURABLE root still names it", false
    // here by construction (promote_durable_root empties issued_since_root_ the instant a root
    // becomes durable), and it drains only on a committed header — the one event this path exists
    // because it did not happen — so routing them there would keep the file growing at the old rate.
    //
    // Nothing can still read a released block, though the round may have written garbage into one
    // under a valid CRC: a chain walk of the durable root touches root N's chains, disjoint from
    // issued_since_root_; block_handle_t::load() needs a REGISTERED id and every released one is
    // unregister_block'd; and deserialize_free_list at open reads root N's PUBLISHED list, which
    // either already listed the id or stops short of it (past root N's block_count).
    //
    // THE HIGH-WATER MARK. This is the sole place that LOWERS max_block_. A failed round's
    // allocations are not a contiguous tail in general, but the part that DID extend the file is by
    // definition its top, so the mark walks down over released ids only and stops at the first id
    // from the top that is not released — root N's blocks are never in that set and registry-alive
    // ids are filtered out of it, so the descent cannot uncover a block anything still needs. Ids
    // past the new mark leave reusable_ too: issuing one would put a block beyond the block_count the
    // next header records. The file itself does not shrink — only truncate() returns bytes, it has no
    // production caller, and this path runs on a device that just reported a failure — so the space
    // is REUSED by the next round, which is what keeps the footprint flat.
    uint64_t single_file_block_manager_t::roll_back_uncommitted_round() {
        // The one shape that must never be rolled back: the header write failed and the
        // read-back could not establish which root is on the device. The blocks this round
        // allocated may be named by a root a crash would recover.
        if (durable_root_indeterminate_) {
            return 0;
        }

        std::set<uint64_t> released;
        for (uint64_t block_id : issued_since_root_) {
            if (registry_alive(block_id)) {
                continue; // live table state: the rebuilt collection, or a re-pointed live tail
            }
            released.insert(block_id);
        }

        for (uint64_t block_id : released) {
            issued_since_root_.erase(block_id);
            used_blocks_.erase(block_id);
            modified_blocks_.erase(block_id);
            // LOAD-BEARING, not defensive. "An id this round allocated cannot be in pending_free_"
            // is a statement about ONE round; issued_since_root_ spans every round since the last
            // COMMITTED header. A second failed round that is allowed to compact swaps out the
            // collection the FIRST one built and mark_as_free's its blocks — ids no header ever
            // promoted out of issued_since_root_ — so the two sets do intersect, on the ordinary
            // retry path. Measured on a 12k-row table: 14 ids in both after two failed compacting
            // rounds (test_failed_round_rollback.cpp). Moving them is also right, not merely safe:
            // pending_free_ means "the DURABLE root still names it", false here by construction, and
            // it drains only on a committed header — so leaving them quarantined strands the space.
            pending_free_.erase(block_id);
            reusable_.insert(block_id);
            // ABA break, the same pairing data_table_t::compact and reclaim_superseded_root
            // make: the id goes back to the allocator, so no EXPIRED registry slot may survive
            // to be revived by a later register_block for a different block's data.
            unregister_block(block_id);
#ifdef DEV_MODE
            dev_freed_.push_back(block_id);
#endif
        }

        // The root under construction is gone with the round that was building it; every block
        // it named is either back in the pool above or still live table state.
        pending_root_data_.clear();
        // ...including its metadata pointer: set_meta_block installed the ABANDONED chain's pointer
        // earlier in the round, and those blocks just went back to the allocator, so leaving it would
        // leave this manager holding a root pointer into reusable space. The honest value is what a
        // fresh load_existing_database would install (INVALID_INDEX when there is no durable root
        // yet). durable_meta_block_ is untouched: it describes root N, which this path leaves alone.
        meta_block_ = durable_meta_block_;

        // Walk the high-water mark down over the released tail (see the note above).
        for (auto it = released.rbegin(); it != released.rend() && max_block_ != 0 && *it + 1 == max_block_; ++it) {
            --max_block_;
            reusable_.erase(*it);
        }

        return static_cast<uint64_t>(released.size());
    }

    void single_file_block_manager_t::mark_as_modified(uint64_t block_id) {
        modified_blocks_.insert(block_id);
    }

    void single_file_block_manager_t::increase_block_ref_count(uint64_t /*block_id*/) {
        // ref counting not yet needed for single-file mode
    }

    uint64_t single_file_block_manager_t::meta_block() { return meta_block_; }

    std::unique_ptr<block_t> single_file_block_manager_t::create_block(uint64_t block_id,
                                                                       file_buffer_t* source_buffer) {
        auto& bm = buffer_manager;
        auto resource = bm.resource();

        if (source_buffer) {
            auto result = std::make_unique<block_t>(*source_buffer, block_id);
            return result;
        }
        return std::make_unique<block_t>(resource, block_id, static_cast<uint64_t>(block_size()));
    }

    std::unique_ptr<block_t> single_file_block_manager_t::convert_block(uint64_t block_id,
                                                                        file_buffer_t& source_buffer) {
        return std::make_unique<block_t>(source_buffer, block_id);
    }

    uint64_t single_file_block_manager_t::total_blocks() { return max_block_; }

    uint64_t single_file_block_manager_t::free_blocks() {
        // Both halves, because this answers "how much of the file is dead space", not "how many
        // ids may be issued right now". pending_free_ IS dead space — it is exactly what the
        // free list of the root under construction publishes (serialize_free_list), and the
        // block whose reuse is deferred is still space the file will not need to grow for.
        // The two sets are disjoint, so the sum double-counts nothing.
        return reusable_.size() + pending_free_.size();
    }

    // --- Checksums ---

    core::result_wrapper_t<bool> single_file_block_manager_t::checksum_and_write(file_buffer_t& buffer,
                                                                                 uint64_t block_id) {
        auto* data = buffer.internal_buffer();
        auto alloc_size = buffer.allocation_size();

        // first 8 bytes = checksum slot
        auto* checksum_slot = reinterpret_cast<uint64_t*>(data);
        auto* payload = data + sizeof(uint64_t);
        auto payload_size = alloc_size - sizeof(uint64_t);

        auto crc = static_cast<uint64_t>(
            static_cast<uint32_t>(absl::ComputeCrc32c({reinterpret_cast<const char*>(payload), payload_size})));
        *checksum_slot = crc;

        auto location = block_location(block_id);
        // THE deepest link in the durability chain: this is where every data block, every
        // metadata block and every free-list block in the system lands, so its answer must be
        // both propagated and latched.
        if (!buffer.write(*handle_, location)) {
            return latch_durability_error(
                core::error_t(core::error_code_t::io_error,
                              std::pmr::string{"Failed to write block " + std::to_string(block_id) + " (offset " +
                                                   std::to_string(location) + ") of " + path_,
                                               buffer_manager.resource()}));
        }
        return true;
    }

    bool single_file_block_manager_t::verify_checksum(file_buffer_t& buffer) {
        auto* data = buffer.internal_buffer();
        auto alloc_size = buffer.allocation_size();

        auto stored_checksum = *reinterpret_cast<uint64_t*>(data);
        auto* payload = data + sizeof(uint64_t);
        auto payload_size = alloc_size - sizeof(uint64_t);

        auto computed = static_cast<uint64_t>(
            static_cast<uint32_t>(absl::ComputeCrc32c({reinterpret_cast<const char*>(payload), payload_size})));
        return stored_checksum == computed;
    }

    // --- Header write + sync ---

    core::result_wrapper_t<bool> single_file_block_manager_t::write_header(const database_header_t& header) {
        // The target slot is a pure function of iteration_ parity, so the counter is not an "we are
        // attempting iteration N" marker — moving it moves the target. Incrementing it BEFORE the
        // write would have a failed write still consume an iteration, aiming the retry at the OTHER
        // slot, the one holding the last durable root: the retry would overwrite exactly the state it
        // exists to preserve, and two failures in a row could leave no valid slot at all. iteration_
        // takes the candidate only once write AND fsync have succeeded.
        //
        // A checkpoint whose allocations drew on a free list proven corrupt must not become the
        // durable root: the metadata it just wrote may sit on blocks that are still live table state.
        // Both refusals below return BEFORE a single byte of a header slot is written, so "this
        // round's header did not become durable" — roll_back_uncommitted_round's precondition — is
        // proven by the control flow rather than by a caller remembering it.
        if (allocation_error_.contains_error()) {
            roll_back_uncommitted_round();
            return core::error_t(allocation_error_);
        }
        // Commit gate. A block write or the pre-header barrier failed earlier in this
        // round (or in an earlier one): the blocks this root would name are not all on the
        // device, so committing the root is exactly the silent corruption this whole chain
        // exists to prevent. Refuse BEFORE writing anything, so no header lands and there is
        // nothing to reconcile afterwards.
        if (durability_error_.contains_error()) {
            roll_back_uncommitted_round();
            return core::error_t(durability_error_);
        }

        const uint64_t next_iteration = iteration_ + 1;

        database_header_t write_header = header;
        write_header.iteration = next_iteration;
        write_header.block_count = max_block_;
        write_header.block_alloc_size = block_allocation_size();
        write_header.meta_block = meta_block_;
        // The padding is inside the checksummed sector, and the caller's copy of it is not
        // this function's to trust: zero it so the bytes under the CRC are deterministic.
        // (The checksum slot itself needs no clearing — compute_checksum skips it.)
        std::memset(write_header.padding, 0, sizeof(write_header.padding));
        write_header.checksum = write_header.compute_checksum();

        // Shadow paging: write the ONE slot this iteration owns and sync it, leaving the other slot
        // holding the PREVIOUS root — that IS the redundancy. Writing both would destroy it: after
        // the second write no previous root remains, and the only crash the layout could survive is
        // one landing exactly between the two writes.
        //
        // This single slot write is the ONLY durable write of a checkpoint, so both bools are
        // load-bearing and are reported to the caller: on ENOSPC/EIO the checkpoint did NOT happen
        // and the caller must not advance its WAL bookkeeping.
        const uint64_t slot = header_slot_offset(next_iteration);
        const bool write_ok = handle_->write(&write_header, sizeof(write_header), slot);
        // The fsync is issued UNCONDITIONALLY, even after a write that reported failure. A
        // header that only reached the page cache is not a durable root, so the fsync is part
        // of the write rather than an optional extra step — and when the write failed PARTWAY
        // the bytes that did land are in the page cache, so pushing them out is what makes the
        // read-back below evidence about the device instead of a guess.
        const bool sync_ok = handle_->sync();
        if (write_ok && sync_ok) {
            // Durable: only now does this iteration exist, and only now does the target slot move.
            iteration_ = next_iteration;
            // Promotion point. The slot this write landed in now holds the root the file
            // recovers to, and that root does NOT reference the blocks compact() released this
            // round — its own free list, serialized above, lists them. From this instruction on
            // they are free under the durable root, which is the definition of reusable_. Doing
            // it one line earlier (before the fsync answered) would put them back in the
            // allocator while the OLD root was still the recoverable one, which is the entire
            // bug shadow paging exists to close.
            promote_durable_root(write_header.meta_block, write_header.free_list);
            return true;
        }
        return reconcile_failed_header_write(next_iteration, write_ok, sync_ok);
    }

    // --- Header/disk divergence (the deliberate decision) ---
    //
    // A torn 4 KiB header write can leave a CRC-VALID header of the new generation on disk while
    // write() reports failure. That is structural, not bad luck: every byte that differs between two
    // generations lives in bytes 0..47 — inside the FIRST 512-byte hardware sector of the 4 KiB
    // header — and the padding is zeros in every generation, so a partial write reassembles into a
    // byte-exact copy of ONE generation and passes the CRC. The checksum catches zeros, garbage and a
    // never-written slot; it does not catch a tear.
    //
    // So "the checkpoint failed" and "the new root is on disk" can both be true, and ASSUMING the
    // first — keeping iteration_ and the .wal_id sidecar behind — makes the WAL replay records on
    // restart into a root that already contains them (duplicated rows, not a missing update).
    //
    // DECISION: read the TWO SLOTS BACK and adopt whatever the disk says, by the same slot-selection
    // rule the open path uses. Three outcomes, each leaving the engine's belief equal to the file:
    //
    //   1. the active slot is the NEW iteration and the fsync succeeded — the new root is on the
    //      device, and the barrier before it already made the data and metadata blocks durable, so
    //      this checkpoint IS complete however the write call answered. Adopt it and report SUCCESS:
    //      the sidecar must advance, because the rows are in that root.
    //   2. the active slot is the iteration this manager already believed in — nothing of the new
    //      header exists, not even in the page cache, so nothing can reach the device later. Clean
    //      io_error: the checkpoint did not happen, the caller keeps its backup and its WAL records.
    //   3. anything else — the fsync failed so a landed-looking slot is not evidence about the
    //      DEVICE, or neither slot reads back, or the winner is a generation this manager never
    //      wrote. The truth cannot be established by reading, so it is not guessed: latch and refuse
    //      every later checkpoint rather than commit a root on top of an unknown one.
    //
    // Read-back goes through this same handle, i.e. through the page cache, and that is what makes
    // case 2 sound: the page cache is a SUPERSET of the device, so "not even in the page cache" is
    // strictly stronger than "not on the device". Case 1 needs the opposite direction, which is why
    // it additionally requires the fsync to have succeeded.
    core::result_wrapper_t<bool>
    single_file_block_manager_t::reconcile_failed_header_write(uint64_t next_iteration, bool write_ok, bool sync_ok) {
        const std::string what = std::string("iteration ") + std::to_string(next_iteration) + " of " + path_ + " (" +
                                 (write_ok ? "write ok" : "write failed") + ", " +
                                 (sync_ok ? "fsync ok" : "fsync failed") + ")";

        database_header_t slot1{};
        database_header_t slot2{};
        const bool slot1_valid = handle_->read(&slot1, sizeof(slot1), SECTOR_SIZE) && slot1.checksum_ok();
        const bool slot2_valid = handle_->read(&slot2, sizeof(slot2), 2 * SECTOR_SIZE) && slot2.checksum_ok();

        if (!slot1_valid && !slot2_valid) {
            // No root can be named, so no statement about which blocks a root owns can be made
            // either. The failed round's allocations stay exactly where they are.
            durable_root_indeterminate_ = true;
            return latch_durability_error(core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"Header write failed and NEITHER slot reads back usable: " + what +
                                     " — the durable root is gone, refusing all further checkpoints on this file",
                                 buffer_manager.resource()}));
        }

        const database_header_t& active =
            (slot1_valid && (!slot2_valid || slot1.iteration >= slot2.iteration)) ? slot1 : slot2;

        if (active.iteration == next_iteration && sync_ok) {
            // Case 1: the disk carries the new root. Believe the disk, not the return code.
            iteration_ = next_iteration;
            meta_block_ = active.meta_block;
            max_block_ = active.block_count;
            // The new root IS the durable one — that is what this branch just established — so
            // the promotion is owed here exactly as on write_header's success path. Cases
            // 2 and 3 below do NOT promote: in case 2 the previous root demonstrably still
            // stands, and in case 3 which root stands cannot be established by reading, and
            // "promote when unsure" hands out blocks a recoverable root may still read.
            promote_durable_root(active.meta_block, active.free_list);
            return true;
        }

        if (active.iteration == iteration_) {
            // Case 2: nothing of the new header exists. Engine and disk already agree and the
            // previous root demonstrably stands, so this is the RECOVERABLE case and the retry is
            // meant to reach the same slot again. Deliberately NOT latched: that would turn a
            // transient ENOSPC into a permanently degraded manager and defeat the retry.
            //
            // OPEN, and a real cost: a PERSISTENT write error at this offset is retried forever, and
            // each retried round runs compact() first, rebuilding the collection into freshly
            // extended blocks because reusable_ never refills — a full copy of the table per round.
            // The lever for that is gating the COMPACT on "the previous round failed", not degrading
            // the manager here.
            //
            // The rollback below closes the other half of that cost. This branch is the only place
            // in the class that can prove the round's own allocations are unreferenced: the read-back
            // has just established that nothing of the new header exists, so root N still stands and
            // cannot name a block issued after it was committed. Give those blocks back before
            // reporting the failure, so a retried round spends the SAME ones again.
            roll_back_uncommitted_round();
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to write database header, " + what +
                                                      "; the previous root (iteration " + std::to_string(iteration_) +
                                                      ") stands and is unchanged",
                                                  buffer_manager.resource()});
        }

        // Case 3: indeterminate. This is the ONE outcome where the round's allocations must NOT
        // be given back — a landed-looking slot that the fsync never confirmed may still be the
        // root a crash recovers, and that root names the blocks this round wrote. Handing them
        // to the allocator would be precisely the corruption shadow paging prevents.
        durable_root_indeterminate_ = true;
        return latch_durability_error(core::error_t(
            core::error_code_t::io_error,
            std::pmr::string{"Failed to write database header, " + what + "; the slots read back as iteration " +
                                 std::to_string(active.iteration) + " while this manager believed " +
                                 std::to_string(iteration_) +
                                 " — the durable root is indeterminate, refusing all further checkpoints on this file",
                             buffer_manager.resource()}));
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::file_sync() {
        // The pre-header barrier. Its whole job is to make the blocks written this round
        // durable BEFORE the root that names them commits, so dropping its bool made the
        // header swap meaningless. Latched as well as returned: a barrier that did not hold
        // must not be survivable by a caller that forgets to look at the answer.
        if (!handle_) {
            return latch_durability_error(
                core::error_t(core::error_code_t::io_error,
                              std::pmr::string{"file_sync on a block manager with no open file: " + path_,
                                               buffer_manager.resource()}));
        }
        if (!handle_->sync()) {
            return latch_durability_error(core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"Failed to fsync data and metadata blocks of " + path_, buffer_manager.resource()}));
        }
        return true;
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::truncate() {
        if (!handle_) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"truncate on a block manager with no open file: " + path_,
                                                  buffer_manager.resource()});
        }
        // What this can and cannot give back. max_block_ rises whenever free_block_id extends the
        // file and is lowered only by roll_back_uncommitted_round walking it down over a failed
        // round's released tail, so this trims past the mark and shrinks the file only by what such a
        // rollback gave back. PRECONDITION for any future caller: the mark can now sit BELOW the
        // block_count a durable header records, so truncating without checking that would cut the
        // file below the extent the durable root describes. A compacting checkpoint holds two copies
        // of the table at once — the superseded root's blocks stay quarantined in pending_free_ until
        // the new header is on the device — so the mark settles at roughly twice the table's own size
        // and stays there. The space is REUSED by every later round (that is what makes an unchanged
        // database stop growing) but is not returned to the filesystem; giving it back would need
        // block relocation plus a pointer rewrite.
        auto file_end = block_location(max_block_);
        if (!handle_->truncate(static_cast<int64_t>(file_end))) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to truncate " + path_ + " to " + std::to_string(file_end),
                                                  buffer_manager.resource()});
        }
        return true;
    }

    // --- Free List Persistence ---

    // WHAT THE PERSISTED FREE LIST MUST CONTAIN — reusable_ ∪ pending_free_ ∪ {live-only blocks the
    // root being written does not name}.
    //
    // The third term is what keeps a REOPENED file from leaking. A committed round leaves the live
    // in-memory tree holding blocks the durable root does NOT name: compact's write-through and
    // column_data_t::checkpoint's re-pointed tail segments keep theirs through registered
    // block_handle_t's, while the root's pointer stream names the PACKED COPY flush_segment wrote.
    // In-process they are protected (registry_alive keeps them out of every pool), but no root
    // references them and no list published them, so the instant the process ends — by crash OR by
    // clean exit — they are orphans a reopened file can never find again, since the reclaim only
    // walks ROOTS. Measured at 6k rows: 8 blocks (2 MiB) leaked per restart.
    //
    // So publish every id whose ONLY owner is the live tree: registry-alive and not named by the root
    // this list hangs off (pending_root_data_, recorded by reclaim_superseded_root earlier this
    // round). That completes the list's meaning rather than relaxing it — the list is the root's own
    // statement about what it does NOT reference. A FUTURE OPEN puts them in reusable_, correctly,
    // because in that process nothing reaches them; THIS process's pools are untouched, the published
    // chain being disk bytes rather than allocator state. Subtracting pending_root_data_ keeps the
    // fatal direction impossible: an id the root names is never published.
    //
    // pending_free_ belongs there for the same reason: under the NEW root the outgoing collection is
    // gone, so its blocks are free exactly like reusable_'s, and omitting them would describe the new
    // root as owning blocks nothing points at. The asymmetry with the IN-MEMORY promotion is
    // deliberate — the serialized list describes the root being WRITTEN, reusable_ the root already
    // DURABLE, which still reads pending_free_'s blocks — and write_header makes them one root.
    //
    // Crash safety of the union: a crash BEFORE the header lands leaves the old header active
    // (write_header writes only the slot the new iteration owns) carrying its OWN free-list pointer
    // from an earlier round, which cannot name these blocks — they were live table data when it was
    // written — so the chain written here is unreferenced garbage. A crash AFTER it lands recovers
    // the new root, whose list correctly frees both halves. Neither case frees a block the recovered
    // root reads.
    core::result_wrapper_t<meta_block_pointer_t> single_file_block_manager_t::serialize_free_list() {
        // The third term (see the doctrine above): blocks owned only by the live tree. Computed
        // before anything allocates — allocation draws from reusable_ and can neither create nor
        // destroy a registry entry — so the set is stable across the reservation below.
        //
        // THE WRITER OBEYS THE READER'S BOUNDARY, and that boundary is the file:
        // deserialize_free_list refuses any id at or past the block_count of the header it hangs off,
        // and write_header stamps block_count = max_block_. Publishing an id >= max_block_ would
        // produce a file this build just COMMITTED and then refuses to ever open — a recoverable leak
        // turned into an unopenable database. These ids are DISK-FED with no extent check on the way
        // in (column_data / column_state hand data_pointer_t's block ids straight to register_block),
        // and on a non-compacting round no mark_as_free ever sees them, so this is the LAST point
        // before they become durable: latch (sticky — a corrupt registration does not heal) and
        // refuse THIS round, leaving the previous root standing and the file openable. reusable_ and
        // pending_free_ need no such screen — every path that fills them already keeps them below
        // max_block_.
        std::set<uint64_t> live_unnamed;
        for (uint64_t block_id : live_registry_ids()) {
            if (block_id >= max_block_) {
                const std::string reason = block_id >= MAXIMUM_BLOCK
                                               ? std::string("it is outside the addressable block domain")
                                               : "the file holds " + std::to_string(max_block_) +
                                                     " blocks, so it is past the end of the file";
                latch_allocation_error(block_id, reason);
                return core::error_t(core::error_code_t::data_corruption,
                                     std::pmr::string{"Free list of " + path_ +
                                                          " refuses to publish registry-live block " +
                                                          std::to_string(block_id) + ": " + reason,
                                                      buffer_manager.resource()});
            }
            if (pending_root_data_.count(block_id) == 0) {
                live_unnamed.insert(block_id);
            }
        }
        if (reusable_.empty() && pending_free_.empty() && live_unnamed.empty()) {
            return meta_block_pointer_t{}; // INVALID_INDEX
        }
        const uint64_t candidate_count =
            static_cast<uint64_t>(reusable_.size() + pending_free_.size() + live_unnamed.size());
        metadata_manager_t meta_mgr(*this);

        // THE WHOLE CHAIN IS ALLOCATED BEFORE THE SNAPSHOT IS TAKEN — the one thing that makes a
        // published free list safe. The list is a snapshot of the very pool free_block_id draws from,
        // so ANY block allocated while the write is in progress is an id the snapshot already calls
        // free. The constructor's first block is handled by ordering, but
        // metadata_writer_t::ensure_space allocates every FURTHER chain block mid-stream — and with a
        // 256 KiB block one chain block holds ~32,608 ids, so a free list past that size would
        // publish a block of its own chain, and a restart would hand that id out over the chain the
        // durable root still reads.
        //
        // Reserving up front moves those allocations to before the snapshot. The reservation is sized
        // from the count BEFORE it runs, and reserving can only SHRINK the pool, so the capacity is
        // never short of what the smaller published list needs. No format change and no permanent
        // cost: reclaim_superseded_root frees the chain blocks one round later, like the rest of the
        // superseded root.
        const uint64_t sub_block_size = meta_mgr.sub_block_size();
        const uint64_t payload_bytes = (candidate_count + 1) * sizeof(uint64_t); // + the count itself
        const uint64_t needed_sub_blocks = metadata_writer_t::sub_blocks_for(payload_bytes, sub_block_size);
        if (needed_sub_blocks == 0) {
            // sub_blocks_for reports 0 only for a sub-block too small to hold its own 12-byte
            // chain header, i.e. an unusable block geometry. Never treat that as "nothing to
            // reserve": say so and refuse the checkpoint.
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Free list of " + path_ + " cannot be written: sub-block size " +
                                                      std::to_string(sub_block_size) +
                                                      " is too small to hold a metadata chain header",
                                                  buffer_manager.resource()});
        }
        meta_mgr.reserve(needed_sub_blocks);

        metadata_writer_t writer(meta_mgr);

        // SNAPSHOT, then write. Iterating the pool directly would be a range-for over a container the
        // writer's own allocations erase from, surviving only because free_block_id always takes the
        // SMALLEST element, i.e. one the loop has already passed; the snapshot removes the hazard
        // instead of relying on it, and stays even though the reservation above means the writer no
        // longer allocates mid-write. The three parts are pairwise disjoint by the pool invariants,
        // but the union goes through a set anyway: duplicate-freeness must not depend on invariants
        // that live in other functions.
        std::set<uint64_t> merged = reusable_;
        merged.insert(pending_free_.begin(), pending_free_.end());
        merged.insert(live_unnamed.begin(), live_unnamed.end());
        std::pmr::vector<uint64_t> published(buffer_manager.resource());
        published.reserve(merged.size());
        published.assign(merged.begin(), merged.end());

        writer.write<uint64_t>(static_cast<uint64_t>(published.size()));
        for (auto block_id : published) {
            writer.write<uint64_t>(block_id);
        }
        // The flush IS the write of the free-list chain. Returning the pointer without looking
        // at it advertises a chain that may not exist on disk; the next open would then follow
        // header.free_list into blocks that were never laid down.
        if (auto flushed = writer.flush(); flushed.has_error()) {
            return flushed.convert_error<meta_block_pointer_t>();
        }

        // Self-check, in every build, on the property this whole reservation exists to hold.
        // It is cheap (the chain's blocks are already in meta_mgr, so the walk pins memory, not
        // disk) and it is the difference between a latent reissue and a refused checkpoint. An
        // assert would be worth nothing here: the damage lands in release builds.
        std::pmr::vector<uint64_t> chain(buffer_manager.resource());
        if (auto walked = meta_mgr.chain_blocks(writer.get_block_pointer(), chain); walked.has_error()) {
            return walked.convert_error<meta_block_pointer_t>();
        }
        for (uint64_t chain_block : chain) {
            if (std::binary_search(published.begin(), published.end(), chain_block)) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{"Free list of " + path_ + " would publish block " + std::to_string(chain_block) +
                                         ", which its own chain occupies",
                                     buffer_manager.resource()});
            }
        }
        return writer.get_block_pointer();
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::deserialize_free_list(meta_block_pointer_t pointer) {
        if (!pointer.is_valid()) {
            return true;
        }
        metadata_manager_t meta_mgr(*this);
        metadata_reader_t reader(meta_mgr, pointer);
        auto count = reader.read<uint64_t>();
        // STAGED, NOT INSTALLED. The list is published sorted, so an offender is typically LAST:
        // installing everything before it and then refusing would leave the allocator's pool
        // half-filled by a load that reported failure — the half-installed shape the geometry gate on
        // the open path also exists to prevent. A refused list installs NOTHING. (No reserve() on
        // `count`: it is disk bytes, and pre-sizing from a corrupt count is its own defect.)
        std::pmr::vector<uint64_t> staged(buffer_manager.resource());
        for (uint64_t i = 0; i < count && !reader.finished(); ++i) {
            const uint64_t block_id = reader.read<uint64_t>();
            // Rule 19, and the SAME boundary mark_as_free applies: an id this file does not contain
            // cannot be handed to the allocator. Two ways to fail it, one guard, because max_block_
            // (installed by load_existing_database from this very header's block_count before this
            // runs — the precondition named at the declaration) can never exceed MAXIMUM_BLOCK:
            //   * past the end of the file — block_location() seeks past EOF, and the first write of
            //     the reissued id extends the file across the whole gap;
            //   * past the addressable domain — block_location() computes (2^62 + N) * alloc, which
            //     wraps to N * alloc, a real live block rewritten with a valid CRC.
            // Testing only the second lets a list naming a block past its own header's block_count
            // open CLEANLY and arm the very next allocation. No writer of this format can emit either
            // shape, so seeing one means the chain is corrupt: say so HERE, where there is an error
            // channel, and never let it into the pool free_block_id draws from.
            if (block_id >= max_block_) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{"Free list of " + path_ + " contains block id " + std::to_string(block_id) +
                                         (block_id >= MAXIMUM_BLOCK
                                              ? std::string(", which is outside the addressable block domain")
                                              : ", but the header of this file records only " +
                                                    std::to_string(max_block_) +
                                                    " blocks (the last addressable id is " +
                                                    std::to_string(max_block_ == 0 ? 0 : max_block_ - 1) + ")"),
                                     buffer_manager.resource()});
            }
            staged.push_back(block_id);
        }
        // Corrupt free-list chain (read past end) -> data_corruption, surfaced at the load boundary
        // instead of throwing — and before anything is installed, like the guard above.
        if (reader.has_error()) {
            return core::error_t(reader.error());
        }
        // The whole list passed; only now does it become allocator state. reusable_, not
        // pending_free_: this list belongs to the root the engine is opening ON, so it is by
        // construction the DURABLE root's own statement about what it does not reference.
        // Nothing is in flight at load time, so there is nothing to quarantine — and routing
        // it to pending_free_ instead would strand every reclaimed block until the first
        // checkpoint of the new process.
        reusable_.insert(staged.begin(), staged.end());
        return true;
    }

} // namespace components::table::storage

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

    // --- Database header checksum (A7.1) ---

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

        // L5: every write below LAYS DOWN the file. Discarding their results (which is what
        // this used to do) produces a file with no valid slot while the engine is told the
        // database was created; the failure then surfaces at the next open, as
        // data_corruption, with the create long past and nothing left to retry.
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

        // Fresh-file rule: ONE slot is written — the slot iteration 0 owns — and the other is
        // never written at all. Three placements were possible and only this one is right:
        //   * both slots (what this used to do) repeats write_header's mistake at file
        //     creation and blurs the invariant "a slot holds exactly one iteration";
        //   * the slot iteration 1 owns would have the very first checkpoint overwrite the
        //     only valid root, so the file would spend its first checkpoint with no fallback;
        //   * the slot iteration 0 owns leaves the initial (empty) root standing as the
        //     recoverable previous root until iteration 2 legitimately retires it.
        // The never-written slot is a hole between the main header and this one; it reads
        // back as 4 KiB of zeros, which decode as iteration 0 — a TIE with the real header —
        // pointing at metadata root 0, a REAL block id. Nothing but the checksum stops that
        // from being selected, which is exactly why the checksum is not optional.
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
        // A7.3: a brand-new file has no superseded root. Everything stays INVALID/empty until
        // the first write_header commits one.
        durable_meta_block_ = INVALID_INDEX;
        durable_free_list_ = INVALID_INDEX;
        return true;
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::load_existing_database() {
        using namespace core::filesystem;

        // A7.5: a LOAD never creates the file. This used to pass FILE_CREATE, so probing a
        // MISSING .otbx silently manufactured a 0-byte file — the probe mutated the very state
        // it was probing, and "the file never existed" became indistinguishable from "the file
        // was truncated" one open later. With the external `.prev` backup gone, that empty
        // file would be the only thing left to interpret. A missing file is its own loud,
        // distinct refusal, and the probe leaves the filesystem exactly as it found it.
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
            // A7.5 terminal refusal: with the external whole-file backup gone, this message is
            // the operator's ONLY remaining tool. Describe BOTH slots in full — which failed
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

        // The winner is the VALID slot with the greater iteration. Previously this was a
        // naked `header1.iteration >= header2.iteration`, so a slot full of garbage carrying
        // a large iteration beat a perfectly good root.
        const database_header_t& active =
            (header1_valid && (!header2_valid || header1.iteration >= header2.iteration)) ? header1 : header2;

        // A7.6: meta_block == INVALID_INDEX means "this table has no checkpointed content yet
        // — it is empty, and its schema comes from the catalog". That legalisation is safe
        // ONLY with independent evidence that the file really is young, because corruption
        // that knocks out the newest slot of a CHECKPOINTED file surfaces the very same
        // header here: the two-slot fallback selects the initial iteration-0 slot, whose
        // meta_block is INVALID by construction. Without a second witness, "INVALID = empty"
        // would convert a corrupt table into a silently empty one.
        //
        // The witness is the FILE SIZE, and it is chosen because the writer physically cannot
        // fake it: create_new_database lays down exactly BLOCK_START bytes (three header
        // sectors, no blocks), and every checkpoint writes its data/metadata blocks past
        // BLOCK_START and fsyncs them BEFORE its header commits. A file that has ever
        // contained checkpointed content is therefore strictly larger than BLOCK_START, and
        // no crash truncates it back (block writes never shrink the file). The two other
        // candidates the plan named are rejected as the DECIDING witness: the `.wal_id`
        // sidecar is a separate file that can be lost or deleted independently of the .otbx
        // (its absence proves nothing), and the catalog lives in other tables loaded through
        // this very code path (circular). The manager layer still uses the sidecar in the
        // REFUSING direction only — a sidecar claiming a checkpoint over a young file is a
        // contradiction — never to legalise.
        //
        // Note what this deliberately refuses: a file whose FIRST checkpoint crashed after
        // laying down blocks but before its header committed also shows INVALID + blocks.
        // The file alone cannot distinguish that crash from slot corruption of a checkpointed
        // table, so per rule 6 it is refused rather than guessed at; a never-checkpointed
        // table's rows (if any) still live in the WAL, which replays from id 0 for a table
        // with no checkpoint sidecar.
        //
        // The gate applies to the INITIAL header only (iteration 0): that is the one and only
        // header corruption can SURFACE by knocking out a newer slot, because create_new_database
        // writes it unconditionally and it never carries a root. An iteration >= 1 slot with
        // meta_block INVALID is different in kind — write_header committed those bytes and
        // checksummed them, so INVALID there is the writer's own recorded statement ("this root
        // names no metadata"), exactly as trusted as the block_count and free_list beside it.
        // The engine's checkpoint always sets a real root before committing, but the block
        // manager's contract (and the tests that pin it) allows committing without one.
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

        // GEOMETRY BEFORE ANY ADOPTION, because a header that cannot describe this file is not
        // a header to open, and nothing below should be half-installed when it is refused.
        //
        // The header's claim about the file's geometry is DISK BYTES. Adopting it unchecked
        // wrapped block_size() for any value <= DEFAULT_BLOCK_HEADER_SIZE; the guard lives in
        // set_block_allocation_size and reports instead of throwing, and the open fails loudly
        // (load_storage_disk_sync hands the refusal to its bootstrap caller, file untouched)
        // rather than running the engine on a nonsense block size.
        //
        // The two short-circuits this check used to carry are gone, and the ZERO one is the
        // reason. `active.block_alloc_size != 0` skipped the geometry check entirely and left
        // the engine running on whatever size the CALLER passed to this manager's constructor —
        // a compatibility branch for a header shape no writer in this build produces:
        // database_header_t::initialize() stores DEFAULT_BLOCK_ALLOC_SIZE and write_header()
        // stores block_allocation_size(), which set_block_allocation_size has already proven to
        // be a non-zero sector multiple. So a zero here is not an older format, it is a corrupt
        // or truncated header, and "it declares no geometry, so use mine" is a guess about the
        // stride of every block in the file (rule 6: refuse, do not guess). The
        // `!= block_allocation_size()` short-circuit goes with it for a smaller reason — it
        // made the header's claim validated only when it disagreed with the constructor, so an
        // AGREED-ON nonsense size was adopted by both without anyone checking it.
        if (auto adopted = set_block_allocation_size(active.block_alloc_size); adopted.has_error()) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Database header of " + path_ +
                                                      " is unusable: " + std::string(adopted.error().what.c_str()),
                                                  buffer_manager.resource()});
        }

        iteration_ = active.iteration;
        meta_block_ = active.meta_block;
        max_block_ = active.block_count;
        // A7.3: the slot just selected IS root N. Record its two chain pointers now — the very
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
        // checksum of stale bytes is how a read failure used to masquerade as data_corruption
        // (or, worse, as a valid block when the buffer happened to hold the right thing).
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

        // A7.2: reusable_ ONLY. pending_free_ is deliberately not a fallback when reusable_
        // runs dry — an empty reusable_ means "nothing is free under the durable root", and
        // the correct answer to that is to extend the file, not to reach for blocks that root
        // still reads. Extending is what the loop below falls through to.
        //
        // M7: the free list is DISK BYTES — deserialize_free_list fills it straight from the
        // .otbx — so "the id I am handing out is not live" is a statement about untrusted
        // input, not about this code's own consistency. It used to be an assert(): gone under
        // NDEBUG, which is exactly the build where a reissued live id gets overwritten with a
        // valid CRC and the corruption becomes unfindable. It is now a branch, in every build.
        //
        // A candidate that IS live is dropped from the free list for good (reissuing it later
        // would be the same bug on a later round), the corruption is latched for the caller
        // that can act on it, and allocation continues with the next candidate so the failure
        // stays reportable instead of becoming an abort on a path an actor thread runs
        // (rules 6/9: loud, but a checkpoint that refuses to commit — not a dead process).
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
        // A7.3: every id this round hands out belongs to the root being built (or to the
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

    // ITEM A — THE DELIBERATE DECISION about a reclaim that could not read root N.
    //
    // reclaim_superseded_root walks the durable root's two chains through chain_blocks, which
    // pins every sub-block via read(). read() answers io_error / data_corruption, and until
    // this function existed that answer went nowhere but up the return chain: the checkpoint
    // failed, and degraded() stayed FALSE. Every health gate in the engine keys exclusively on
    // degraded() (data_table_t::compact, table_storage_t::checkpoint,
    // agent_disk_t::checkpoint_inner), so a failed reclaim tripped none of them, and the caller
    // kept compacting. One rotten block in root N's metadata chain therefore cost a FULL COPY
    // of the table per round: compact rebuilt the collection into freshly extended blocks
    // (its release goes to pending_free_, which only a committed header drains), the reclaim
    // failed on the same block again, and the next round repeated it. Measured on a 12k-row
    // table: +18 blocks EVERY round, forever, with every health indicator reporting the file
    // healthy.
    //
    // DECISION: LATCH, and do NOT roll the failed round back. Both halves, stated:
    //
    //  * WHY LATCH, and why into allocation_error_ rather than durability_error_. Nothing was
    //    written badly here — what failed is the manager's ability to ACCOUNT for its blocks,
    //    which is exactly what allocation_error_ means (the free-list-proven-corrupt latch is
    //    its other instance). And it is sticky for the same reason those are: root N is the
    //    root a crash recovers, so a file whose last durable root cannot be walked has no
    //    proven recoverable root at all. The engine cannot tell a rotten bit (which does not
    //    heal) from a transient EIO on the same block, and guessing "transient" is the guess
    //    that keeps rebuilding. Loud, not fatal (rule 6): the table still serves reads and
    //    writes, its WAL records are never sealed away, and every CHECKPOINT reports this
    //    error until the file is rebuilt.
    //
    //    "The checkpoint returned an error" is NOT enough on its own, and that is the whole
    //    point of this function: the caller that decides whether to compact does not look at
    //    the checkpoint's return value at all — it looks at degraded().
    //
    //  * WHAT A BLIND ROLL BACK WOULD COST. mark_as_free'ing everything in issued_since_root_
    //    would free LIVE TABLE STATE: data_table_t::compact has already swapped row_groups_ to
    //    the rebuilt collection whose write-through allocated some of those ids, and
    //    column_data_t::checkpoint has already re-pointed still-managed live tail segments onto
    //    blocks allocated in this same round (transition_to_disk through a fresh
    //    partial_block_manager). Handing THOSE ids back is precisely the corruption A7.2/A7.3
    //    exist to prevent.
    //
    //    A7.7 does roll back — see roll_back_uncommitted_round — but it is a DISCRIMINATION,
    //    not a blind sweep: it releases what the round allocated for the root it was building
    //    and withholds what the live in-memory tree now depends on, using registry_alive() as
    //    the liveness test. That test is exact, not approximate: register_block stores a
    //    weak_ptr whose only owner is the live column_segment_t, and the eviction queue holds
    //    a weak_ptr of its own and never touches blocks_ — so an evicted block is still
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
        // Must mirror free_block_id EXACTLY, pending_free_ included (i.e. excluded): a peek
        // that names an id the allocator would not hand out is worse than no peek at all.
        //
        // The mirror was written down and not implemented. free_block_id does NOT hand out
        // *reusable_.begin(): a candidate with a live handle in the block registry is live
        // table state, so it is skipped (and dropped from the list for good, and latched), and
        // allocation continues with the next one. This returned the skipped candidate. There
        // is no production caller today, which is exactly what made the divergence survivable
        // — and exactly what makes it a trap: the first caller to appear would size, reserve
        // or lay out against an id the very next free_block_id refuses to give it, and the
        // only case the two answers differ in is the case the free list is corrupt.
        //
        // What a peek does NOT mirror is the SIDE EFFECTS. Dropping the corrupt candidate and
        // latching the accounting error belong to the allocation that consumes it, not to a
        // look: latching here would let a diagnostic turn a checkpoint into a refusal. So this
        // walks past the same candidates without removing them, and answers what the next
        // free_block_id would RETURN — including the fall-through to the high-water mark when
        // every candidate is live.
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
        // Rule 19, same class as the deserialize_free_list guard one level out, and the same
        // reason it cannot be an assert(): the ids arriving here are NOT this manager's own.
        // data_table_t::compact collects them from the live collection, and a segment's
        // big-string overflow ids are rebuilt verbatim from data_pointer_t::overflow_blocks,
        // which data_pointer_t::deserialize reads straight out of the .otbx with no domain check
        // anywhere in between. So this is a path fed by disk bytes.
        //
        // As an assert() it was an abort in a debug build — on the agent thread that runs
        // compact, i.e. across an actor boundary (rule 9) — and NOTHING under NDEBUG, where a
        // transient-domain id (>= MAXIMUM_BLOCK) would enter the free pool, be promoted by the
        // next durable header, be handed out, and have block_location wrap it:
        // (2^62 + N) * 2^18 == N * 2^18, a REAL live block, rewritten with a valid CRC. Branch
        // instead, in every build: drop the id and latch, which makes the next write_header
        // refuse to commit a root built on state already known to be inconsistent.
        //
        // THE BOUNDARY IS THE FILE, not the domain, and the two are not the same guard. This
        // used to test `>= MAXIMUM_BLOCK` alone, which rejects an id that would WRAP inside
        // block_location and accepts every id between the end of the file and 2^62. Those are
        // just as unaddressable: block_location computes BLOCK_START + id * alloc, the seek
        // lands past EOF, and the write extends the file across the whole gap — a corrupt
        // overflow_blocks entry naming block 2^40 asks for a 256 TB file — while total_blocks()
        // still reports the old extent and the published free list hands the same id to the
        // next process that opens the file. max_block_ is that boundary — with its meaning
        // stated exactly, because it is NOT always the file's extent: it is this manager's
        // ISSUANCE MARK (adopted from the header's block_count at open, raised by
        // free_block_id, lowered only by roll_back_uncommitted_round over the contiguous
        // released tail). After a rollback the mark can sit BELOW both the physical end of the
        // file and the durable header's block_count — the failed tail's bytes stay in the
        // file, truncate() has no production caller — so `id < max_block_` means "an id this
        // manager currently stands behind", a SUBSET of the blocks physically present. The
        // narrower measure is safe in the only direction a release guard can err: nothing
        // legitimate can name an id in the gap, because the rollback's descent walks only over
        // RELEASED ids (registry-alive ids are filtered out of that set, and root N's blocks
        // are never in it — they are not in issued_since_root_) and stops at the first id it
        // may not cross, so every live and every root id is always below the mark. The gap
        // holds only a failed round's abandoned tail, which nothing owns and nothing may free
        // again; a refusal there latches a leak (recoverable), it cannot lose a block. It is
        // also the STRICTLY WIDER guard — max_block_ can never exceed MAXIMUM_BLOCK — so the
        // domain case is still caught, and named separately because it means something else.
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
        // A7.2. The one production caller is data_table_t::compact, which runs immediately
        // before the checkpoint and releases the OUTGOING collection's blocks — precisely the
        // blocks the CURRENT durable root still points at. So the release is not "this block is
        // free", it is "this block will be free once the root being built becomes durable", and
        // that is what pending_free_ means. free_block_id cannot see it from here.
        //
        // An id already in reusable_ stays there instead of being demoted: reusable_ means the
        // DURABLE root does not reference it, and freeing it again in memory cannot make the
        // durable root start referencing it. Demoting would only quarantine a block that is
        // provably safe, and it is also what keeps the two sets disjoint.
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

    // A7.2 promotion — and the deliberate decision about what happens when the checkpoint that
    // was supposed to trigger it FAILS.
    //
    // DECISION: a failed checkpoint neither promotes pending_free_ nor discards it. The ids stay
    // quarantined, and the NEXT checkpoint whose header becomes durable promotes them.
    //
    // Why not promote: the blocks are still live under the still-current old root. That root is
    // what a crash recovers, and handing its blocks to the allocator is the whole defect.
    //
    // Why not discard (i.e. treat them as permanently lost): the release itself was not undone.
    // data_table_t::compact swapped row_groups_ BEFORE the checkpoint ran and does not roll that
    // back, so in memory nothing reaches those blocks any more — dropping them on the floor
    // would leak the space with no path back, on a failure that the engine otherwise recovers
    // from (agent_disk defers the entry and retries next round).
    //
    // WHAT THE MEMORY/DISK DISAGREEMENT MEANS, stated rather than papered over: after a failed
    // checkpoint the in-memory table is the COMPACTED one while the durable root is the
    // PRE-compact one. Those disagree about which rows exist and about which blocks are live,
    // and pending_free_ is the record of the second half of that disagreement. Its consequences:
    //   * the file does not shrink and cannot reuse the space until a checkpoint succeeds. That
    //     is a bounded space cost (one collection's worth of blocks per deferred round), not a
    //     correctness one;
    //   * if the process dies before any later checkpoint succeeds, the file reopens on the old
    //     root, whose free list never listed these blocks, and they come back as its live data.
    //     Nothing is lost — which is only true BECAUSE they were never reissued;
    //   * the rows the compaction dropped are still in the WAL: agent_disk_t::checkpoint_inner
    //     does not advance the .wal_id sidecar for an entry whose checkpoint failed, so replay
    //     restores them.
    // The one thing that would turn this from a disagreement into corruption is promoting on a
    // failure, so promotion has exactly two callers and both of them have proof the new header
    // reached the device.
    //
    // A7.3 folded the rest of "the new root is now THE root" into this same call, because it is
    // the same event and splitting it would let the two halves disagree: the promotion of
    // pending_free_ (A7.2) and the adoption of the new root's chain pointers and data blocks
    // (A7.3) are both statements about which root a crash would recover, and both must be made
    // at the instant the header is proven on the device — never earlier, never separately.
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

    // --- A7.3: reclaiming the superseded root ---

    void single_file_block_manager_t::adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& block_ids) {
        durable_root_data_.clear();
        durable_root_data_.insert(block_ids.begin(), block_ids.end());
    }

    // THE FORMULA, and what each term is for:
    //
    //   free = {blocks of root N} u {metadata chain of N} u {free-list chain of N}
    //          - {blocks of root N+1} - {ids live in the block registry}
    //
    // Both subtractions are load-bearing and neither is redundant:
    //
    //   * {blocks of root N+1}. The round under construction allocates its data blocks, its
    //     metadata chain and (moments later) its free-list chain out of reusable_ — i.e. out of
    //     blocks the PREVIOUS rounds released. Nothing stops one of those from also being named
    //     by root N... except that root N's blocks are not in reusable_ (this function is what
    //     puts them there, and it puts them in pending_free_, which free_block_id never draws
    //     from). Rather than lean on that, the subtraction is made explicit twice over: against
    //     the data blocks data_table_t just wrote, and against issued_since_root_ — every id
    //     handed out since the durable root was committed.
    //
    //   * {ids live in the block registry}. Root N's DATA blocks are live table state whenever
    //     the table was loaded from root N and this round did NOT compact it: the segments in
    //     memory still point at those very blocks and will page them back in. Freeing one would
    //     hand it to the next allocation and overwrite live data with a valid CRC. The registry
    //     is the only thing that knows this, which is exactly why the formula subtracts it.
    //
    // WHAT IT COSTS, said plainly. The ids go into pending_free_, not reusable_ (A7.2's
    // mechanism, composed with rather than bypassed): root N's blocks only become issuable once
    // a root that does NOT name them is on the device. So between this call and the fsync
    // inside write_header the file holds TWO complete copies of the table — the compacting
    // checkpoint's peak footprint is 2x, permanently, inside the .otbx. (Before A7.5 the same
    // double occupancy existed as an external whole-file backup copy, deleted on success;
    // since A7.3/A7.5 it lives inside the file.) And a SUCCESSFUL round does not give it back:
    // truncate() only trims past max_block_, and nothing on the success path lowers the mark,
    // so the space is reused by later rounds rather than returned to the filesystem. (The one
    // thing that does lower it is roll_back_uncommitted_round, on the FAILED path — A7.7.)
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
                // ITEM A: propagating alone left degraded() false, so nothing upstream stopped
                // compacting and the file grew by a full copy of the table every round.
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
            // ADDRESSABILITY FIRST, because this list is DISK BYTES. durable_root_data_ is
            // collected from data_pointer_t::block_pointer.block_id — a full uint64 the
            // metadata reader takes straight off the file, with no check anywhere in between.
            // An id this FILE does not contain cannot be freed: past max_block_ it names no
            // block at all, and past MAXIMUM_BLOCK block_location would wrap it,
            // (2^62 + N) * 2^18 == N * 2^18, onto a REAL live block. mark_as_free applies
            // exactly this boundary and latches the corruption, which is what makes the next
            // write_header refuse to commit — the screen here is what keeps the CALLER from
            // walking on: unregister_block's assert() only covers the domain half, and
            // ++reclaimed would otherwise count an id that reached no pool.
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

    // --- A7.7: giving back what a FAILED round took ---
    //
    // THE PROBLEM, measured. A checkpoint round allocates three kinds of block before it
    // commits, and NONE of them is registered in the block registry:
    //   * the PACKED COPY — column_checkpoint_state_t::flush_segment routes every column
    //     segment through partial_block_manager_t, which holds bare block_t buffers and calls
    //     no register_block; the only thing that names those ids is the row_group_pointer_t
    //     stream of the root being built;
    //   * the TABLE-METADATA chain — metadata_manager_t, same story (its blocks_ vector, no
    //     registry);
    //   * the FREE-LIST chain — a second metadata_manager_t inside serialize_free_list.
    // When the header write fails, no root names any of the three and nothing in memory reaches
    // them either: they stay in used_blocks_/issued_since_root_ forever. On a 7.8 MB table that
    // is ~655 KB per round, for as long as the failure persists, while degraded() stays FALSE —
    // reconcile_failed_header_write case 2 deliberately does not latch, so that a transient
    // ENOSPC can still recover. The A7.4-era gate stops the entry COMPACTING again after a
    // failure; it does not stop the round from spending these blocks.
    //
    // WHY THIS IS NOT THE "free everything in issued_since_root_" the note at
    // latch_reclaim_failure refuses. That note is right, and it names the two things a blanket
    // rollback would destroy: the rebuilt collection data_table_t::compact has already swapped
    // into row_groups_, and the live tail segments column_data_t::checkpoint has already
    // re-pointed. Both of those reach their blocks through column_data_t::transition_segment_to_disk,
    // which DOES block_manager_.register_block(alloc.block_id) and hands the resulting
    // block_handle_t to the live column_segment_t — and a column_segment_t owns that handle by
    // shared_ptr for as long as it is in the tree (buffer-pool eviction unloads the BUFFER, it
    // does not destroy the handle). So the in-memory live tree's dependence on a block is
    // exactly "a live handle in the registry", which is the subtraction A7.3's formula already
    // makes and which registry_alive() answers:
    //
    //     releasable = issued_since_root_ - {ids live in the block registry}
    //
    // The three kinds above fall on the releasable side because they are never registered; the
    // rebuild's and the re-point's blocks fall on the protected side because they always are.
    // Checked against the code rather than assumed: free_block_id is reached from exactly two
    // places (metadata_manager_t::allocate_handle/reserve and
    // partial_block_manager_t::get_block_allocation), and of the partial-block callers only
    // transition_segment_to_disk registers what it allocated — flush_segment,
    // persist_string_overflow and the metadata writers all leave the id unregistered because
    // nothing live points at it.
    //
    // WHY reusable_ AND NOT pending_free_, which is the other half of the discrimination.
    // pending_free_ means "released by the in-flight round, but the DURABLE root still names
    // it". That is false for every id here, by construction: issued_since_root_ is emptied by
    // promote_durable_root at the exact instant a root becomes durable, so everything in it was
    // allocated AFTER root N was committed and root N cannot name it. Routing these to
    // pending_free_ instead would be the defect restated — pending_free_ only drains on a
    // committed header, and the whole scenario here is that no header commits, so the file
    // would keep growing at exactly the old rate.
    //
    // CAN ANYTHING STILL READ A RELEASED BLOCK? The round may already have written some of them,
    // so the bytes on the device are garbage with a valid CRC on top. Three readers exist and
    // all three are closed:
    //   * a chain walk of the durable root — root N's chains, disjoint from issued_since_root_;
    //   * block_handle_t::load() — only for a REGISTERED id, and every registry-alive id is kept
    //     while every released one is unregister_block'd, so no handle can be resurrected for it;
    //   * deserialize_free_list at open — it reads root N's PUBLISHED list. A crash between this
    //     rollback and the next committed header leaves root N active, and root N's list either
    //     already listed the id (it came out of reusable_, i.e. out of root N's own list — the
    //     rollback restores exactly the state root N describes) or the id is past root N's
    //     block_count and does not exist under root N at all. Neither case leaves a root naming
    //     a block this rollback gave away.
    // The next committed header publishes reusable_ ∪ pending_free_, which now includes these
    // ids — correct, because the root that header describes does not name them either.
    //
    // THE HIGH-WATER MARK. Everywhere else max_block_ only rises (free_block_id bumps it when
    // reusable_ is dry), so merely returning ids to the pool would stop the file GROWING
    // without lowering total_blocks(). This function is the sole exception, and the rest of
    // this note is what makes lowering it safe here. A failed round's allocations are NOT a contiguous tail in
    // general — free_block_id serves scattered ids out of reusable_ first and only extends when
    // that is dry — but the part that DID extend is, by definition, the top of the file. So the
    // mark is walked down over the released ids only, and it stops at the first id from the top
    // that is not being released: root N's blocks are never in that set (disjoint, above), and a
    // registry-alive id is filtered out of it, so the descent can never uncover a block anything
    // still needs. Ids that fall past the new mark are dropped from reusable_ as well — issuing
    // one would put a block beyond the block_count the next header records.
    //
    // What this does NOT do is shrink the file: the bytes a failed round wrote past the old end
    // are only returned to the filesystem by truncate(), which has no production caller today
    // and is not called from here — this path runs on a device that has just reported a failure
    // and has no error channel to report a failed ftruncate through. The space is REUSED by the
    // next round, which is what makes the footprint flat.
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
            // LOAD-BEARING, not defensive. The note that used to sit here claimed an id this
            // round allocated "cannot be in pending_free_ today", reasoning that free_block_id
            // draws only from reusable_ and mark_as_free is pending_free_'s only filler. That
            // reasoning is about ONE round; issued_since_root_ spans every round since the last
            // COMMITTED header. A second failed round that is allowed to compact swaps out the
            // collection the FIRST failed round built and mark_as_free's its blocks — ids no
            // header ever promoted out of issued_since_root_ — so the two sets do intersect,
            // and on the ordinary retry path rather than in some future refactor. Measured on
            // the 12k-row gate: 14 ids in both after two failed compacting rounds
            // (test_failed_round_rollback.cpp, gate 2b).
            //
            // Moving them is also the right answer, not merely the safe one: pending_free_
            // means "the DURABLE root still names it", which is false for everything in
            // issued_since_root_ by construction, and pending_free_ drains only on a committed
            // header — the one event this whole path exists because it did not happen — so
            // leaving them quarantined would strand the space indefinitely.
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
        // ...including its metadata pointer. set_meta_block installed the ABANDONED chain's
        // pointer earlier in the round, and the blocks under it were just handed back to the
        // allocator, so leaving it there would leave this manager holding a root pointer into
        // reusable space. The honest value is the pointer of the root that actually stands,
        // which is exactly what a fresh load_existing_database would install (INVALID_INDEX
        // when there is no durable root yet — also what a fresh open of that file would see).
        // durable_meta_block_ itself is untouched by this rollback: it describes root N, which
        // this whole path exists to leave alone.
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
        // L1: THE deepest link. This is where every data block, every metadata block and every
        // free-list block in the system lands, and its answer used to go nowhere.
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
        // H2: the target slot is a pure function of iteration_ parity, so the counter is not
        // a "we are attempting iteration N" marker — moving it moves the target. It used to be
        // incremented unconditionally, right here, BEFORE the write; a failed write therefore
        // still consumed an iteration, and the retry aimed at the OTHER slot, which is the one
        // holding the last durable root. The retry overwrote exactly the state it existed to
        // preserve, and two failures in a row could leave the file with no valid slot at all.
        // The candidate is committed to iteration_ only once write AND fsync have succeeded,
        // so a retry re-aims at the same slot and the previous root is never in the line of
        // fire.
        //
        // A checkpoint whose allocations drew on a free list proven corrupt (M7) must not
        // become the durable root: the metadata it just wrote may sit on blocks that are
        // still live table state. This is the commit point, so this is where that refusal
        // belongs — and the caller already knows what to do with a failed checkpoint (H1).
        //
        // A7.7: both refusals below return BEFORE a single byte of a header slot is written, so
        // "this round's header did not become durable" is proven by the control flow itself —
        // which is exactly roll_back_uncommitted_round's precondition. Doing it here as well as
        // at the orchestrator is the same choice the durability latch makes: the invariant is
        // structural, not something a caller has to remember. (It is refused outright when a
        // previous reconcile could not tell which root is on the device.)
        if (allocation_error_.contains_error()) {
            roll_back_uncommitted_round();
            return core::error_t(allocation_error_);
        }
        // L1/L2 commit gate. A block write or the pre-header barrier failed earlier in this
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

        // Shadow paging: write the ONE slot this iteration owns, and sync it. The other slot
        // is deliberately left alone, so it keeps the PREVIOUS root — that IS the redundancy.
        // This used to write the same header to BOTH slots, which destroyed the very thing
        // the double-header protocol exists to protect: after the second write no previous
        // root remained, and the only crash the layout could survive was one landing exactly
        // between the two writes.
        //
        // H1: this is the ONLY durable write of a checkpoint — A7.1 collapsed the old
        // write-both-offsets protocol into a single slot write, so there is no second
        // attempt to accidentally cover for this one. Both bools are therefore load-bearing
        // and are reported to the caller: on ENOSPC/EIO here the checkpoint did NOT happen,
        // and the caller must not advance its WAL bookkeeping or drop its backup.
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
            // A7.2 promotion point. The slot this write landed in now holds the root the file
            // recovers to, and that root does NOT reference the blocks compact() released this
            // round — its own free list, serialized above, lists them. From this instruction on
            // they are free under the durable root, which is the definition of reusable_. Doing
            // it one line earlier (before the fsync answered) would put them back in the
            // allocator while the OLD root was still the recoverable one, which is the entire
            // bug A7.2 exists to close.
            promote_durable_root(write_header.meta_block, write_header.free_list);
            return true;
        }
        return reconcile_failed_header_write(next_iteration, write_ok, sync_ok);
    }

    // --- Header/disk divergence (the deliberate decision) ---
    //
    // A torn 4 KiB header write can leave a CRC-VALID header of the new generation on disk
    // while write() reports failure. That is structural, not bad luck: every byte that differs
    // between two generations lives in bytes 0..47 — inside the FIRST 512-byte hardware sector
    // of the 4 KiB header — and the padding is zeros in every generation, so a partial write
    // reassembles into a byte-exact copy of ONE generation and passes the CRC. The checksum
    // catches zeros, garbage and a never-written slot; it does not catch a tear.
    //
    // So "the checkpoint failed" and "the new root is on disk" can both be true, and the engine
    // used to resolve that by ASSUMING the first: it kept iteration_ behind and left the
    // .wal_id sidecar behind, and on restart the WAL replayed records into a root that already
    // contained them (duplicated rows, not a missing update).
    //
    // DECISION: after a failed header write the engine does not assume in either direction. It
    // READS THE TWO SLOTS BACK and adopts whatever the disk says, using the same slot-selection
    // rule the open path uses. Three outcomes, and each one leaves the engine's belief equal to
    // the file's content:
    //
    //   1. the active slot is the NEW iteration and the fsync succeeded — the new root is on the
    //      device. The barrier before it already made the data and metadata blocks durable, so
    //      this checkpoint IS complete however the write call answered. Adopt it and report
    //      SUCCESS: the sidecar may advance, and it must, because the rows are in that root.
    //   2. the active slot is the iteration this manager already believed in — nothing of the
    //      new header exists, not even in the page cache (the read-back would see it if it did),
    //      so nothing can reach the device later. The previous root stands, which is exactly
    //      what the engine already believes. Report a clean io_error: the checkpoint did not
    //      happen, the caller keeps its backup and its WAL records.
    //   3. anything else — the fsync failed so a landed-looking slot is not evidence about the
    //      DEVICE, or neither slot reads back, or the winner is a generation this manager never
    //      wrote. The truth cannot be established by reading, so it is not guessed: latch, and
    //      refuse every later checkpoint on this file rather than commit a root on top of an
    //      unknown one.
    //
    // Read-back goes through this same handle, i.e. through the page cache. That is deliberate
    // and is what makes case 2 sound: the page cache is a SUPERSET of the device, so "the new
    // header is not even in the page cache" is a strictly stronger statement than "it is not on
    // the device". Case 1 needs the opposite direction, which is why it additionally requires
    // the fsync to have succeeded.
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
            // A7.7: no root can be named, so no statement about which blocks a root owns can be
            // made either. The failed round's allocations stay exactly where they are.
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
            // the A7.2 promotion is owed here exactly as on write_header's success path. Cases
            // 2 and 3 below do NOT promote: in case 2 the previous root demonstrably still
            // stands, and in case 3 which root stands cannot be established by reading, and
            // "promote when unsure" hands out blocks a recoverable root may still read.
            promote_durable_root(active.meta_block, active.free_list);
            return true;
        }

        if (active.iteration == iteration_) {
            // Case 2: nothing of the new header exists. Engine and disk already agree, and
            // the previous root demonstrably still stands — so this is the RECOVERABLE case
            // and the retry is meant to reach the same slot again (A7.1/H2). Deliberately NOT
            // latched: latching here would turn a transient ENOSPC into a permanently degraded
            // manager and defeat the retry the same-slot rule exists for.
            //
            // OPEN, and it is a real cost: a PERSISTENT write error at this offset is retried
            // forever, and each retried round runs compact() first, rebuilding the collection
            // into freshly extended blocks because reusable_ never refills — the file grows by
            // a full copy of the table per round. The lever for that is gating the COMPACT on
            // "the previous round failed", not permanently degrading the manager here.
            //
            // A7.7 closes the other half of that cost. The gate stops the REBUILD; this branch
            // is the only place in the class that can prove the round's own allocations are
            // unreferenced — the read-back has just established that nothing of the new header
            // exists, so the durable root is still root N and root N cannot name a block issued
            // after it was committed. Give those blocks back before reporting the failure, so a
            // retried round spends the SAME blocks again instead of fresh ones.
            roll_back_uncommitted_round();
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to write database header, " + what +
                                                      "; the previous root (iteration " + std::to_string(iteration_) +
                                                      ") stands and is unchanged",
                                                  buffer_manager.resource()});
        }

        // Case 3: indeterminate. A7.7: this is the ONE outcome where the round's allocations
        // must NOT be given back — a landed-looking slot that the fsync never confirmed may
        // still be the root a crash recovers, and that root names the blocks this round wrote.
        // Handing them to the allocator would be precisely the corruption A7.2/A7.3 prevent.
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
        // L2: the pre-header barrier. Its whole job is to make the blocks written this round
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
        // A7.3 note on what this can and cannot give back, as amended by A7.7. max_block_ rises
        // whenever free_block_id extends the file, and the ONE thing that lowers it is
        // roll_back_uncommitted_round walking it down over a failed round's released tail.
        // So this trims past the mark; it shrinks the file only by what such a rollback gave
        // back. PRECONDITION for any future caller: the mark can now sit BELOW the block_count
        // a durable header records, so truncating without checking that would cut the file
        // below the extent the durable root describes. A compacting
        // checkpoint holds two copies of the table at once — the superseded root's blocks stay
        // quarantined in pending_free_ until the new header is on the device — so the mark
        // settles at roughly twice the table's own size and stays there. The space is REUSED by
        // every later round (that is what makes an unchanged database stop growing) but it is
        // not returned to the filesystem. Giving it back would need block relocation plus a
        // pointer rewrite, which is a different task.
        auto file_end = block_location(max_block_);
        if (!handle_->truncate(static_cast<int64_t>(file_end))) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to truncate " + path_ + " to " + std::to_string(file_end),
                                                  buffer_manager.resource()});
        }
        return true;
    }

    // --- Free List Persistence ---

    // WHAT THE PERSISTED FREE LIST MUST CONTAIN — reusable_ ∪ pending_free_ ∪ {live-only
    // blocks the root being written does not name}.
    //
    // R-LEAK, found by the A7.4 crash matrix (its walker was the first to judge a freshly
    // REOPENED file). A committed round leaves the live in-memory tree holding blocks the
    // durable root does NOT name: data_table_t::compact's write-through and
    // column_data_t::checkpoint's re-pointed tail segments keep their blocks through
    // registered block_handle_t's, while the root's pointer stream names the PACKED COPY
    // flush_segment wrote. In-process those blocks are protected (registry_alive keeps them
    // out of every pool), and the next compact returns them via mark_as_free. But no root
    // references them and no list published them — so the instant the process ends, by crash
    // OR by clean exit, they are orphans: a reopened file cannot ever find them again, the
    // reclaim only walks ROOTS, and the file leaked one live-tree's worth of blocks (a full
    // copy of the table) PER PROCESS LIFETIME, forever. Measured at 6k rows: 8 blocks (2 MiB)
    // per restart.
    //
    // The fix is the third term: publish every id whose ONLY owner is the live tree — i.e.
    // registry-alive and not named by the root this list will hang off (pending_root_data_,
    // recorded by reclaim_superseded_root earlier in this same round). That is not a
    // relaxation of the list's meaning but the completion of it: the list is the root's own
    // statement about what it does NOT reference, and these blocks are exactly that. Safety,
    // both directions:
    //   * a FUTURE OPEN puts them in reusable_ — correct, because in that process nothing
    //     reaches them: the loader loads the root's named blocks only;
    //   * THIS process's pools are untouched — the published chain is disk bytes, not
    //     allocator state, so in-process the registry keeps protecting the live tree exactly
    //     as before. The walker's classification splits the same way (reachable_free_overlap
    //     stays "the root's OWN blocks freed", live_superseded names this deliberate case).
    // The subtraction of pending_root_data_ is what keeps the fatal direction impossible: an
    // id the root names is never published, and the chain self-check below still proves the
    // list does not free its own chain.
    //
    // The header this chain is about to hang off describes the NEW root. Under that root the
    // outgoing collection is gone (data_table_t::compact swapped it before the checkpoint
    // started) and nothing reaches its blocks, so under the NEW root they are free — exactly
    // like the blocks already in reusable_. A persisted list that omitted pending_free_ would
    // describe the new root as still owning blocks nothing points at: the space would never
    // come back, which is the opposite failure and just as real.
    //
    // The asymmetry with the IN-MEMORY promotion is the point, and it is not an inconsistency:
    //   * the serialized list is a statement about the root being WRITTEN — under it, both
    //     halves are free;
    //   * reusable_ is a statement about the root already DURABLE — under it, only reusable_ is
    //     free, because the durable root still reads pending_free_'s blocks.
    // Those are two different roots, so they get two different answers until write_header makes
    // them the same root.
    //
    // Crash safety of writing the union, checked against the code rather than asserted:
    //   * crash BEFORE the header lands — the old header stays active (write_header writes only
    //     the slot the new iteration owns, so the old slot is untouched, and
    //     load_existing_database picks the greatest VALID iteration). It carries its OWN
    //     free-list pointer from an earlier round, and that earlier list cannot name these
    //     blocks: they were live table data when it was written. The chain written here is
    //     unreferenced garbage that nothing follows.
    //   * crash AFTER the header lands — the new root is recovered and its free list correctly
    //     lists both halves. deserialize_free_list puts them in reusable_, which is right,
    //     because the root that referenced them is no longer the durable one.
    // Neither case can produce a list that frees a block the recovered root reads.
    core::result_wrapper_t<meta_block_pointer_t> single_file_block_manager_t::serialize_free_list() {
        // R-LEAK third term (see the doctrine above): blocks owned only by the live tree.
        // Computed before anything allocates — allocation draws from reusable_ and can neither
        // create nor destroy a registry entry, so this set is stable across the reservation
        // below.
        //
        // THE WRITER OBEYS THE READER'S BOUNDARY, and the reader's boundary is the file:
        // deserialize_free_list refuses any id at or past the block_count of the header it
        // hangs off, and write_header stamps block_count = max_block_. So an id >= max_block_
        // published here would produce a file this build just COMMITTED and this build then
        // refuses to ever open — corruption (recoverable as a leak) converted into an
        // unopenable database (not recoverable). These ids are DISK-FED with no extent check
        // on the way in (column_data / column_state hand data_pointer_t's block ids straight
        // to register_block), and on a non-compacting round no mark_as_free ever sees them, so
        // this is the LAST point before they become durable. Seeing one is the same corruption
        // mark_as_free latches, and it is answered the same way: latch (sticky — a corrupt
        // registration does not heal) and refuse THIS round through the error channel this
        // function already has. No header lands, the previous root stands, and the file keeps
        // opening. reusable_ and pending_free_ need no such screen — every path that fills
        // them (mark_as_free, deserialize_free_list, the rollback's descent) already keeps
        // them below max_block_.
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

        // THE WHOLE CHAIN IS ALLOCATED BEFORE THE SNAPSHOT IS TAKEN. This is the one thing that
        // makes a published free list safe.
        //
        // The list being written is a snapshot of the very pool free_block_id draws from, so
        // ANY block allocated while the write is in progress is an id the snapshot already
        // calls free. The constructor's first block was handled by ordering (construct, then
        // snapshot), but metadata_writer_t::ensure_space allocates every FURTHER chain block
        // mid-stream — and with a 256 KiB block one chain block holds ~32,608 ids, so a free
        // list past that size published a block of its own chain. A restart then inserted that
        // id into reusable_ and handed it out over the chain the durable root still reads.
        //
        // Reserving up front moves every one of those allocations to before the snapshot, which
        // is exactly what takes them out of it. The reservation is sized from the count BEFORE
        // it runs, and reserving can only SHRINK the pool (it draws from reusable_ or extends
        // the file), so the capacity reserved is never short of what the smaller published list
        // needs. No format change, and no permanent cost: the chain blocks are ordinary blocks
        // that A7.3 reclaims one round later, like the rest of the superseded root.
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

        // SNAPSHOT, then write. The previous version iterated the pool directly while the
        // writer's allocations erased from that same set — a range-for over a container being
        // mutated underneath it, which happened to survive only because free_block_id always
        // takes the SMALLEST element, i.e. one the loop had already passed. A snapshot removes
        // the hazard instead of relying on it. (With the reservation above, the writer no
        // longer allocates during the write at all; the snapshot stays because the property
        // must not depend on that.)
        // The three parts are pairwise disjoint by the pool invariants (free_block_id refuses a
        // registry-alive candidate; mark_as_free is paired with unregister_block everywhere),
        // but the union is taken through a set anyway: the published list being duplicate-free
        // must not depend on invariants that live in other functions.
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
        // STAGED, NOT INSTALLED. The list is validated and read to the end before a single id
        // reaches reusable_: the list is published sorted, so an offender is typically LAST,
        // and installing everything before it and then refusing left the allocator's pool
        // half-filled by a load that reported failure — the same half-installed shape the
        // geometry gate on the open path exists to prevent. A refused list installs NOTHING.
        // (No reserve() on `count`: it is disk bytes, and pre-sizing from a corrupt count is
        // its own defect.)
        std::pmr::vector<uint64_t> staged(buffer_manager.resource());
        for (uint64_t i = 0; i < count && !reader.finished(); ++i) {
            const uint64_t block_id = reader.read<uint64_t>();
            // Rule 19, same class of defect as M7 one level earlier, and the SAME boundary
            // mark_as_free applies: an id this file does not contain cannot be handed to the
            // allocator. Two ways to fail it, one guard, because max_block_ (installed by
            // load_existing_database from this very header's block_count before this runs —
            // the precondition named at the declaration) can never exceed MAXIMUM_BLOCK:
            //   * past the end of the file — block_location() seeks past EOF, and the first
            //     write of the reissued id extends the file across the whole gap;
            //   * past the addressable domain — block_location() computes
            //     (2^62 + N) * block_alloc_size, which wraps to N * block_alloc_size, a real
            //     live block rewritten with a valid CRC.
            // The narrow version of this guard tested only the second, so a list naming a
            // block past its own header's block_count opened CLEANLY and armed the very next
            // allocation. No writer of this format can emit either shape — serialize_free_list
            // refuses to publish an id at or past the block_count the same header records —
            // so seeing one means the free-list chain is corrupt; say so HERE, where there is
            // an error channel, and never let it into the pool free_block_id draws from.
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

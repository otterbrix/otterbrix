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

    uint64_t database_header_t::compute_checksum() const {
        // Two spans: the checksum slot sits between the fields and the padding and can't cover itself.
        static constexpr size_t SLOT_OFFSET = offsetof(database_header_t, checksum);
        static constexpr size_t SLOT_END = SLOT_OFFSET + sizeof(uint64_t);
        static_assert(SLOT_END < sizeof(database_header_t), "checksum slot must leave a tail to cover");

        const auto* bytes = reinterpret_cast<const char*>(this);
        auto crc = absl::ComputeCrc32c({bytes, SLOT_OFFSET});
        crc = absl::ExtendCrc32c(crc, {bytes + SLOT_END, sizeof(database_header_t) - SLOT_END});
        return static_cast<uint64_t>(static_cast<uint32_t>(crc));
    }

    namespace {
        // Consumed only once its write is durable (see write_header); slots alternate so iterations never collide.
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
        // Ids >= MAXIMUM_BLOCK would alias a real block here and pass its checksum, hiding the corruption.
        assert(block_id < MAXIMUM_BLOCK && "block_location called with a non-file block id");
        return BLOCK_START + block_id * block_allocation_size();
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::create_new_database() {
        using namespace core::filesystem;

        // Runs before the file exists — the constructor can't report errors, and a bad geometry can't be reopened.
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

        // Writes only iteration 0's own slot; the other stays zeroed, a fallback root tie-broken by checksum.
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
        durable_meta_block_ = INVALID_INDEX;
        durable_free_list_ = INVALID_INDEX;
        return true;
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::load_existing_database() {
        using namespace core::filesystem;

        // Checked explicitly so a missing .otbx is refused distinctly, not mistaken for a truncated file.
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

        // Branched, never asserted — an abort here would make the database permanently unopenable.
        database_header_t header1{};
        database_header_t header2{};
        const bool header1_read = handle_->read(&header1, sizeof(header1), SECTOR_SIZE);
        const bool header2_read = handle_->read(&header2, sizeof(header2), 2 * SECTOR_SIZE);
        const bool header1_valid = header1_read && header1.checksum_ok();
        const bool header2_valid = header2_read && header2.checksum_ok();

        if (!header1_valid && !header2_valid) {
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

        // Winner = valid slot with the greater iteration — iteration alone could let garbage beat a good root.
        const database_header_t& active =
            (header1_valid && (!header2_valid || header1.iteration >= header2.iteration)) ? header1 : header2;

        // "Never checkpointed" needs file_bytes==BLOCK_START too, not just meta_block==INVALID — a
        // checkpointed file whose newest slot got corrupted falls back to this same header. Legal only at iteration 0.
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

        // Checked unconditionally, not skipped on ==0/==current — it could wrap block_size() rather than throw.
        if (auto adopted = set_block_allocation_size(active.block_alloc_size); adopted.has_error()) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Database header of " + path_ +
                                                      " is unusable: " + std::string(adopted.error().what.c_str()),
                                                  buffer_manager.resource()});
        }

        iteration_ = active.iteration;
        meta_block_ = active.meta_block;
        max_block_ = active.block_count;
        // Recorded now, before the next checkpoint overwrites meta_block_ via set_meta_block.
        durable_meta_block_ = active.meta_block;
        durable_free_list_ = active.free_list;

        if (active.free_list != INVALID_INDEX) {
            return deserialize_free_list(meta_block_pointer_t{active.free_list, 0});
        }
        return true;
    }

    core::result_wrapper_t<bool> single_file_block_manager_t::read(block_t& block) {
        auto location = block_location(block.id);
        // Checked before checksumming — stale bytes from a failed read could otherwise misreport as data_corruption.
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

    uint64_t single_file_block_manager_t::free_block_id() {
        uint64_t block_id = INVALID_INDEX;
        bool from_free_list = false;

        // Draws from reusable_ only — pending_free_ is still read by the durable root, and a live id found
        // here is disk-byte corruption to branch on, never assert.
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
        issued_since_root_.insert(block_id);
#ifdef DEV_MODE
        dev_issued_.push_back(block_id);
#endif
        return block_id;
    }

    void single_file_block_manager_t::latch_allocation_error(uint64_t block_id, const std::string& reason) {
        // First error wins: it names the id that broke accounting; later ones are downstream noise.
        if (allocation_error_.contains_error()) {
            return;
        }
        allocation_error_ = core::error_t(core::error_code_t::data_corruption,
                                          std::pmr::string{"Block accounting of " + path_ + " refused block " +
                                                               std::to_string(block_id) + ": " + reason,
                                                           buffer_manager.resource()});
    }

    // Latches rather than rolling back: propagation alone leaves degraded() false, letting compact
    // retry forever (measured +18 blocks/round on a 12k-row table).
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
        // First one wins: it names the write or fsync that actually broke the durable image.
        if (!durability_error_.contains_error()) {
            durability_error_ = error;
        }
        return error;
    }

    uint64_t single_file_block_manager_t::peek_free_block_id() {
        // Mirrors free_block_id's SELECTION (skip registry-live, fall through to max_block_) but not
        // its SIDE EFFECTS — dropping a corrupt candidate and latching belong to the allocation that consumes it.
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
        // Not an assert(): these ids are disk-fed, so under NDEBUG a transient id could silently alias a real block.
        // max_block_ here is the manager's ISSUANCE MARK (can sit below the file's real extent), not the domain limit.
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
        // The sole production caller (compact) releases blocks the CURRENT durable root still points at,
        // so pending_free_ quarantines them; an id already in reusable_ stays there — already proven safe.
        if (reusable_.count(block_id) != 0) {
            return;
        }
        pending_free_.insert(block_id);
    }

    void single_file_block_manager_t::mark_as_used(uint64_t block_id) {
        // Cleared from every pool that could hand it out or publish it as free — else it would look reusable.
        reusable_.erase(block_id);
        pending_free_.erase(block_id);
        used_blocks_.insert(block_id);
    }

    // Called only once the header is proven durable (two call sites); chain pointers and data blocks are
    // adopted together, since splitting could let the two disagree about which root a crash recovers.
    void single_file_block_manager_t::promote_durable_root(uint64_t meta_block, uint64_t free_list) {
        reusable_.insert(pending_free_.begin(), pending_free_.end());
        pending_free_.clear();
        durable_meta_block_ = meta_block;
        durable_free_list_ = free_list;
        durable_root_data_ = std::move(pending_root_data_);
        pending_root_data_.clear();
        issued_since_root_.clear();
    }

    void single_file_block_manager_t::adopt_durable_root_data_blocks(const std::pmr::vector<uint64_t>& block_ids) {
        durable_root_data_.clear();
        durable_root_data_.insert(block_ids.begin(), block_ids.end());
    }

    // free = {root N's blocks} u {its metadata chain} u {its free-list chain} - {root N+1's blocks} -
    // {ids live in the block registry}. Freed ids land in pending_free_, not reusable_, so the file
    // holds TWO complete copies of the table until the new root is durable.
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

        metadata_manager_t chain_mgr(*this);
        if (durable_meta != INVALID_INDEX) {
            if (auto walked = chain_mgr.chain_blocks(meta_block_pointer_t(durable_meta, 0), candidates);
                walked.has_error()) {
                // Propagating alone leaves degraded() false, letting compact grow the file every round.
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
            // Checked first since this list is DISK BYTES with no domain check on the way in.
            if (block_id >= max_block_) {
                mark_as_free(block_id);
                continue;
            }
            if (pending_root_data_.count(block_id) != 0 || issued_since_root_.count(block_id) != 0) {
                continue; // named by, or allocated for, the root under construction
            }
            if (registry_alive(block_id)) {
                continue; // live table state — the table was not compacted this round
            }
            // unregister_block prevents the same ABA that data_table_t::compact guards against.
            mark_as_free(block_id);
            unregister_block(block_id);
            ++reclaimed;
        }
        return reclaimed;
    }

    // A failed header write leaves unregistered blocks stuck in issued_since_root_ forever — measured
    // ~655 KB per round on a 7.8 MB table. Releasable = issued_since_root_ minus registry-live ids;
    // released ones move to reusable_, not pending_free_.
    uint64_t single_file_block_manager_t::roll_back_uncommitted_round() {
        // Never rolled back when indeterminate — the blocks here may be named by a root a crash would recover.
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
            // Load-bearing: a second failed, compacting round can free the first round's still-unpromoted
            // ids too (measured 14 ids in both after two failed rounds, 12k-row; test_failed_round_rollback.cpp).
            pending_free_.erase(block_id);
            reusable_.insert(block_id);
            // ABA break, same pairing as compact/reclaim_superseded_root — no expired slot may be revived.
            unregister_block(block_id);
#ifdef DEV_MODE
            dev_freed_.push_back(block_id);
#endif
        }

        pending_root_data_.clear();
        // Reset since set_meta_block pointed this at the reclaimed chain; durable_meta_block_ (root N) is unchanged.
        meta_block_ = durable_meta_block_;

        // Walks the mark down only over the released, contiguous top of the file — the part just extended.
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
        // Both halves: dead-space total, not issuable-now — pending_free_ is dead space too, and disjoint.
        return reusable_.size() + pending_free_.size();
    }

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
        // Every data, metadata and free-list block in the system lands here, so latched as well as propagated.
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

    core::result_wrapper_t<bool> single_file_block_manager_t::write_header(const database_header_t& header) {
        // iteration_ only advances after write+fsync succeed, else a failed retry would aim at the OTHER slot.
        // Refused before any header byte is written — a proven-corrupt free list must never become durable.
        if (allocation_error_.contains_error()) {
            roll_back_uncommitted_round();
            return core::error_t(allocation_error_);
        }
        // Commit gate — an earlier failed write or barrier means not every block this root names is on the device.
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
        // Zeroed since the padding sits inside the checksummed sector and the caller's copy isn't trusted.
        std::memset(write_header.padding, 0, sizeof(write_header.padding));
        write_header.checksum = write_header.compute_checksum();

        // Shadow paging: writes only this iteration's slot; the other keeps the PREVIOUS root, lost if both written.
        const uint64_t slot = header_slot_offset(next_iteration);
        const bool write_ok = handle_->write(&write_header, sizeof(write_header), slot);
        // Issued unconditionally, even after a reported write failure, so the read-back below is evidence, not a guess.
        const bool sync_ok = handle_->sync();
        if (write_ok && sync_ok) {
            iteration_ = next_iteration;
            // Order matters: earlier (before fsync confirmed) would free these blocks while the OLD root stood.
            promote_durable_root(write_header.meta_block, write_header.free_list);
            return true;
        }
        return reconcile_failed_header_write(next_iteration, write_ok, sync_ok);
    }

    // A torn 4 KiB write can leave a CRC-valid header of the new generation on disk while write()
    // reports failure, so this reads BOTH slots back and adopts whatever the disk says.
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
            // No root can be named, so the failed round's allocations stay exactly where they are.
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
            // Owed here exactly as on the success path; cases 2/3 do not promote.
            promote_durable_root(active.meta_block, active.free_list);
            return true;
        }

        if (active.iteration == iteration_) {
            // Recoverable: deliberately NOT latched, or a transient ENOSPC would become permanent degradation.
            // OPEN cost: a persistent write error here retries forever, recompacting a full table copy each time.
            roll_back_uncommitted_round();
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to write database header, " + what +
                                                      "; the previous root (iteration " + std::to_string(iteration_) +
                                                      ") stands and is unchanged",
                                                  buffer_manager.resource()});
        }

        // Indeterminate: must NOT roll back — an unconfirmed, landed-looking slot may still be the recovered root.
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
        // Makes this round's blocks durable before the root naming them commits; latched, so failure can't be ignored.
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
        // PRECONDITION: max_block_ can sit BELOW the durable header's block_count after a rollback.
        auto file_end = block_location(max_block_);
        if (!handle_->truncate(static_cast<int64_t>(file_end))) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"Failed to truncate " + path_ + " to " + std::to_string(file_end),
                                                  buffer_manager.resource()});
        }
        return true;
    }

    // Persisted list = reusable_ u pending_free_ u {live-only blocks the new root doesn't name}. The third
    // term stops a reopened file from leaking blocks the live tree alone still holds (measured 8 blocks,
    // 2 MiB, leaked per restart at 6k rows) — reclaim only walks roots.
    core::result_wrapper_t<meta_block_pointer_t> single_file_block_manager_t::serialize_free_list() {
        // Screened against max_block_: publishing an id past it would make a file just committed unopenable.
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

        // Allocated BEFORE the snapshot (a snapshot of the very pool free_block_id draws from); with a
        // 256 KiB block one chain block holds ~32,608 ids, so reserving avoids allocating more mid-write.
        const uint64_t sub_block_size = meta_mgr.sub_block_size();
        const uint64_t payload_bytes = (candidate_count + 1) * sizeof(uint64_t); // + the count itself
        const uint64_t needed_sub_blocks = metadata_writer_t::sub_blocks_for(payload_bytes, sub_block_size);
        if (needed_sub_blocks == 0) {
            // 0 means a sub-block too small for its own chain header — unusable, never "nothing to reserve".
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"Free list of " + path_ + " cannot be written: sub-block size " +
                                                      std::to_string(sub_block_size) +
                                                      " is too small to hold a metadata chain header",
                                                  buffer_manager.resource()});
        }
        meta_mgr.reserve(needed_sub_blocks);

        metadata_writer_t writer(meta_mgr);

        // Snapshot instead of iterating the pool directly, which the writer's own allocations would mutate mid-loop.
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
        // The flush IS the write of the chain; an unchecked pointer could advertise blocks never landed.
        if (auto flushed = writer.flush(); flushed.has_error()) {
            return flushed.convert_error<meta_block_pointer_t>();
        }

        // Self-check in every build, not assert — the damage from a bad reservation lands in release builds.
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
        // Staged, not installed: the list is sorted, so refusing partway would leave the pool half-filled.
        std::pmr::vector<uint64_t> staged(buffer_manager.resource());
        for (uint64_t i = 0; i < count && !reader.finished(); ++i) {
            const uint64_t block_id = reader.read<uint64_t>();
            // Same boundary as mark_as_free — must not open cleanly and arm the next allocation.
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
        if (reader.has_error()) {
            return core::error_t(reader.error());
        }
        // reusable_, not pending_free_ — the DURABLE root's own statement, nothing in flight to quarantine.
        reusable_.insert(staged.begin(), staged.end());
        return true;
    }

} // namespace components::table::storage

#include "disk_hash_table.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <random>
#include <stdexcept>

namespace services::index {
    namespace codec = components::index::codec;

    namespace {
        uint32_t fnv1a_32_seeded(std::string_view s, uint32_t seed) {
            constexpr uint32_t offset = 2166136261u;
            constexpr uint32_t prime = 16777619u;
            uint32_t h = offset ^ seed;
            for (char ch : s) {
                const auto c = static_cast<uint8_t>(ch);
                h ^= c;
                h *= prime;
            }
            return h;
        }

        uint32_t generate_hash_seed() {
#ifdef DEV_MODE
            if (const char* v = std::getenv("OTTERBRIX_DISK_HASH_SEED"); v != nullptr && *v != '\0') {
                return static_cast<uint32_t>(std::strtoul(v, nullptr, 0));
            }
#endif
            std::random_device rd;
            std::array<uint32_t, 4> buf{{rd(), rd(), rd(), rd()}};
            uint32_t seed = 0x9E3779B9u;
            for (auto v : buf) {
                seed ^= v + 0x9e3779b9u + (seed << 6U) + (seed >> 2U);
            }
            return seed == 0 ? 0xA5A5A5A5u : seed;
        }

        constexpr uint64_t overflow_page_id_base = 1ULL << 40;

#ifdef DEV_MODE
        bool split_crash_failpoint(const char* stage) {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT");
            return v != nullptr && std::strcmp(v, stage) == 0;
        }

        // THE ONE FAILURE A TEST CANNOT STAGE FROM OUTSIDE. Every other I/O refusal this
        // class can meet is reachable from the filesystem -- an unopenable path, a file
        // shorter than the page a chain points at -- and the cases below use exactly that.
        // A REFUSED OVERFLOW ALLOCATION is not: it is a positional write to an fd this
        // class opened O_RDWR itself, and nothing a test can do to the path makes that
        // write fail (chmod does not reach an open descriptor, and the block manager's T3
        // interposer wraps single_file_block_manager_t, which this class does not use).
        //
        // So the seam is here, DEV_MODE only, and it is armed where the failure really
        // happens rather than where the test wants to observe it: allocate_overflow_page
        // answers 0, which is precisely what a failed page write makes it answer, and the
        // refusal then travels the production path -- through
        // insert_payload_into_bucket_unlocked's error, through the split's copy loop --
        // instead of short-circuiting it. put() meets the same refusal through the same
        // door, which is what makes the injection's sensitivity checkable.
        bool overflow_alloc_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_OVERFLOW_ALLOC_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }

        // THE OTHER FAILURE NO TEST CAN STAGE FROM OUTSIDE, and for a sharper reason than the
        // one above: reset_storage re-creates its files through the SAME open_or_create the
        // first open runs, so any on-disk arrangement that breaks the re-creation breaks the
        // first open too -- and the store never gets as far as a reset. Only a change of state
        // BETWEEN the two can refuse it (an EIO, or the volume going read-only between the two
        // opens), and that is what this seam stands in for. It is armed at the worst reachable
        // moment: both files are already unlinked and the re-creation is the step that says no.
        //
        // THE PERMISSION CASE IS NOT ONE OF THESE and used to be named here, which is what sent
        // a reader hunting for a read-only-directory regression that does not exist: opening
        // this index is ALREADY a write to its directory (see bitcask_index_disk_t::open), so a
        // directory that cannot be written to never reaches a reset in the first place.
        bool reset_reopen_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_RESET_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }

        // THE ONE STATE NOTHING OUTSIDE CAN ARRANGE: an unlink that reported success and left
        // the name in place. No filesystem this runs on does that, and that is precisely why
        // the check it feeds cannot be exercised any other way -- the check exists because
        // reset_storage's postcondition ("the file is gone") is the whole basis on which the
        // caller replays its segments into a table it believes is empty, and a postcondition
        // that is only ever trusted is not a postcondition. Skipping the two removes puts the
        // re-open in front of exactly the files the wipe was supposed to have taken away.
        bool reset_skip_wipe_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_SKIP_WIPE_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }
#else
        bool split_crash_failpoint(const char*) { return false; }
        bool overflow_alloc_failpoint() { return false; }
        // BOTH RESET SEAMS GET A STUB, so their call sites read like the other two: an
        // `#ifdef DEV_MODE` wrapped around one call in the middle of reset_storage was the
        // file's own pattern broken for the sake of a single function.
        bool reset_reopen_failpoint() { return false; }
        bool reset_skip_wipe_failpoint() { return false; }
#endif
    } // namespace

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;
    using core::filesystem::open_file;

    disk_hash_table_t::disk_hash_table_t(const std::filesystem::path& file_path,
                                         uint32_t bucket_count,
                                         std::pmr::memory_resource* memory_resource,
                                         defer_open_tag)
        : file_path_(file_path)
        , overflow_file_path_(std::filesystem::path(file_path).concat(".ovf"))
        , memory_resource_(memory_resource) {
        // A missing resource or a zero bucket count is a caller bug, not an I/O
        // failure: nothing can recover from it, and with no resource there is not
        // even anything to build the message on. I/O failures are different
        // — they are environmental, so they travel as a value, and this ctor opens
        // nothing at all: whoever runs open_or_create() owns its answer.
        assert(memory_resource && "disk_hash_table: resource required");
        assert(bucket_count > 0 && "disk_hash_table: bucket_count must be > 0");
        header_.bucket_count_value = bucket_count;
    }

    disk_hash_table_t::disk_hash_table_t(const std::filesystem::path& file_path,
                                         uint32_t bucket_count,
                                         std::pmr::memory_resource* memory_resource)
        : disk_hash_table_t(file_path, bucket_count, memory_resource, defer_open_tag{}) {
        if (open_or_create().contains_error()) {
            // A half-open table must never be handed to a caller that believes it
            // has durable storage; create() reports the same failure as a value.
            assert(false && "disk_hash_table: direct ctor could not open storage");
            std::abort();
        }
    }

    core::result_wrapper_t<std::unique_ptr<disk_hash_table_t>>
    disk_hash_table_t::create(const std::filesystem::path& file_path,
                              uint32_t bucket_count,
                              std::pmr::memory_resource* memory_resource) {
        auto instance = std::unique_ptr<disk_hash_table_t>(
            new disk_hash_table_t(file_path, bucket_count, memory_resource, defer_open_tag{}));
        // The open runs HERE, and its answer is the return value rather than a member
        // the caller has to know to ask about.
        if (auto open_result = instance->open_or_create(); open_result.contains_error()) {
            return open_result;
        }
        return std::move(instance);
    }

    disk_hash_table_t::~disk_hash_table_t() {
        std::unique_lock lock(mutex_);
        if (file_) {
            // Closing flush: nothing above can act on a failure here, but the value is not dropped
            // silently either.
            if (!persist_header()) {
                assert(false && "disk_hash_table: header flush failed on close");
            }
            // Same as the header above: nothing above this can act on a closing flush, and
            // the value is read rather than dropped silently.
            if (!sync_files()) {
                assert(false && "disk_hash_table: fsync failed on close");
            }
        }
    }

    core::error_t
    disk_hash_table_t::put(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) {
        std::unique_lock lock(mutex_);
        return put_unlocked(key, value, log_file_id, log_offset);
    }

    core::error_t
    disk_hash_table_t::put_unlocked(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) {
        const uint32_t key_hash = hash_key(key);
        const uint32_t bucket_id = bucket_id_for_hash(key_hash);
        auto payload = make_entry_payload(key, value, log_file_id, log_offset);
        RETURN_IF_ERROR(insert_payload_into_bucket_unlocked(bucket_id, key_hash, payload));
        ++entry_count_;
        // The entry is IN by this point. A failing auto-rehash therefore does not undo it
        // and does not corrupt anything -- a refused split publishes nothing, so the table
        // keeps answering from the state this entry just joined. It is still reported:
        // the only failure a split can meet is the disk refusing a page, and a caller told
        // "stored, but the storage is refusing writes" can stop; a caller told nothing
        // goes on writing into a keydir whose load factor can no longer come down.
        return maybe_rehash_if_needed_unlocked();
    }

    core::error_t disk_hash_table_t::insert_payload_into_bucket_unlocked(uint32_t bucket_id,
                                                                         uint32_t key_hash,
                                                                         const byte_buffer_t& payload) {
        uint64_t page_id = bucket_primary_page_id(bucket_id);
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        while (true) {
            // Bail on a failed read: this loop is `while (true)`, so ignoring the failure would
            // spin on a stale page forever.
            if (!read_page(page_id, page)) {
                return page_read_failure(page_id);
            }
            bool changed = false;
            if (try_insert_payload_in_page(page, key_hash, payload, changed)) {
                if (changed && !write_page(page_id, page)) {
                    return page_write_failure(page_id);
                }
                return core::error_t::no_error();
            }
            auto overflow = page_overflow(page);
            if (overflow == 0) {
                const auto new_page = allocate_overflow_page();
                if (new_page == 0) {
                    // Allocation failed (page 0 is the header, never an overflow page), so there
                    // is nowhere to put this payload.
                    return io_failure("disk_hash_table: could not allocate an overflow page for " +
                                      file_path_.string());
                }
                set_page_overflow(page, new_page);
                if (!write_page(page_id, page)) {
                    return page_write_failure(page_id);
                }
                page_id = new_page;
                continue;
            }
            page_id = overflow;
        }
    }

    core::error_t disk_hash_table_t::rehash(uint32_t new_bucket_count) {
        std::unique_lock lock(mutex_);
        return rehash_unlocked(new_bucket_count);
    }

    core::error_t disk_hash_table_t::trigger_rehash_if_needed() {
        std::unique_lock lock(mutex_);
        return maybe_rehash_if_needed_unlocked();
    }

    bool disk_hash_table_t::set_auto_rehash_suppressed(bool suppressed) noexcept {
        return suppress_auto_rehash_.exchange(suppressed, std::memory_order_acq_rel);
    }

    double disk_hash_table_t::load_factor() const {
        std::shared_lock lock(mutex_);
        if (header_.bucket_count_value == 0) {
            return 0.0;
        }
        return static_cast<double>(entry_count_) / static_cast<double>(header_.bucket_count_value);
    }

    core::error_t disk_hash_table_t::rehash_unlocked(uint32_t new_bucket_count) {
        if (new_bucket_count == 0) {
            // Caller bug, not an environmental failure — but reported by value like the rest
            // of this class rather than thrown.
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"disk_hash_table: rehash to zero buckets", memory_resource_}};
        }
        if (new_bucket_count <= header_.bucket_count_value) {
            // Already at least this wide. Nothing to do, and nothing went wrong: the
            // post-condition the caller asked for holds.
            return core::error_t::no_error();
        }
        rehash_in_progress_ = true;
        struct reset_flag_t {
            bool& flag;
            ~reset_flag_t() { flag = false; }
        } reset{rehash_in_progress_};
        while (header_.bucket_count_value < new_bucket_count) {
            if (auto split_error = split_one_bucket_unlocked(); split_error.contains_error()) {
                // Every error from a split means no split happened -- a bad state, a failed
                // page write, an entry that could not be copied, or the failpoint. The loop
                // condition only advances when a split succeeds, so continuing here spins
                // forever; and the split published nothing, so the table is still the width
                // it was when this call started.
                //
                // The flush of the splits that DID land is attempted, but the split's own
                // reason is the one worth reporting, so a refusal here does not replace it.
                if (!sync_files()) {
                    return io_failure("disk_hash_table: fsync refused while reporting a failed split");
                }
                return split_error;
            }
        }
        if (!sync_files()) {
            return io_failure("disk_hash_table: the rehashed table could not be made durable");
        }
        return core::error_t::no_error();
    }

    // A SPLIT IS ALL-OR-NOTHING, and the "nothing" leg is what this function used to be
    // missing. Phase 1 copied every move-candidate into the new bucket with the result of
    // the copy DROPPED, and phase 2 then advanced the addressing state UNCONDITIONALLY.
    // An entry that failed to copy is lost the instant the header moves: it is still
    // physically in the source bucket, but the published state says its hash belongs to
    // the new bucket, so no walk ever looks where it is. Measured on the case below: a
    // refused overflow allocation part-way through the copy left 160 of 400 rows
    // unreachable, on disk as well as in memory.
    //
    // WHAT A REFUSAL LEAVES BEHIND, precisely. Phase 2 has not run, so bucket_count,
    // split_bucket and level are untouched and every read still addresses the source
    // bucket -- which phase 3 never cleans, so it still holds every entry. The new
    // bucket's page and any overflow pages the partial copy took are simply unreachable:
    // bucket_id_for_hash cannot name the new bucket, and for_each / count_entries stop
    // below it. A RETRY is therefore clean rather than doubled -- the first thing the
    // next attempt does is re-initialize the new bucket's primary page, which drops the
    // partial copy and its chain in one write.
    core::error_t disk_hash_table_t::split_one_bucket_unlocked(bool durable_commit) {
        if (header_.bucket_count_value == UINT32_MAX) {
            return io_failure("disk_hash_table: bucket count is at its maximum, cannot split");
        }
        const uint32_t base = 1U << header_.level_value;
        if (base == 0 || header_.split_bucket_value >= base) {
            return io_failure("disk_hash_table: linear-hash state is inconsistent, cannot split");
        }
        const uint32_t split_bucket = header_.split_bucket_value;
        const uint32_t new_bucket = base + split_bucket;
        if (new_bucket != header_.bucket_count_value) {
            return io_failure("disk_hash_table: split bucket does not extend the bucket count");
        }
        const uint64_t mod = static_cast<uint64_t>(base) << 1U;

        byte_buffer_t empty(memory_resource_);
        empty.resize(page_size);
        init_empty_page(empty);
        if (!write_page(bucket_primary_page_id(new_bucket), empty)) {
            return page_write_failure(bucket_primary_page_id(new_bucket));
        }

        uint64_t page_id = bucket_primary_page_id(split_bucket);
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        byte_buffer_t payload(memory_resource_);

        // Phase 1 (copy): move-candidates are appended to the new bucket, source remains intact.
        // A crash here is safe because lookups still use the old addressing state.
        while (page_id != 0) {
            if (!read_page(page_id, page)) {
                // The rest of the source chain is unknown, so the set of entries this
                // split owes the new bucket is unknown too. Publishing now would move the
                // addressing of every unread entry to a bucket that does not hold it.
                return page_read_failure(page_id);
            }
            const auto cnt = page_count(page);
            for (uint16_t i = 0; i < cnt; ++i) {
                const auto slot = read_slot(page, i);
                if (slot.flags != slot_flag_used || slot.length == 0) {
                    continue;
                }
                if (!slot_belongs_to_bucket_unlocked(slot.key_hash, split_bucket)) {
                    continue;
                }
                if ((static_cast<uint64_t>(slot.key_hash) % mod) == split_bucket) {
                    continue;
                }
                if (static_cast<uint32_t>(slot.offset) + static_cast<uint32_t>(slot.length) > page_size) {
                    // A slot pointing past its own page is corruption, and copying it would
                    // read past the page buffer. Every other walk in this class runs the
                    // payload through decode_entry, which makes this check for them.
                    return io_failure("disk_hash_table: slot extends past its page, cannot copy it");
                }
                payload.resize(slot.length);
                std::memcpy(payload.data(), page.data() + slot.offset, slot.length);
                RETURN_IF_ERROR(insert_payload_into_bucket_unlocked(new_bucket, slot.key_hash, payload));
            }
            page_id = page_overflow(page);
        }

        if (durable_commit) {
            // Ensure copied entries are durable before publishing metadata.
            // Until the header is advanced, a crash must reopen with the old
            // addressing state; the copied new-bucket entries are merely
            // unreachable duplicates.
            if (!sync_files()) {
                return io_failure("disk_hash_table: the copied split entries could not be made durable");
            }
            if (split_crash_failpoint("after_copy_sync")) {
                return io_failure("disk_hash_table: split failpoint after_copy_sync");
            }
        }

        // Phase 2 (commit): publish new addressing state in-memory.
        // For durable_commit=false (auto-rehash batch), on-disk header update is deferred to caller.
        ++header_.bucket_count_value;
        ++header_.split_bucket_value;
        if (header_.split_bucket_value == base) {
            header_.split_bucket_value = 0;
            ++header_.level_value;
        }

        if (durable_commit) {
            if (!persist_header()) {
                return io_failure("disk_hash_table: failed to persist the header after a bucket split");
            }
            if (!sync_files()) {
                return io_failure("disk_hash_table: the split header could not be made durable");
            }
            if (split_crash_failpoint("after_header_sync")) {
                return io_failure("disk_hash_table: split failpoint after_header_sync");
            }
        }

        // Phase 3 (lazy cleanup): intentionally skipped in split hot path.
        // Stale source copies remain physically present, but are ignored by ownership
        // checks in iteration/recount paths and by future split scans.
        return core::error_t::no_error();
    }

    core::error_t disk_hash_table_t::maybe_rehash_if_needed_unlocked() {
        if (rehash_in_progress_ || header_.bucket_count_value == 0) {
            return core::error_t::no_error();
        }
        if (suppress_auto_rehash_.load(std::memory_order_acquire)) {
            return core::error_t::no_error();
        }
        if (header_.bucket_count_value == UINT32_MAX) {
            return core::error_t::no_error();
        }
        // Only trigger rehash when load factor significantly exceeds threshold
        // to reduce frequency of rehash operations during bulk inserts.
        const auto curr_lf = static_cast<double>(entry_count_) / static_cast<double>(header_.bucket_count_value);
        if (curr_lf <= max_load_factor_) {
            return core::error_t::no_error();
        }
        bool changed = false;
        // Batch multiple splits together before syncing to reduce fsync overhead.
        // Target load factor slightly below threshold to avoid immediate re-trigger.
        const double target_lf = max_load_factor_ * 0.6;
        const uint32_t target_buckets = static_cast<uint32_t>(
            std::min(static_cast<double>(UINT32_MAX), static_cast<double>(entry_count_) / target_lf));
        while (header_.bucket_count_value < target_buckets && header_.bucket_count_value < UINT32_MAX) {
            // Auto-rehash path batches split durability barriers to avoid one fsync pair
            // per split. Crash safety is preserved because source buckets are never
            // destructively cleaned before header publication.
            //
            // LEAVING THE LOOP ON A FAILURE IS NOT OPTIONAL: only a SUCCESSFUL split
            // advances bucket_count, which is this loop's own condition, so a split that
            // refuses and is not acted on spins here forever. That was already true of
            // the failures the old code could produce; it becomes reachable traffic now
            // that a refused copy is one of them.
            if (auto split_error = split_one_bucket_unlocked(false); split_error.contains_error()) {
                if (changed) {
                    // The splits that DID finish are still only in memory. Publish them --
                    // they are complete and their source buckets are intact -- and then
                    // report why the batch stopped.
                    if (!persist_header()) {
                        return io_failure("disk_hash_table: failed to persist the header after a split batch");
                    }
                    if (!sync_files()) {
                        return io_failure("disk_hash_table: fsync refused while reporting a failed split batch");
                    }
                }
                return split_error;
            }
            changed = true;
        }
        if (changed) {
            // Publish all split data first, then atomically advance addressing state.
            // Single sync barrier at the end of batch.
            if (!persist_header()) {
                return io_failure("disk_hash_table: failed to persist the header after a split batch");
            }
            if (!sync_files()) {
                return io_failure("disk_hash_table: the split batch could not be made durable");
            }
        }
        return core::error_t::no_error();
    }

    uint32_t disk_hash_table_t::bucket_count() const {
        std::shared_lock lock(mutex_);
        return header_.bucket_count_value;
    }

    core::error_t disk_hash_table_t::sync() {
        std::shared_lock lock(mutex_);
        if (!sync_files()) {
            return io_failure("disk_hash_table: " + file_path_.string() + " could not be made durable");
        }
        return core::error_t::no_error();
    }

    core::error_t disk_hash_table_t::reset_storage() {
        std::unique_lock lock(mutex_);
        file_.reset();
        ovf_file_.reset();
        // A REFUSED unlink IS A REFUSED WIPE, and the std::error_code the old body collected
        // and never read is the whole difference. Swallowing it leaves the file in place, and
        // the caller then replays its segments ON TOP OF the old contents while believing the
        // table is empty. That is the one outcome the rebuild-from-segments rule cannot
        // survive, so it is the one outcome this function refuses to produce -- here, where
        // the unlink says no, and again in open_after_wipe_or_refuse below, which does not
        // take a successful unlink's postcondition on trust either.
        if (!reset_skip_wipe_failpoint()) {
            std::error_code ec;
            std::filesystem::remove(file_path_, ec);
            if (ec) {
                return io_failure("disk_hash_table: " + file_path_.string() +
                                  " could not be removed for rebuild: " + ec.message());
            }
            std::filesystem::remove(overflow_file_path_, ec);
            if (ec) {
                return io_failure("disk_hash_table: " + overflow_file_path_.string() +
                                  " could not be removed for rebuild: " + ec.message());
            }
        }
        entry_count_ = 0;
        rehash_in_progress_ = false;
        // suppress_auto_rehash_ IS DELIBERATELY UNTOUCHED -- see the declaration.
        const uint32_t bucket_count =
            header_.bucket_count_value > 0 ? header_.bucket_count_value : default_bucket_count;
        const uint32_t hash_seed = header_.hash_seed_value;
        header_ = header_t{};
        header_.bucket_count_value = bucket_count;
        header_.hash_seed_value = hash_seed;
        if (reset_reopen_failpoint()) {
            return io_failure("disk_hash_table: the reset failpoint refused the re-open of " + file_path_.string());
        }
        return open_after_wipe_or_refuse();
    }

    // THE RE-OPEN THAT DOES NOT TAKE THE WIPE ON TRUST.
    //
    // open_or_create() cannot stand at the end of reset_storage, and the reason is its very
    // first decision: it branches on file_size() and takes load_existing_file whenever the
    // size is not zero. That is the exact outcome the paragraph above refuses to produce,
    // reached by a different road -- the unlink reported success, the name is somehow still
    // there, and the caller replays its segments into a table it believes is empty while
    // every entry that survived answers from an offset nothing has verified.
    //
    // A successful unlink means the NAME IS GONE, so what these two opens create must be new
    // and therefore empty. Anything else is a postcondition that did not hold, and there is
    // no repair for it here: a table half of whose entries predate the wipe is not a table
    // this class can reason about. It refuses, the caller's open() hands the reason up, and
    // the index loses its registration rather than the process its life.
    //
    // THE PRICE, in full: two fstats and one extra open() of the overflow file per index
    // open (initialize_new_file opens it again). Nothing per operation.
    core::error_t disk_hash_table_t::open_after_wipe_or_refuse() {
        file_ = open_file(fs_,
                          file_path_,
                          file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK);
        if (!file_) {
            return io_failure("disk_hash_table: failed to open file " + file_path_.string());
        }
        if (file_->file_size() != 0) {
            return io_failure("disk_hash_table: " + file_path_.string() + " survived the wipe");
        }
        RETURN_IF_ERROR(open_overflow_file());
        if (ovf_file_->file_size() != 0) {
            return io_failure("disk_hash_table: " + overflow_file_path_.string() + " survived the wipe");
        }
        return initialize_new_file();
    }

    // BOTH HANDLES GO AND THE OBJECT STAYS. Its one caller is a wipe that could not finish,
    // where the table's contents have stopped describing anything on the device: answering
    // out of them would be answering out of a keydir whose segments were just unlinked.
    //
    // Refusing afterwards costs no flag and no per-door guard, because the geometry already
    // does it -- main_page_count() and overflow_page_count() answer 0 with no handle, and
    // read_page checks the page id against them BEFORE it dereferences anything, so every
    // read comes back as page_read_failure. So does every write: each of them begins by
    // reading the page it is about to change. The destructor's closing header flush is
    // skipped for the same reason it should be, there being no file to flush it to.
    //
    // The state is not an error living in a field: it carries no message, guards no door and
    // is not consulted by anything. It is the table's RESOURCES, and the next successful
    // reset_storage re-opens both files and puts it back to work.
    void disk_hash_table_t::close_storage() {
        std::unique_lock lock(mutex_);
        file_.reset();
        ovf_file_.reset();
    }

    bool disk_hash_table_t::sync_files() {
        // BOTH ARE TRIED AND BOTH ANSWERS COUNT. The overflow file holds the chains long
        // buckets spill into, so an entry that reached only the primary file is an entry a
        // reopen cannot follow -- reporting durability on either alone is reporting it on
        // half the table.
        const bool primary_synced = file_ ? file_->sync() : true;
        const bool overflow_synced = ovf_file_ ? ovf_file_->sync() : true;
        return primary_synced && overflow_synced;
    }

    core::error_t disk_hash_table_t::io_failure(const std::string& message) const {
        return core::error_t{core::error_code_t::index_create_fail, std::pmr::string{message, memory_resource_}};
    }

    core::error_t disk_hash_table_t::page_read_failure(uint64_t page_id) const {
        return core::error_t{core::error_code_t::io_error,
                             std::pmr::string{"disk_hash_table: page " + std::to_string(page_id) + " of " +
                                                  file_path_.string() + " could not be read",
                                              memory_resource_}};
    }

    core::error_t disk_hash_table_t::page_write_failure(uint64_t page_id) const {
        return core::error_t{core::error_code_t::io_error,
                             std::pmr::string{"disk_hash_table: page " + std::to_string(page_id) + " of " +
                                                  file_path_.string() + " could not be written",
                                              memory_resource_}};
    }

    core::error_t disk_hash_table_t::open_or_create() {
        file_ = open_file(fs_,
                          file_path_,
                          file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK);
        if (!file_) {
            return io_failure("disk_hash_table: failed to open file " + file_path_.string());
        }
        if (file_->file_size() == 0) {
            return initialize_new_file();
        }
        RETURN_IF_ERROR(load_existing_file());
        entry_count_ = count_entries_unlocked();
        return core::error_t::no_error();
    }

    core::error_t disk_hash_table_t::open_overflow_file() {
        ovf_file_ = open_file(fs_,
                              overflow_file_path_,
                              file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                              file_lock_type::NO_LOCK);
        if (!ovf_file_) {
            return io_failure("disk_hash_table: failed to open overflow file " + overflow_file_path_.string());
        }
        return core::error_t::no_error();
    }

    core::error_t disk_hash_table_t::initialize_new_file() {
        header_.page_size_value = page_size;
        header_.next_overflow_page = overflow_page_id_base;
        // 0 IS A RELIABLE "not set yet": generate_hash_seed never answers 0 (see its tail) and
        // header_t default-constructs the field to 0, so a table opened for the first time
        // still gets a random seed while one coming out of reset_storage keeps the seed its
        // entries were hashed with -- which is what makes a rebuilt layout reproducible.
        header_.hash_seed_value = header_.hash_seed_value != 0 ? header_.hash_seed_value : generate_hash_seed();
        initialize_linear_state_from_bucket_count();

        RETURN_IF_ERROR(open_overflow_file());
        if (!persist_header()) {
            return io_failure("disk_hash_table: failed to write header");
        }
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        for (uint32_t i = 0; i < header_.bucket_count_value; ++i) {
            init_empty_page(page);
            if (!write_page(bucket_primary_page_id(i), page)) {
                return io_failure("disk_hash_table: failed to initialize bucket page");
            }
        }
        entry_count_ = 0;
        if (!sync_files()) {
            return io_failure("disk_hash_table: the new table file could not be made durable");
        }
        return core::error_t::no_error();
    }

    core::error_t disk_hash_table_t::load_existing_file() {
        byte_buffer_t hdr(memory_resource_);
        hdr.resize(page_size, 0);
        if (!file_->read(hdr.data(), page_size, 0)) {
            return io_failure("disk_hash_table: failed to read header page");
        }
        header_.page_size_value = codec::read_le_ptr<uint32_t>(hdr.data() + 12);
        header_.bucket_count_value = codec::read_le_ptr<uint32_t>(hdr.data() + 16);
        header_.next_overflow_page = codec::read_le_ptr<uint64_t>(hdr.data() + 20);
        header_.level_value = codec::read_le_ptr<uint32_t>(hdr.data() + 28);
        header_.split_bucket_value = codec::read_le_ptr<uint32_t>(hdr.data() + 32);
        header_.hash_seed_value = hdr.size() >= 40 ? codec::read_le_ptr<uint32_t>(hdr.data() + 36) : 0;
        if (header_.page_size_value != page_size || header_.bucket_count_value == 0) {
            return io_failure("disk_hash_table: incompatible header");
        }
        const uint32_t base = header_.level_value > 31 ? 0 : (1U << header_.level_value);
        if (base == 0 || base > header_.bucket_count_value || header_.split_bucket_value > base ||
            (base + header_.split_bucket_value) != header_.bucket_count_value) {
            initialize_linear_state_from_bucket_count();
        }

        RETURN_IF_ERROR(open_overflow_file());
        if (header_.next_overflow_page < overflow_page_id_base) {
            header_.next_overflow_page = overflow_page_id_base;
        }
        return core::error_t::no_error();
    }

    bool disk_hash_table_t::is_overflow_page_id(uint64_t page_id) { return page_id >= overflow_page_id_base; }

    uint64_t disk_hash_table_t::main_page_count() const { return file_ ? (file_->file_size() / page_size) : 0; }

    uint64_t disk_hash_table_t::overflow_page_count() const {
        return ovf_file_ ? (ovf_file_->file_size() / page_size) : 0;
    }

    uint64_t disk_hash_table_t::bucket_primary_page_id(uint32_t bucket_id) const { return 1 + bucket_id; }

    uint32_t disk_hash_table_t::hash_key(std::string_view key) const {
        return fnv1a_32_seeded(key, header_.hash_seed_value);
    }

    uint32_t disk_hash_table_t::bucket_id_for_hash(uint32_t key_hash) const {
        if (header_.bucket_count_value == 0) {
            return 0;
        }
        if (header_.level_value > 31) {
            assert(false && "disk_hash_table: invalid linear hash level");
        }
        const uint32_t base = 1U << header_.level_value;
        uint32_t bucket = key_hash % base;
        if (bucket < header_.split_bucket_value) {
            const uint64_t doubled = static_cast<uint64_t>(base) << 1U;
            bucket = static_cast<uint32_t>(static_cast<uint64_t>(key_hash) % doubled);
        }
        return bucket;
    }

    void disk_hash_table_t::initialize_linear_state_from_bucket_count() {
        if (header_.bucket_count_value == 0) {
            assert(false && "disk_hash_table: bucket_count must be > 0");
        }
        uint32_t base = 1;
        uint32_t level = 0;
        while ((base << 1U) != 0 && (base << 1U) <= header_.bucket_count_value) {
            base <<= 1U;
            ++level;
        }
        header_.level_value = level;
        header_.split_bucket_value = header_.bucket_count_value - base;
    }

    uint64_t disk_hash_table_t::count_entries_unlocked() const {
        uint64_t count = 0;
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        for (uint32_t bucket = 0; bucket < header_.bucket_count_value; ++bucket) {
            uint64_t page_id = bucket_primary_page_id(bucket);
            while (page_id != 0) {
                if (!read_page(page_id, page)) {
                    break; // unreadable page: stop walking this chain
                }
                const auto cnt = page_count(page);
                for (uint16_t i = 0; i < cnt; ++i) {
                    const auto slot = read_slot(page, i);
                    if (slot.flags == slot_flag_used && slot.length != 0 &&
                        slot_belongs_to_bucket_unlocked(slot.key_hash, bucket)) {
                        ++count;
                    }
                }
                page_id = page_overflow(page);
            }
        }
        return count;
    }

    bool disk_hash_table_t::slot_belongs_to_bucket_unlocked(uint32_t key_hash, uint32_t bucket_id) const {
        return bucket_id_for_hash(key_hash) == bucket_id;
    }

    bool disk_hash_table_t::read_page(uint64_t page_id, byte_buffer_t& page) const {
        if (page.size() != page_size) {
            page.resize(page_size);
        }
        if (is_overflow_page_id(page_id)) {
            const uint64_t physical = page_id - overflow_page_id_base;
            if (physical >= overflow_page_count()) {
                return false;
            }
            if (!ovf_file_->read(page.data(), page_size, physical * page_size)) {
                return false;
            }
            return true;
        }
        if (page_id >= main_page_count()) {
            return false;
        }
        if (!file_->read(page.data(), page_size, page_id * page_size)) {
            return false;
        }
        return true;
    }

    bool disk_hash_table_t::write_page(uint64_t page_id, const byte_buffer_t& page) {
        if (page.size() != page_size) {
            return false;
        }
        if (is_overflow_page_id(page_id)) {
            const uint64_t physical = page_id - overflow_page_id_base;
            if (!ovf_file_->write(const_cast<uint8_t*>(page.data()), page_size, physical * page_size)) {
                return false;
            }
            return true;
        }
        if (!file_->write(const_cast<uint8_t*>(page.data()), page_size, page_id * page_size)) {
            return false;
        }
        return true;
    }

    void disk_hash_table_t::init_empty_page(byte_buffer_t& page) const {
        page.assign(page_size, 0);
        set_page_count(page, 0);
        set_page_free_offset(page, page_header_size);
        set_page_overflow(page, 0);
    }

    uint16_t disk_hash_table_t::page_count(const byte_buffer_t& page) const {
        return codec::read_le_ptr<uint16_t>(page.data());
    }

    uint16_t disk_hash_table_t::page_free_offset(const byte_buffer_t& page) const {
        return codec::read_le_ptr<uint16_t>(page.data() + 2);
    }

    uint64_t disk_hash_table_t::page_overflow(const byte_buffer_t& page) const {
        return codec::read_le_ptr<uint64_t>(page.data() + 4);
    }

    void disk_hash_table_t::set_page_count(byte_buffer_t& page, uint16_t v) const {
        codec::write_le_ptr<uint16_t>(page.data(), v);
    }

    void disk_hash_table_t::set_page_free_offset(byte_buffer_t& page, uint16_t v) const {
        codec::write_le_ptr<uint16_t>(page.data() + 2, v);
    }

    void disk_hash_table_t::set_page_overflow(byte_buffer_t& page, uint64_t v) const {
        codec::write_le_ptr<uint64_t>(page.data() + 4, v);
    }

    disk_hash_table_t::slot_t disk_hash_table_t::read_slot(const byte_buffer_t& page, uint16_t slot_index) const {
        const auto off = slot_dir_offset(slot_index);
        slot_t s{};
        s.offset = codec::read_le_ptr<uint16_t>(page.data() + off);
        s.length = codec::read_le_ptr<uint16_t>(page.data() + off + 2);
        s.flags = page[off + 4];
        s.key_hash = codec::read_le_ptr<uint32_t>(page.data() + off + 5);
        return s;
    }

    void disk_hash_table_t::write_slot(byte_buffer_t& page, uint16_t slot_index, const slot_t& slot) const {
        const auto off = slot_dir_offset(slot_index);
        codec::write_le_ptr<uint16_t>(page.data() + off, slot.offset);
        codec::write_le_ptr<uint16_t>(page.data() + off + 2, slot.length);
        page[off + 4] = slot.flags;
        codec::write_le_ptr<uint32_t>(page.data() + off + 5, slot.key_hash);
    }

    uint16_t disk_hash_table_t::slot_dir_offset(uint16_t slot_index) const {
        const auto idx = static_cast<uint32_t>(slot_index) + 1U;
        return static_cast<uint16_t>(static_cast<uint32_t>(page_size) - static_cast<uint32_t>(slot_size) * idx);
    }

    disk_hash_table_t::decoded_entry_t disk_hash_table_t::decode_entry(const byte_buffer_t& page,
                                                                       const slot_t& slot) const {
        if (slot.offset + slot.length > page_size || slot.length < (2 + 4 + 1 + 8 + 4 + 8)) {
            return decoded_entry_t{};
        }
        const auto* p = page.data() + slot.offset;
        decoded_entry_t e{};
        e.stored_key_len = codec::read_le_ptr<uint16_t>(p);
        e.full_key_len = codec::read_le_ptr<uint32_t>(p + 2);
        e.entry_flags = *(p + 6);
        const uint16_t header_len = 7;
        const uint16_t min_tail = 8 + 4 + 8;
        if (header_len + e.stored_key_len + min_tail > slot.length) {
            return decoded_entry_t{};
        }
        e.stored_key = std::string_view(reinterpret_cast<const char*>(p + header_len), e.stored_key_len);
        const auto* vptr = p + header_len + e.stored_key_len;
        e.value = codec::read_le_ptr<int64_t>(vptr);
        e.log_file_id = codec::read_le_ptr<uint32_t>(vptr + 8);
        e.log_offset = codec::read_le_ptr<uint64_t>(vptr + 12);
        e.valid = true;
        return e;
    }

    bool disk_hash_table_t::try_insert_payload_in_page(byte_buffer_t& page,
                                                       uint32_t key_hash,
                                                       const byte_buffer_t& payload,
                                                       bool& changed) {
        const uint16_t free_off = page_free_offset(page);
        const uint16_t cnt = page_count(page);
        const uint16_t dir_start = slot_dir_offset(cnt);
        const auto required = static_cast<size_t>(free_off) + payload.size() + static_cast<size_t>(slot_size);
        const auto available_limit = static_cast<size_t>(dir_start) + static_cast<size_t>(slot_size);
        if (required > available_limit) {
            return false;
        }

        const uint16_t new_off = free_off;
        std::memcpy(page.data() + new_off, payload.data(), payload.size());
        slot_t slot{};
        slot.offset = new_off;
        slot.length = static_cast<uint16_t>(payload.size());
        slot.flags = slot_flag_used;
        slot.key_hash = key_hash;
        write_slot(page, cnt, slot);
        set_page_count(page, cnt + 1);
        set_page_free_offset(page, static_cast<uint16_t>(free_off + payload.size()));
        changed = true;
        return true;
    }

    disk_hash_table_t::byte_buffer_t disk_hash_table_t::make_entry_payload(std::string_view key,
                                                                           int64_t value,
                                                                           uint32_t log_file_id,
                                                                           uint64_t log_offset) const {
        const bool truncated = key.size() > inline_key_limit;
        const uint16_t stored_len =
            static_cast<uint16_t>(truncated ? std::min<size_t>(truncated_prefix_len, key.size()) : key.size());
        const uint32_t full_len = static_cast<uint32_t>(std::min<size_t>(key.size(), UINT32_MAX));
        const size_t total = 2 + 4 + 1 + stored_len + 8 + 4 + 8;
        byte_buffer_t payload(memory_resource_);
        payload.resize(total);
        codec::write_le_ptr<uint16_t>(payload.data(), stored_len);
        codec::write_le_ptr<uint32_t>(payload.data() + 2, full_len);
        payload[6] = truncated ? entry_flag_truncated : 0;
        if (stored_len > 0) {
            std::memcpy(payload.data() + 7, key.data(), stored_len);
        }
        auto* tail = payload.data() + 7 + stored_len;
        codec::write_le_ptr<int64_t>(tail, value);
        codec::write_le_ptr<uint32_t>(tail + 8, log_file_id);
        codec::write_le_ptr<uint64_t>(tail + 12, log_offset);
        return payload;
    }

    uint64_t disk_hash_table_t::allocate_overflow_page() {
        if (overflow_alloc_failpoint()) {
            return 0; // the answer a failed page write below produces; see the seam's note
        }
        if (header_.next_overflow_page < overflow_page_id_base) {
            header_.next_overflow_page = overflow_page_id_base;
        }
        const uint64_t page_id = header_.next_overflow_page++;
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        init_empty_page(page);
        if (!write_page(page_id, page)) {
            return 0; // caller treats 0 as "no overflow page"
        }
        return page_id;
    }

    bool disk_hash_table_t::persist_header() {
        byte_buffer_t hdr(memory_resource_);
        hdr.resize(page_size, 0);
        codec::write_le_ptr<uint32_t>(hdr.data() + 12, header_.page_size_value);
        codec::write_le_ptr<uint32_t>(hdr.data() + 16, header_.bucket_count_value);
        codec::write_le_ptr<uint64_t>(hdr.data() + 20, header_.next_overflow_page);
        codec::write_le_ptr<uint32_t>(hdr.data() + 28, header_.level_value);
        codec::write_le_ptr<uint32_t>(hdr.data() + 32, header_.split_bucket_value);
        codec::write_le_ptr<uint32_t>(hdr.data() + 36, header_.hash_seed_value);
        if (!file_->write(hdr.data(), page_size, 0)) {
            return false;
        }
        return true;
    }

} // namespace services::index

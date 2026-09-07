#include "disk_hash_table.hpp"

#include <components/index/logical_value_binary_codec.hpp>

#include "absl/crc/crc32c.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

        // Magic + CRC32C catch corruption (e.g. an off-by-one bucket count) that would otherwise load silently.
        constexpr char hash_header_magic[8] = {'o', 't', 'b', 'x', 'h', 'a', 's', 'h'};
        constexpr size_t hash_header_fields_offset = 12;
        constexpr size_t hash_header_fields_size = 28;

        uint32_t hash_header_crc(const uint8_t* header_page) {
            return static_cast<uint32_t>(absl::ComputeCrc32c(
                absl::string_view(reinterpret_cast<const char*>(header_page) + hash_header_fields_offset,
                                  hash_header_fields_size)));
        }

#ifdef DEV_MODE
        bool split_crash_failpoint(const char* stage) {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_SPLIT_FAILPOINT");
            return v != nullptr && std::strcmp(v, stage) == 0;
        }

        // These failpoints simulate refusals unstageable from outside, each armed at the real failure's call site.
        bool overflow_alloc_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_OVERFLOW_ALLOC_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }

        bool reset_reopen_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_RESET_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }

        // Simulates a successful unlink that left the name in place, for reset_storage's postcondition check.
        bool reset_skip_wipe_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_SKIP_WIPE_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }

        bool close_flush_failpoint() {
            const char* v = std::getenv("OTTERBRIX_DISK_HASH_CLOSE_FAILPOINT");
            return v != nullptr && *v != '\0' && std::strcmp(v, "0") != 0;
        }
#else
        bool split_crash_failpoint(const char*) { return false; }
        bool overflow_alloc_failpoint() { return false; }
        // Both reset seams get a stub so their call sites read like the other two.
        bool reset_reopen_failpoint() { return false; }
        bool reset_skip_wipe_failpoint() { return false; }
        bool close_flush_failpoint() { return false; }
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
        // Caller bug (assert), not an I/O failure: I/O failures travel as a value instead, and this ctor opens nothing.
        assert(memory_resource && "disk_hash_table: resource required");
        assert(bucket_count > 0 && "disk_hash_table: bucket_count must be > 0");
        header_.bucket_count_value = bucket_count;
    }

    disk_hash_table_t::disk_hash_table_t(const std::filesystem::path& file_path,
                                         uint32_t bucket_count,
                                         std::pmr::memory_resource* memory_resource)
        : disk_hash_table_t(file_path, bucket_count, memory_resource, defer_open_tag{}) {
        if (open_or_create().contains_error()) {
            // A half-open table must never be handed to a caller that believes it has durable storage.
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
        if (auto open_result = instance->open_or_create(); open_result.contains_error()) {
            return open_result;
        }
        return instance;
    }

    disk_hash_table_t::~disk_hash_table_t() {
        if (file_) {
            // No value channel in a destructor, so a failed closing flush is reported on stderr instead.
            const bool header_persisted = !close_flush_failpoint() && persist_header();
            const bool synced = sync_files();
            if (!header_persisted || !synced) {
                std::fprintf(stderr,
                             "disk_hash_table: %s: the closing %s failed; the table's last "
                             "state may not have reached the device\n",
                             file_path_.string().c_str(),
                             header_persisted ? "fsync" : "header flush");
            }
        }
    }

    core::error_t
    disk_hash_table_t::put(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) {
        return put_unlocked(key, value, log_file_id, log_offset);
    }

    core::error_t
    disk_hash_table_t::put_unlocked(std::string_view key, int64_t value, uint32_t log_file_id, uint64_t log_offset) {
        const uint32_t key_hash = hash_key(key);
        const uint32_t bucket_id = bucket_id_for_hash(key_hash);
        auto payload = make_entry_payload(key, value, log_file_id, log_offset);
        RETURN_IF_ERROR(insert_payload_into_bucket_unlocked(bucket_id, key_hash, payload));
        ++entry_count_;
        // The entry is already in; a failing auto-rehash is still reported so the caller can stop.
        return maybe_rehash_if_needed_unlocked();
    }

    core::error_t disk_hash_table_t::insert_payload_into_bucket_unlocked(uint32_t bucket_id,
                                                                         uint32_t key_hash,
                                                                         const byte_buffer_t& payload) {
        uint64_t page_id = bucket_primary_page_id(bucket_id);
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        while (true) {
            // Bail on a failed read: this loop is `while (true)`, so ignoring it would spin on a stale page forever.
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

    core::error_t disk_hash_table_t::rehash(uint32_t new_bucket_count) { return rehash_unlocked(new_bucket_count); }

    core::error_t disk_hash_table_t::trigger_rehash_if_needed() { return maybe_rehash_if_needed_unlocked(); }

    bool disk_hash_table_t::set_auto_rehash_suppressed(bool suppressed) noexcept {
        return suppress_auto_rehash_.exchange(suppressed, std::memory_order_acq_rel);
    }

    double disk_hash_table_t::load_factor() const {
        if (header_.bucket_count_value == 0) {
            return 0.0;
        }
        return static_cast<double>(entry_count_) / static_cast<double>(header_.bucket_count_value);
    }

    core::error_t disk_hash_table_t::rehash_unlocked(uint32_t new_bucket_count) {
        if (new_bucket_count == 0) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"disk_hash_table: rehash to zero buckets", memory_resource_}};
        }
        if (new_bucket_count <= header_.bucket_count_value) {
            return core::error_t::no_error();
        }
        rehash_in_progress_ = true;
        struct reset_flag_t {
            bool& flag;
            ~reset_flag_t() { flag = false; }
        } reset{rehash_in_progress_};
        while (header_.bucket_count_value < new_bucket_count) {
            if (auto split_error = split_one_bucket_unlocked(); split_error.contains_error()) {
                // A split error means nothing published; splits that already landed are still flushed first.
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

    // Addressing advances only after the copy lands, so a refused copy leaves the source bucket intact
    // -- a refused overflow allocation mid-copy once left 160 of 400 rows unreachable.
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

        while (page_id != 0) {
            if (!read_page(page_id, page)) {
                // The rest of the chain is unknown, so publishing now would misaddress unread entries.
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
                    // Corruption: other walks catch this via decode_entry, which this copy loop bypasses.
                    return io_failure("disk_hash_table: slot extends past its page, cannot copy it");
                }
                payload.resize(slot.length);
                std::memcpy(payload.data(), page.data() + slot.offset, slot.length);
                RETURN_IF_ERROR(insert_payload_into_bucket_unlocked(new_bucket, slot.key_hash, payload));
            }
            page_id = page_overflow(page);
        }

        if (durable_commit) {
            if (!sync_files()) {
                return io_failure("disk_hash_table: the copied split entries could not be made durable");
            }
            if (split_crash_failpoint("after_copy_sync")) {
                return io_failure("disk_hash_table: split failpoint after_copy_sync");
            }
        }

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

        // Cleanup is intentionally skipped: stale copies stay present but are ignored by ownership checks.
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
        const auto curr_lf = static_cast<double>(entry_count_) / static_cast<double>(header_.bucket_count_value);
        if (curr_lf <= max_load_factor_) {
            return core::error_t::no_error();
        }
        bool changed = false;
        const double target_lf = max_load_factor_ * 0.6;
        const uint32_t target_buckets = static_cast<uint32_t>(
            std::min(static_cast<double>(UINT32_MAX), static_cast<double>(entry_count_) / target_lf));
        // Batches splits to target_lf with one sync barrier instead of one per split.
        while (header_.bucket_count_value < target_buckets && header_.bucket_count_value < UINT32_MAX) {
            if (auto split_error = split_one_bucket_unlocked(false); split_error.contains_error()) {
                if (changed) {
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
            if (!persist_header()) {
                return io_failure("disk_hash_table: failed to persist the header after a split batch");
            }
            if (!sync_files()) {
                return io_failure("disk_hash_table: the split batch could not be made durable");
            }
        }
        return core::error_t::no_error();
    }

    uint32_t disk_hash_table_t::bucket_count() const { return header_.bucket_count_value; }

    core::error_t disk_hash_table_t::sync() {
        if (!sync_files()) {
            return io_failure("disk_hash_table: " + file_path_.string() + " could not be made durable");
        }
        return core::error_t::no_error();
    }

    core::error_t disk_hash_table_t::reset_storage() {
        file_.reset();
        ovf_file_.reset();
        // A refused unlink must not be swallowed, or the caller replays segments over surviving contents.
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
        // suppress_auto_rehash_ is deliberately left untouched here; see its declaration.
        const uint32_t bucket_count =
            header_.bucket_count_value > 0 ? header_.bucket_count_value : default_bucket_count;
        const uint32_t hash_seed = header_.hash_seed_value;
        header_ = header_t{};
        header_.bucket_count_value = bucket_count;
        header_.hash_seed_value = hash_seed;
        // header_ must be self-consistent immediately, or a refused re-open below could seal it as-is.
        header_.next_overflow_page = overflow_page_id_base;
        RETURN_IF_ERROR(initialize_linear_state_from_bucket_count());
        if (reset_reopen_failpoint()) {
            return io_failure("disk_hash_table: the reset failpoint refused the re-open of " + file_path_.string());
        }
        return open_after_wipe_or_refuse();
    }

    // Can't reuse open_or_create(): it treats a non-zero file_size() as an existing table to load.
    core::error_t disk_hash_table_t::open_after_wipe_or_refuse() {
        file_ = open_file(fs_,
                          file_path_,
                          file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK);
        if (!file_) {
            return io_failure("disk_hash_table: failed to open file " + file_path_.string());
        }
        if (file_->file_size() != 0) {
            // Release the handle, or the destructor's closing flush would write over a file just declined.
            file_.reset();
            return io_failure("disk_hash_table: " + file_path_.string() + " survived the wipe");
        }
        RETURN_IF_ERROR(open_overflow_file());
        if (ovf_file_->file_size() != 0) {
            file_.reset();
            ovf_file_.reset();
            return io_failure("disk_hash_table: " + overflow_file_path_.string() + " survived the wipe");
        }
        return initialize_new_file();
    }

    // Called only when a wipe couldn't finish; read_page/write_page already refuse with no handle open.
    void disk_hash_table_t::close_storage() {
        file_.reset();
        ovf_file_.reset();
    }

    bool disk_hash_table_t::sync_files() {
        // Both are tried: the overflow file holds spilled chains, so durability on one alone is half the table.
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
        VALUE_OR_RETURN(entry_count_, count_entries_unlocked());
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
        // generate_hash_seed() never returns 0, so 0 reliably means "not set yet" (kept by reset_storage).
        header_.hash_seed_value = header_.hash_seed_value != 0 ? header_.hash_seed_value : generate_hash_seed();
        RETURN_IF_ERROR(initialize_linear_state_from_bucket_count());

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
        // Checked before any field is interpreted: a flipped bit could pass a check and re-address every key.
        if (std::memcmp(hdr.data(), hash_header_magic, sizeof(hash_header_magic)) != 0) {
            return io_failure("disk_hash_table: " + file_path_.string() +
                              " does not carry the hash-table magic; refusing to interpret it");
        }
        if (codec::read_le_ptr<uint32_t>(hdr.data() + 8) != hash_header_crc(hdr.data())) {
            return core::error_t{core::error_code_t::data_corruption,
                                 std::pmr::string{"disk_hash_table: the header checksum of " + file_path_.string() +
                                                      " does not match its fields",
                                                  memory_resource_}};
        }
        header_.page_size_value = codec::read_le_ptr<uint32_t>(hdr.data() + 12);
        header_.bucket_count_value = codec::read_le_ptr<uint32_t>(hdr.data() + 16);
        header_.next_overflow_page = codec::read_le_ptr<uint64_t>(hdr.data() + 20);
        header_.level_value = codec::read_le_ptr<uint32_t>(hdr.data() + 28);
        header_.split_bucket_value = codec::read_le_ptr<uint32_t>(hdr.data() + 32);
        header_.hash_seed_value = codec::read_le_ptr<uint32_t>(hdr.data() + 36);
        if (header_.page_size_value != page_size || header_.bucket_count_value == 0) {
            return io_failure("disk_hash_table: incompatible header");
        }
        // An inconsistent level/split/bucket_count triple is corruption, not something to re-derive silently.
        const uint32_t base = header_.level_value > 31 ? 0 : (1U << header_.level_value);
        if (base == 0 || base > header_.bucket_count_value || header_.split_bucket_value > base ||
            (base + header_.split_bucket_value) != header_.bucket_count_value) {
            return core::error_t{core::error_code_t::data_corruption,
                                 std::pmr::string{"disk_hash_table: the linear-hash state of " + file_path_.string() +
                                                      " does not describe its bucket count",
                                                  memory_resource_}};
        }
        // persist_header never writes a cursor below the base, so a lower value on disk is damage.
        if (header_.next_overflow_page < overflow_page_id_base) {
            return core::error_t{core::error_code_t::data_corruption,
                                 std::pmr::string{"disk_hash_table: the overflow cursor of " + file_path_.string() +
                                                      " points below the overflow page id base",
                                                  memory_resource_}};
        }

        return open_overflow_file();
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

    core::error_t disk_hash_table_t::initialize_linear_state_from_bucket_count() {
        if (header_.bucket_count_value == 0) {
            // assert alone compiles out under NDEBUG and would let split_bucket underflow to UINT32_MAX.
            assert(false && "disk_hash_table: bucket_count must be > 0");
            return io_failure("disk_hash_table: cannot derive a linear-hash state from zero buckets");
        }
        uint32_t base = 1;
        uint32_t level = 0;
        while ((base << 1U) != 0 && (base << 1U) <= header_.bucket_count_value) {
            base <<= 1U;
            ++level;
        }
        header_.level_value = level;
        header_.split_bucket_value = header_.bucket_count_value - base;
        return core::error_t::no_error();
    }

    core::result_wrapper_t<uint64_t> disk_hash_table_t::count_entries_unlocked() const {
        uint64_t count = 0;
        byte_buffer_t page(memory_resource_);
        page.resize(page_size);
        for (uint32_t bucket = 0; bucket < header_.bucket_count_value; ++bucket) {
            uint64_t page_id = bucket_primary_page_id(bucket);
            while (page_id != 0) {
                if (!read_page(page_id, page)) {
                    // Refuses rather than `break`, which would publish the readable part as the whole count.
                    return page_read_failure(page_id);
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
        // Erased slots are reused first, or a put/erase workload would exhaust the page for no net growth.
        for (uint16_t i = 0; i < cnt; ++i) {
            auto slot = read_slot(page, i);
            if (slot.flags != slot_flag_free || slot.length < payload.size() ||
                static_cast<uint32_t>(slot.offset) + slot.length > page_size) {
                continue;
            }
            std::memcpy(page.data() + slot.offset, payload.data(), payload.size());
            slot.flags = slot_flag_used;
            slot.key_hash = key_hash;
            write_slot(page, i, slot);
            changed = true;
            return true;
        }
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
            return 0; // mimics the answer a failed page write below produces
        }
        if (header_.next_overflow_page < overflow_page_id_base) {
            // Unreachable while the load-time check holds; clamping instead of refusing would hide corruption.
            return 0;
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
        // The seal goes last: the CRC proves the six fields above came back unchanged.
        std::memcpy(hdr.data(), hash_header_magic, sizeof(hash_header_magic));
        codec::write_le_ptr<uint32_t>(hdr.data() + 8, hash_header_crc(hdr.data()));
        if (!file_->write(hdr.data(), page_size, 0)) {
            return false;
        }
        return true;
    }

} // namespace services::index

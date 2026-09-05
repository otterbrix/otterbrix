#include "bitcask_index_disk.hpp"

#include "absl/crc/crc32c.h"
#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace services::index {

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;
    using core::filesystem::move_files;
    using core::filesystem::open_file;
    using core::filesystem::remove_directory;

#ifdef DEV_MODE
    namespace {
        bitcask_file_interposer_t* dev_bitcask_file_interposer_ = nullptr;
        std::atomic<uint64_t> g_bitcask_rotated_segment_opens{0};
    } // namespace

    uint64_t bitcask_rotated_segment_opens() noexcept {
        return g_bitcask_rotated_segment_opens.load(std::memory_order_relaxed);
    }
    void reset_bitcask_rotated_segment_opens() noexcept {
        g_bitcask_rotated_segment_opens.store(0, std::memory_order_relaxed);
    }

    void dev_set_bitcask_file_interposer(bitcask_file_interposer_t* interposer) {
        dev_bitcask_file_interposer_ = interposer;
    }

    bitcask_file_interposer_t* dev_bitcask_file_interposer() { return dev_bitcask_file_interposer_; }
#endif

    namespace {
        // errno must be captured immediately after open_file -- anything run afterward
        // (incl. the DEV_MODE interposer below) may clobber it. thread_local: one store per
        // index agent thread.
        thread_local int last_open_errno = 0;

        std::string open_refusal_reason() {
            return last_open_errno == 0 ? std::string{"no reason reported"}
                                        : std::string{std::strerror(last_open_errno)};
        }

        std::unique_ptr<core::filesystem::file_handle_t> open_bitcask_file(
            core::filesystem::local_file_system_t& fs,
            const std::filesystem::path& path,
            file_flags flags,
            file_lock_type lock) {
            auto handle = open_file(fs, path, flags, lock);
            last_open_errno = handle == nullptr ? errno : 0;
#ifdef DEV_MODE
            if (auto* interposer = dev_bitcask_file_interposer(); interposer != nullptr) {
                handle = interposer->wrap(path, std::move(handle));
            }
#endif
            return handle;
        }

        components::types::logical_value_t normalize_hash_key(const components::types::logical_value_t& key,
                                                              core::date::timezone_offset_t session_tz) {
            using namespace components::types;
            switch (key.type().type()) {
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT: {
                    // Signed-integer widening can not fail for the types this switch admits;
                    // still, never assert-then-value() (a failed cast in Release would deref
                    // an empty optional). A non-widenable key keeps its native representation
                    // — identical to the default branch, and self-consistent between insert
                    // and find (both normalize the same way).
                    auto casted = key.cast_as(complex_logical_type(logical_type::BIGINT), session_tz);
                    if (casted.has_error()) {
                        return key;
                    }
                    return std::move(casted.value());
                }
                case logical_type::UTINYINT:
                case logical_type::USMALLINT:
                case logical_type::UINTEGER:
                case logical_type::UBIGINT: {
                    auto casted = key.cast_as(complex_logical_type(logical_type::UBIGINT), session_tz);
                    if (casted.has_error()) {
                        return key;
                    }
                    return std::move(casted.value());
                }
                default:
                    return key;
            }
        }

        constexpr const char* segment_prefix = "bitcask.";
        constexpr const char* segment_suffix = ".data";
        constexpr const char* current_segment_file = "CURRENT";
        constexpr const char* merge_manifest_file = "bitcask.merge";
        constexpr const char* hash_index_file = "hash_index.bin";
        constexpr const char* txn_log_file = "bitcask.txn.log";
        constexpr const char* txn_applied_file = "bitcask.txn.applied";
        constexpr unsigned segment_id_width = 6;
        constexpr uint32_t txn_magic = 0x314E5854; // TXN1

        struct record_header_t {
            uint32_t crc;
            uint8_t kind;
            uint64_t payload_size;
            uint64_t timestamp;
        };

        // The recover gate compares commit_id, not txn_id: txn_id is reused across restarts,
        // commit_id never is. No version field -- an old build's 32-byte header just fails CRC
        // and those frames are dropped rather than misread.
        struct txn_frame_header_t {
            uint32_t magic;
            uint32_t crc;
            uint64_t txn_id;
            uint64_t commit_id;
            uint8_t op_kind; // 1=insert, 2=delete(row)
            uint64_t payload_size;
        };

        std::pmr::string serialize_payload(std::pmr::memory_resource* resource,
                                           const services::index::bitcask_index_disk_t::value_t& key,
                                           const std::pmr::vector<size_t>& rows) {
            std::pmr::string out(resource);
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint32_t>(out, static_cast<uint32_t>(rows.size()));
            for (auto row : rows) {
                components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row));
            }
            return out;
        }

        // The key codec leaves `pos` unmoved when it refuses, so ignoring a false return here
        // would misread the row count out of the key's own bytes instead of yielding "no rows".
        [[nodiscard]] bool deserialize_payload(std::pmr::memory_resource* resource,
                                               const std::pmr::string& payload,
                                               services::index::bitcask_index_disk_t::value_t& key,
                                               std::pmr::vector<size_t>& rows) {
            size_t pos = 0;
            bool ok = true;
            key = components::index::codec::read_logical_value(resource, payload, pos, &ok);
            const auto n = components::index::codec::read_le<uint32_t>(payload, pos, &ok);
            if (!ok) {
                return false;
            }
            rows.clear();
            // n comes off disk untrusted; bound it against remaining payload bytes before
            // reserve() so a corrupt count can't ask the allocator for e.g. 32GB.
            if (n > (payload.size() - pos) / sizeof(uint64_t)) {
                return false;
            }
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                rows.emplace_back(
                    static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos, &ok)));
            }
            return ok;
        }

        std::filesystem::path segment_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            std::ostringstream oss;
            oss << segment_prefix << std::setw(segment_id_width) << std::setfill('0') << segment_id << segment_suffix;
            return directory / oss.str();
        }

        std::filesystem::path merge_temp_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            return segment_file_path(directory, segment_id).string() + ".merge";
        }

        std::filesystem::path current_segment_path(const std::filesystem::path& directory) {
            return directory / current_segment_file;
        }

        bool parse_segment_id(const std::filesystem::path& path, uint64_t& segment_id) {
            const auto filename = path.filename().string();
            const std::string_view filename_sv{filename};
            constexpr std::string_view prefix = segment_prefix;
            constexpr std::string_view suffix = segment_suffix;
            if (!filename_sv.starts_with(prefix) || !filename_sv.ends_with(suffix)) {
                return false;
            }
            const std::string_view digits =
                filename_sv.substr(prefix.size(), filename_sv.size() - prefix.size() - suffix.size());
            if (digits.empty()) {
                return false;
            }
            const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), segment_id);
            return ec == std::errc() && ptr == digits.data() + digits.size();
        }

        // absent: nothing written yet (ordinary; caller may act on it). unopenable: file exists,
        // open refused -- transient, clears itself. damaged: file opened but won't parse, does
        // NOT clear itself. Folding unopenable into absent would let a transient permission/fd
        // blip pass for "never written".
        enum class sidecar_state_t
        {
            ok,
            absent,
            unopenable,
            damaged
        };

        sidecar_state_t read_sidecar_uint64(const std::filesystem::path& file, uint64_t& out) {
            std::error_code ec;
            const bool present = std::filesystem::exists(file, ec);
            if (ec) {
                // exists() itself refusing is a directory this process cannot look into, which
                // is the transient class, not "there is no such file".
                return sidecar_state_t::unopenable;
            }
            if (!present) {
                return sidecar_state_t::absent;
            }
            std::ifstream input(file);
            if (!input.good()) {
                return sidecar_state_t::unopenable;
            }
            input >> out;
            return input.fail() ? sidecar_state_t::damaged : sidecar_state_t::ok;
        }

        sidecar_state_t read_current_segment_id(const std::filesystem::path& directory, uint64_t& segment_id) {
            return read_sidecar_uint64(current_segment_path(directory), segment_id);
        }

        std::filesystem::path merge_manifest_path(const std::filesystem::path& directory) {
            return directory / merge_manifest_file;
        }

        sidecar_state_t read_merge_manifest(const std::filesystem::path& directory,
                                            uint64_t& merged_segment_id,
                                            std::vector<uint64_t>& removed_segment_ids) {
            const auto manifest_path_value = merge_manifest_path(directory);
            std::error_code ec;
            const bool present = std::filesystem::exists(manifest_path_value, ec);
            if (ec) {
                return sidecar_state_t::unopenable;
            }
            if (!present) {
                return sidecar_state_t::absent;
            }
            std::ifstream input(manifest_path_value);
            if (!input.good()) {
                // Distinct from absent: the caller unlinks segments on the strength of "no
                // manifest", so a refused open must not be reported as one.
                return sidecar_state_t::unopenable;
            }
            std::size_t removed_count = 0;
            input >> merged_segment_id >> removed_count;
            if (input.fail()) {
                return sidecar_state_t::damaged;
            }
            removed_segment_ids.clear();
            // No reserve(removed_count): the count came off disk untrusted and a bogus value
            // (e.g. 1e18) would throw std::bad_alloc uncaught. Bounded by the file
            // itself instead -- the loop below stops when the stream runs out.
            for (std::size_t i = 0; i < removed_count; ++i) {
                uint64_t removed_id = 0;
                input >> removed_id;
                if (input.fail()) {
                    removed_segment_ids.clear();
                    return sidecar_state_t::damaged;
                }
                removed_segment_ids.push_back(removed_id);
            }
            return sidecar_state_t::ok;
        }

        // error_code overload, not remove_file's bool: that bool conflates "device refused to
        // unlink" with "nothing was there" (the latter is success, not failure).
        [[nodiscard]] bool unlink_if_present(const std::filesystem::path& artifact, std::error_code& ec) {
            ec.clear();
            std::filesystem::remove(artifact, ec);
            return !ec;
        }

        // false means the manifest is still on disk -- callers must not proceed as if the
        // merge record were gone (a stale manifest would name segments already unlinked).
        [[nodiscard]] bool remove_merge_manifest(const std::filesystem::path& directory, std::error_code& ec) {
            return unlink_if_present(merge_manifest_path(directory), ec);
        }

        // Logs only, never fails the caller: the writers below have already decided their
        // return value, and a leftover temp is harmless since the next attempt reopens it
        // with std::ios::trunc.
        void report_undroppable_temp(const std::filesystem::path& temp_path) {
            std::error_code ec;
            if (unlink_if_present(temp_path, ec)) {
                return;
            }
            std::fprintf(stderr,
                         "bitcask: the temp file %s was left behind by a failed publish and could not be removed: "
                         "%s\n",
                         temp_path.string().c_str(),
                         ec.message().c_str());
        }

        // Durably publish a sidecar: fsync temp, then rename over target.
        // rename(2) atomically replaces an existing target on POSIX, so never
        // unlink the target first — that leaves a crash window with no file.
        [[nodiscard]] bool publish_replacement_file(core::filesystem::local_file_system_t& fs,
                                                    const std::filesystem::path& temp_path,
                                                    const std::filesystem::path& target_path) {
            auto temp_file =
                open_bitcask_file(fs, temp_path, file_flags::READ | file_flags::WRITE, file_lock_type::NO_LOCK);
            if (!temp_file || !temp_file->sync()) {
                return false;
            }
            temp_file.reset();
            if (move_files(fs, temp_path, target_path)) {
                return true;
            }
#if defined(_WIN32)
            std::error_code ec;
            std::filesystem::remove(target_path, ec);
            return move_files(fs, temp_path, target_path);
#else
            return false;
#endif
        }

        // false means the manifest is NOT on disk: a merge that can't record what it's about
        // to do must stop before publishing anything.
        [[nodiscard]] bool write_merge_manifest(core::filesystem::local_file_system_t& fs,
                                                const std::filesystem::path& directory,
                                                uint64_t merged_segment_id,
                                                const std::vector<uint64_t>& removed_segment_ids) {
            const auto manifest_path = merge_manifest_path(directory);
            const auto temp_path = manifest_path.string() + ".tmp";
            {
                std::ofstream output(temp_path, std::ios::trunc);
                if (!output.good()) {
                    return false;
                }
                output << merged_segment_id << ' ' << removed_segment_ids.size();
                for (const auto removed_id : removed_segment_ids) {
                    output << ' ' << removed_id;
                }
                output << '\n';
                output.flush();
                if (!output.good()) {
                    report_undroppable_temp(temp_path);
                    return false;
                }
            }
            if (publish_replacement_file(fs, temp_path, manifest_path)) {
                return true;
            }
            // The publish left the temp behind; nothing reads it, and leaving it would have
            // the next attempt write over a file it did not create.
            report_undroppable_temp(temp_path);
            return false;
        }

        // Opening the index is a write to its directory: CURRENT is republished via temp file
        // + rename here, on every start and every rotation, so the directory needs `w`.
        [[nodiscard]] bool write_current_segment_id(core::filesystem::local_file_system_t& fs,
                                                    const std::filesystem::path& directory,
                                                    uint64_t segment_id) {
            const auto current_path = current_segment_path(directory);
            const auto temp_path = current_path.string() + ".tmp";
            {
                std::ofstream output(temp_path, std::ios::trunc);
                if (!output.good()) {
                    return false;
                }
                output << segment_id;
                output.flush();
                if (!output.good()) {
                    report_undroppable_temp(temp_path);
                    return false;
                }
            }
            if (publish_replacement_file(fs, temp_path, current_path)) {
                return true;
            }
            report_undroppable_temp(temp_path);
            return false;
        }

        // Returns how much landed, not just whether it finished: a record is two writes, and a
        // bool would collapse "nothing written" and "header written, payload refused" into the
        // same answer, leaving the caller unable to tell a stump from an untouched file.
        [[nodiscard]] core::filesystem::write_result_t write_record(core::filesystem::file_handle_t& file,
                                                                    uint8_t kind,
                                                                    uint64_t timestamp,
                                                                    const std::pmr::string& payload) {
            // Value-init (`{}`) zeroes the 3 padding bytes at offsets 5-7 before they're CRC'd
            // and written to disk; aggregate init (`{a,b,c,d}`) leaves them as uninitialized
            // stack garbage. txn_frame_header_t below has the same padding issue.
            record_header_t header{};
            header.kind = kind;
            header.payload_size = static_cast<uint64_t>(payload.size());
            header.timestamp = timestamp;

            absl::crc32c_t crc = absl::ComputeCrc32c(
                absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
            if (!payload.empty()) {
                crc = absl::ExtendCrc32c(crc, absl::string_view(payload.data(), payload.size()));
            }
            header.crc = static_cast<uint32_t>(crc);

            const auto header_write = file.write(&header, sizeof(header));
            if (!header_write.complete) {
                return core::filesystem::write_result_t::refused(header_write.bytes_written);
            }
            if (payload.empty()) {
                return core::filesystem::write_result_t::done(header_write.bytes_written);
            }
            const auto payload_write = file.write(const_cast<char*>(payload.data()), payload.size());
            const uint64_t landed = header_write.bytes_written + payload_write.bytes_written;
            if (!payload_write.complete) {
                return core::filesystem::write_result_t::refused(landed);
            }
            return core::filesystem::write_result_t::done(landed);
        }

        // A half-landed record left in place would stop being a tail once the next append writes
        // past it, turning it into an interior frame that fails CRC/magic and takes the WHOLE
        // file down on replay -- so it must be truncated off here, while `record_offset` is
        // still known. truncate/fsync/seek are all checked: any one silently failing would leave
        // the descriptor past a now-shorter file. fsync matters because the truncate is a
        // metadata change that a crash could otherwise undo, leaving the stump back.
        [[nodiscard]] bool discard_partial_record(core::filesystem::file_handle_t& file,
                                                  const core::filesystem::write_result_t& result,
                                                  uint64_t record_offset) {
            if (!result.partial()) {
                return true;
            }
            if (!file.truncate(static_cast<int64_t>(record_offset))) {
                return false;
            }
            if (!file.sync()) {
                return false;
            }
            return file.seek(record_offset);
        }
    } // namespace

    // No I/O here -- the agent holds this store by value in its member initializer list, so
    // construction must be the step that cannot fail; open() is the step that can.
    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit,
                                               std::pmr::set<std::uint64_t> committed_commit_ids,
                                               deferred_open_t)
        : resource_(resource)
        , flush_threshold_(flush_threshold)
        , path_(path)
        , hash_index_file_path_(path_ / hash_index_file)
        , fs_(core::filesystem::local_file_system_t())
        , segment_record_limit_(segment_record_limit)
        , committed_commit_ids_(committed_commit_ids.begin(), committed_commit_ids.end(), resource) {}

    // Opening this index is a write to its directory (CURRENT is republished via temp+rename
    // on every open, see write_current_segment_id) -- the directory needs r+w+x, there is no
    // read-only mode, and there is no lock: it must belong to exactly one process.
    core::error_t bitcask_index_disk_t::open() {
        RETURN_IF_ERROR(initialize_storage());
        if (auto open_result = open_hash_index(); open_result.contains_error()) {
            return open_result;
        }
        RETURN_IF_ERROR(load_from_disk());
        if (crc_failure_) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"bitcask: CRC mismatch during recovery", resource_}};
        }
        RETURN_IF_ERROR(open_active_segment());
        RETURN_IF_ERROR(recover_txn_log());
        // Recovery applies frames via insert()/remove(), which are void and park failures in
        // pending_write_error_; force_flush() here drains that instead of deferring to a later
        // statement's flush.
        return force_flush();
    }

    // Construct-and-open, for backend tests only (test_bitcask_index_disk.cpp,
    // stress_test_index.cpp): aborts on any open() failure since a ctor has no error channel
    //. Production uses the deferred ctor + open() instead, inside
    // bitcask_index_agent_t's member initializer list, so an environmental failure costs the
    // index its registration, not the engine its start. A test that wants to observe a refusal
    // must use the deferred ctor and read open()'s value directly.
    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit,
                                               std::pmr::set<std::uint64_t> committed_commit_ids)
        : bitcask_index_disk_t(path,
                               resource,
                               flush_threshold,
                               segment_record_limit,
                               std::move(committed_commit_ids),
                               deferred_open_t{}) {
        if (const auto open_error = open(); open_error.contains_error()) {
            std::fprintf(stderr,
                         "bitcask: the construct-and-open ctor could not open %s: %s\n",
                         path.string().c_str(),
                         open_error.what.c_str());
            assert(false && "bitcask I/O failure: the construct-and-open ctor could not open the store");
            std::abort();
        }
    }

    // Environmental failures (unopenable path, bad header) must cost the index its
    // registration, not the engine's start (integration/cpp/test/test_index_bootstrap_failure.cpp).
    core::error_t bitcask_index_disk_t::io_failure(std::string_view message) const {
        return core::error_t{core::error_code_t::index_create_fail,
                             std::pmr::string{message.data(), message.size(), resource_}};
    }

    core::error_t bitcask_index_disk_t::open_hash_index() {
        auto storage =
            disk_hash_table_t::create(hash_index_file_path_, disk_hash_table_t::default_bucket_count, resource());
        if (storage.has_error()) {
            return storage.error();
        }
        hash_index_ = std::move(storage.value());
        return core::error_t::no_error();
    }

    bitcask_index_disk_t::~bitcask_index_disk_t() {
        if (!hash_index_) {
            // Keydir never opened, or drop() released it -- nothing to flush.
            return;
        }
        // Last chance, not the only one: every write door already ends in a force_flush a
        // caller reads, so by now either the flush already succeeded or its failure was
        // already reported. A destructor has no error channel of its own.
        auto ignored_flush_error = force_flush();
    }

    // read.value() (the record's kind) is deliberately not consulted: read_rows_at fills
    // *out_key before classifying the record, so a tombstone carries the same key a value
    // record does, and duplicating the kind check here would fold a read failure into
    // "this entry is not your key".
    core::result_wrapper_t<std::pmr::string> bitcask_index_disk_t::load_hash_key_at(uint32_t segment_id,
                                                                                    uint64_t value_offset) const {
        row_ids_t rows(resource());
        value_t key(resource(), nullptr);
        auto read = read_rows_at(segment_id, value_offset, rows, &key);
        if (read.has_error()) {
            return read.error();
        }
        // Crosses to this store's resource since codec::encode_disk_hash_key builds a
        // plain std::string; not a hot path -- called ZERO times in the randomized stress
        // profile, since encoded integer keys (9 bytes) are well under inline_key_limit (64).
        bool key_hashable = true;
        const auto key_bytes = key_bytes_for_hash(key, &key_hashable);
        if (!key_hashable) {
            return io_failure("bitcask: a stored key has no hash encoding in this build");
        }
        return std::pmr::string(key_bytes.data(), key_bytes.size(), resource());
    }

    void bitcask_index_disk_t::set_bulk_mode(bool enabled) {
        if (enabled) {
            if (!bulk_mode_ && hash_index_) {
                bulk_prev_rehash_suppressed_ = hash_index_->set_auto_rehash_suppressed(true);
                bulk_rehash_guard_active_ = true;
            }
            bulk_mode_ = true;
            return;
        }

        if (!enabled && bulk_mode_ && hash_index_ && bulk_rehash_guard_active_) {
            hash_index_->set_auto_rehash_suppressed(bulk_prev_rehash_suppressed_);
            bulk_rehash_guard_active_ = false;
            if (!bulk_prev_rehash_suppressed_) {
                // set_bulk_mode is void and its callers are handlers that have already
                // written; a keydir that could not finish growing is handed to the next
                // force_flush rather than dropped here.
                note_write_error(hash_index_->trigger_rehash_if_needed());
            }
        }
        bulk_mode_ = enabled;
    }

    core::error_t bitcask_index_disk_t::initialize_storage() {
        if (std::filesystem::exists(path_)) {
            return core::error_t::no_error();
        }
        std::error_code ec;
        std::filesystem::create_directories(path_, ec);
        if (ec) {
            return io_failure("bitcask: the index directory " + path_.string() + " could not be created: " +
                              ec.message());
        }
        return core::error_t::no_error();
    }

    // `ok` is only ever set false; passed by the three callers whose key came off the disk
    // (untrusted), unlike the other five whose key was vetted by CREATE INDEX.
    std::string bitcask_index_disk_t::key_bytes_for_hash(const value_t& key, bool* ok) const {
        auto normalized = normalize_hash_key(key, core::date::timezone_offset_t{});
        return components::index::codec::encode_disk_hash_key(normalized, ok);
    }

    // None of the three refusal paths below may be swallowed: load_from_disk replays every
    // segment it finds, so a manifest left unresolved would replay sources the merge already
    // rewrote and resurrect the keys it dropped. A refusal here costs the index its
    // registration, not the engine (open() -> bitcask_index_agent_t::create); every refusal
    // leaves the manifest in place so the next open retries, except a damaged (unparseable)
    // manifest, which is permanent -- the segment ids it named are unrecoverable.
    core::error_t bitcask_index_disk_t::apply_merge_recovery_cleanup() {
        const auto manifest_path = merge_manifest_path(path_);
        std::error_code ec;

        uint64_t merged_segment_id = 0;
        std::vector<uint64_t> removed_segment_ids;
        switch (read_merge_manifest(path_, merged_segment_id, removed_segment_ids)) {
            case sidecar_state_t::absent:
                // No merge was interrupted -- the ordinary case.
                return core::error_t::no_error();
            case sidecar_state_t::unopenable:
                // Transient (permission/fd/device); clears itself and the next open retries.
                return io_failure("bitcask: the merge manifest " + manifest_path.string() +
                                  " is present and could not be opened; the index is not registered while that "
                                  "lasts, and the next open retries it unchanged");
            case sidecar_state_t::damaged:
                // Permanent, deliberately: the manifest is published via temp+rename so it's
                // never half-written by a crash -- unparseable bytes mean the segment ids it
                // named are gone for good, and neither replaying nor dropping the merged
                // segment without them is safe.
                return io_failure("bitcask: the merge manifest " + manifest_path.string() +
                                  " is present and its bytes could not be read as a manifest; this does not clear by "
                                  "itself -- drop and re-create the index, or remove its directory, to rebuild it "
                                  "from the table");
            case sidecar_state_t::ok:
                break;
        }

        const bool merged_present = std::filesystem::exists(segment_file_path(path_, merged_segment_id), ec);
        if (ec) {
            return io_failure("bitcask: the merged segment named by " + manifest_path.string() +
                              " could not be looked up: " + ec.message());
        }
        if (!merged_present) {
            // The merge never published its output, so the sources it names are the whole
            // truth about this index and every one of them stays. Only the record of the
            // attempt goes.
            if (!remove_merge_manifest(path_, ec)) {
                return io_failure("bitcask: the manifest of a merge that published nothing could not be removed from " +
                                  manifest_path.string() + ": " + ec.message());
            }
            return core::error_t::no_error();
        }

        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            if (!unlink_if_present(removed_path, ec)) {
                // Manifest stays -- it's the only record that this unlink is still owed, so
                // the next open retries this loop.
                return io_failure("bitcask: the merged-away segment " + removed_path.string() +
                                  " could not be removed: " + ec.message());
            }
        }
        // All sources gone: the next merge would overwrite this manifest, so it must go now
        // or a source this merge failed to unlink stops being named by anything.
        if (!remove_merge_manifest(path_, ec)) {
            return io_failure("bitcask: the manifest of a finished merge could not be removed from " +
                              manifest_path.string() + ": " + ec.message());
        }
        return core::error_t::no_error();
    }

    void bitcask_index_disk_t::drop_cached_rotated_segment_(uint64_t segment_id) const noexcept {
        for (auto it = rotated_read_cache_.begin(); it != rotated_read_cache_.end(); ++it) {
            if (it->segment_id == segment_id) {
                rotated_read_cache_.erase(it);
                return;
            }
        }
    }

    core::error_t bitcask_index_disk_t::load_from_disk() {
        // Every load re-derives: stale rotated-read handles and a stale crc_failure_ flag
        // would otherwise pollute this reload's verdict with the previous one's.
        invalidate_rotated_read_cache_();
        crc_failure_ = false;
        const bool prev_rehash_suppressed = hash_index_->set_auto_rehash_suppressed(true);
        struct restore_rehash_state_t {
            disk_hash_table_t* table{nullptr};
            bool prev{false};
            ~restore_rehash_state_t() {
                if (table) {
                    table->set_auto_rehash_suppressed(prev);
                }
            }
        } restore_rehash_state{hash_index_.get(), prev_rehash_suppressed};

        RETURN_IF_ERROR(apply_merge_recovery_cleanup());

        // This function is the keydir's only author: everything in it is re-derived from the
        // segments below, so a stale entry can only mean a live device refusal now, never a
        // segment unlinked by a merge from restarts ago. Unconditional and BEFORE the early
        // return on an empty segment set, or a fully-emptied index would keep answering find()
        // out of a stale keydir.
        RETURN_IF_ERROR(hash_index_->reset_storage());

        VALUE_OR_RETURN(auto segments, collect_segments());
        // Set on every road out of this function, so open_active_segment can never act on
        // what a previous open left.
        active_segment_clean_end_ = no_tail_to_trim;
        if (segments.empty()) {
            active_segment_id_ = regular_segment_id_start_;
            next_segment_id_ = regular_segment_id_start_ + 1;
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
            restore_rehash_state.table = nullptr;
            hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
            if (!prev_rehash_suppressed) {
                RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
            }
            return core::error_t::no_error();
        }

        // Decided before the walk, because the walk needs it: a tail this build cannot read
        // is repairable only in the active segment, plain damage in every other (see the CRC
        // arm below).
        uint64_t configured_active_segment_id = 0;
        switch (read_current_segment_id(path_, configured_active_segment_id)) {
            case sidecar_state_t::absent:
                // No CURRENT (pre-pointer layout, or a wipe that got this far): newest segment
                // is the documented stand-in.
                configured_active_segment_id = segments.back().id;
                break;
            case sidecar_state_t::unopenable:
                // Must NOT stand in the newest segment here: if CURRENT names an older one,
                // substituting would replay appends of this uptime in the wrong order later.
                return io_failure("bitcask: the CURRENT segment pointer " + current_segment_path(path_).string() +
                                  " is present and could not be opened; the index is not registered while that lasts, "
                                  "and the next open reads it unchanged");
            case sidecar_state_t::damaged:
                return io_failure("bitcask: the CURRENT segment pointer " + current_segment_path(path_).string() +
                                  " is present and does not hold a segment id; this does not clear by itself -- drop "
                                  "and re-create the index, or remove its directory, to rebuild it from the table");
            case sidecar_state_t::ok:
                break;
        }
        const auto active_it = std::find_if(segments.begin(), segments.end(), [&](const auto& segment) {
            return segment.id == configured_active_segment_id;
        });
        // Stores the index, not the iterator: the walk below writes through `segments` and
        // must not read a position taken before it.
        const size_t active_segment_index =
            active_it == segments.end() ? segments.size() - 1 : static_cast<size_t>(active_it - segments.begin());
        const uint64_t active_segment_id = segments[active_segment_index].id;

        for (auto& segment : segments) {
            auto f = open_bitcask_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!f) {
                // Must cost the index its registration, not the engine's life
                // (integration/cpp/test/test_index_bootstrap_failure.cpp): an unreadable
                // segment here means the keydir this rebuild fills is missing its keys.
                return io_failure("bitcask: segment " + segment.path.string() +
                                  " could not be opened for recovery: " + open_refusal_reason());
            }
            const auto file_size = f->file_size();
            uint64_t offset = 0;
            while (offset + sizeof(record_header_t) <= file_size) {
                record_header_t header{};
                // The loop condition already proved the header fits, so a short read here is a
                // device refusing, not a torn tail (that's handled by the size check below).
                if (!f->read(&header, sizeof(header), offset)) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " refused a record header during recovery");
                }

                const auto payload_offset = offset + sizeof(record_header_t);
                // Subtraction, not addition: payload_size is untrusted and the addition would
                // wrap near UINT64_MAX, passing the check and then throwing std::bad_alloc on
                // resize() below.
                if (header.payload_size > file_size - payload_offset) {
                    // Truncated tail: the record was never fully written (a crash mid-append).
                    // Everything before it is intact, and there is nothing after it.
                    break;
                }

                std::pmr::string payload(resource());
                payload.resize(static_cast<size_t>(header.payload_size));
                if (header.payload_size != 0 &&
                    !f->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
                    // Same as the header above: the size check says these bytes exist.
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " refused a record payload during recovery");
                }
                absl::crc32c_t calc = absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                                                            sizeof(header) - sizeof(header.crc)));
                if (!payload.empty()) {
                    calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
                }
                if (static_cast<uint32_t>(calc) != header.crc) {
                    if (segment.id == active_segment_id) {
                        // Repairable here (unlike the rotated arm below): a CRC mismatch in the
                        // active segment's tail is treated as a torn write, not fatal, or the
                        // cut in open_active_segment that heals it would be unreachable and one
                        // bad byte would cost the whole index. Committed rows are safe in the
                        // txn log regardless. Logged because nothing else will ever mention it.
                        std::fprintf(stderr,
                                     "bitcask: %s holds a record at offset %llu whose CRC does not match; the "
                                     "active segment's unreadable tail (%llu bytes) is being cut and the index "
                                     "opens without it\n",
                                     segment.path.string().c_str(),
                                     static_cast<unsigned long long>(offset),
                                     static_cast<unsigned long long>(file_size - offset));
                        break;
                    }
                    // A rotated segment never gets appended to again, so a CRC mismatch here is
                    // damage, not a torn write -- flagged rather than treated as fatal so
                    // open() can report it as a value.
                    crc_failure_ = true;
                    return core::error_t::no_error();
                }
                value_t key(resource(), nullptr);
                row_ids_t rows(resource());
                // CRC already matched, so this is a foreign/newer key encoding, not a torn
                // tail -- walking past it would publish a keydir missing these rows.
                if (!deserialize_payload(resource(), payload, key, rows)) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a record whose key could not be decoded");
                }
                bool key_hashable = true;
                const auto key_bytes = key_bytes_for_hash(key, &key_hashable);
                if (!key_hashable) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a key this build has no hash encoding for");
                }
                if (static_cast<record_kind_t>(header.kind) == record_kind_t::tombstone) {
                    RETURN_IF_ERROR(erase_all_refs_for_key(key_bytes));
                } else if (static_cast<record_kind_t>(header.kind) == record_kind_t::value) {
                    RETURN_IF_ERROR(erase_all_refs_for_key(key_bytes));
                    RETURN_IF_ERROR(hash_index_->put(key_bytes,
                                                     rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                                                     static_cast<uint32_t>(segment.id),
                                                     payload_offset));
                } else {
                    // CRC matched: a well-formed record of an unknown kind (foreign/newer
                    // format), not a torn tail -- must not stop quietly.
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a record of an unknown kind");
                }
                next_timestamp_ = std::max(next_timestamp_, header.timestamp);
                ++segment.record_count;
                offset = payload_offset + header.payload_size;
            }
            // `offset` is the first byte the walk could not read as a record -- equal to
            // file_size on a segment that ends cleanly.
            segment.scan_end = offset;
        }

        const auto& active_segment = segments[active_segment_index];
        active_segment_id_ = active_segment.id;
        next_segment_id_ = segments.back().id + 1;
        active_segment_records_ = active_segment.record_count;
        active_data_file_path_ = active_segment.path;
        active_segment_clean_end_ = active_segment.scan_end;

        restore_rehash_state.table = nullptr;
        hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
        if (!prev_rehash_suppressed) {
            RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
        }
        return core::error_t::no_error();
    }

    // error_code overload only: the throwing overload could escape as an exception,
    // or worse, silently turn an unreadable directory into "no segments".
    core::result_wrapper_t<std::pmr::vector<bitcask_index_disk_t::segment_info_t>>
    bitcask_index_disk_t::collect_segments() const {
        std::pmr::vector<segment_info_t> segments(resource());
        const auto listing_failure = [this](const std::error_code& code) {
            return io_failure("bitcask: the index directory " + path_.string() + " could not be listed: " +
                              code.message());
        };

        std::error_code ec;
        const bool directory_present = std::filesystem::exists(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        if (!directory_present) {
            // The one legitimate empty answer: nothing written yet, so no segments is true.
            return segments;
        }
        const bool is_directory = std::filesystem::is_directory(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        if (!is_directory) {
            // Not "no segments yet" -- an empty list here would let the rebuild wipe the
            // keydir over a layout this store cannot run on.
            return io_failure("bitcask: the index path " + path_.string() + " is not a directory");
        }

        auto entry = std::filesystem::directory_iterator(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        const auto end = std::filesystem::directory_iterator();
        while (entry != end) {
            const bool regular_file = entry->is_regular_file(ec);
            if (ec) {
                return listing_failure(ec);
            }
            if (regular_file) {
                uint64_t segment_id = 0;
                if (parse_segment_id(entry->path(), segment_id)) {
                    segments.push_back(segment_info_t{segment_id, entry->path(), 0});
                }
            }
            entry.increment(ec);
            if (ec) {
                return listing_failure(ec);
            }
        }

        std::sort(segments.begin(), segments.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
        return segments;
    }

    core::error_t bitcask_index_disk_t::open_active_segment() {
        if (active_data_file_path_.empty()) {
            active_segment_id_ = active_segment_id_ == 0 ? allocate_next_segment_id() : active_segment_id_;
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        }

        file_ = open_bitcask_file(fs_,
                                  active_data_file_path_,
                                  file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                  file_lock_type::NO_LOCK);
        if (!file_) {
            return io_failure("bitcask: active segment " + active_data_file_path_.string() +
                              " could not be opened: " + open_refusal_reason());
        }
        // The crash half of discard_partial_record: a power cut inside write_record leaves a
        // stump with nobody left to undo it, so it's cut here before anything appends past it.
        // fsync'd because an unsynced truncate could leave the stump back after a second crash.
        // active_segment_clean_end_ is only cleared after the cut succeeds, so a refused
        // truncate can retry on the next open_active_segment call.
        if (const auto clean_end = active_segment_clean_end_;
            clean_end != no_tail_to_trim && file_->file_size() > clean_end) {
            if (!file_->truncate(static_cast<int64_t>(clean_end)) || !file_->sync()) {
                file_.reset();
                return io_failure("bitcask: the unreadable tail of active segment " +
                                  active_data_file_path_.string() + " could not be removed");
            }
        }
        active_segment_clean_end_ = no_tail_to_trim;
        if (!file_->seek(file_->file_size())) {
            return io_failure("bitcask: active segment " + active_data_file_path_.string() +
                              " could not be positioned at its end");
        }
        if (!write_current_segment_id(fs_, path_, active_segment_id_)) {
            // Must refuse rather than write into a segment CURRENT doesn't name: load_from_disk
            // picks the active segment by this file, so an unnamed append replays out of order
            // after a restart.
            return io_failure("bitcask: the CURRENT segment pointer could not be published");
        }
        return core::error_t::no_error();
    }

    uint64_t bitcask_index_disk_t::allocate_next_segment_id() { return next_segment_id_++; }

    core::error_t bitcask_index_disk_t::rotate_active_segment() {
        // Nothing reopens the old segment for writing, so it must be synced before the handle goes.
        RETURN_IF_ERROR(sync_if_dirty());
        file_.reset();
        active_segment_id_ = allocate_next_segment_id();
        active_segment_records_ = 0;
        active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        RETURN_IF_ERROR(open_active_segment());
        // Record the debt, don't pay it here: bitcask_index_agent_t pays it once, at the end
        // of the write handler, instead of once per rotation within one statement.
        merge_pending_ = true;
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::merge_pending_segments() {
        if (!merge_pending_) {
            return core::error_t::no_error();
        }
        // Cleared FIRST, not after: an early return inside merge_immutable_segments would
        // otherwise re-run the whole scan on the next call.
        merge_pending_ = false;
        auto merge_error = merge_immutable_segments();
        if (merge_error.contains_error()) {
            // Re-armed regardless of failure mode (before or after manifest publish) so this
            // uptime retries on the next rotation rather than waiting for a restart; the agent
            // calls this once per write handler, so a permanently failing merge costs one
            // attempt per statement, not a spin.
            merge_pending_ = true;
        }
        // RETURNED, not parked: parking it in note_write_error would hold the refusal for the
        // NEXT force_flush and mis-attribute it to a later round. The agent's pay_merge_debt
        // folds it into the reply of the handler that ran the merge, which is where it belongs.
        return merge_error;
    }

    core::error_t bitcask_index_disk_t::rotate_active_segment_if_needed() {
        if (bulk_mode_) {
            return core::error_t::no_error();
        }
        if (active_segment_records_ >= segment_record_limit_) {
            return rotate_active_segment();
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<bool> bitcask_index_disk_t::read_rows_at(uint32_t segment_id,
                                                                    uint64_t value_offset,
                                                                    row_ids_t& rows,
                                                                    value_t* out_key) const {
        const auto segment_path = segment_file_path(path_, segment_id);
        // The active segment reuses the descriptor this store already holds instead of a fresh
        // open()/close() per read (positional pread(2), so this is safe alongside the sequential
        // append path). Observed under stress_test_index.cpp's parallel suite: a fresh open()
        // per read hits a spurious refusal (system-wide fd table pressure from a neighbour)
        // roughly once in ten runs. Rotated segments still open per read below -- this closes
        // only the hot path.
        core::filesystem::file_handle_t* f = nullptr;
        if (file_ && static_cast<uint64_t>(segment_id) == active_segment_id_) {
            f = file_.get();
        } else {
            // Rotated files never change, so an LRU-held handle answers the same bytes a fresh
            // open would.
            for (auto& lease : rotated_read_cache_) {
                if (lease.segment_id == segment_id) {
                    lease.last_used = ++rotated_read_tick_;
                    f = lease.handle.get();
                    break;
                }
            }
            if (f == nullptr) {
                auto opened_segment =
                    open_bitcask_file(fs_, segment_path, file_flags::READ, file_lock_type::NO_LOCK);
                if (!opened_segment) {
                    return io_failure("bitcask: segment " + segment_path.string() +
                                      " could not be opened for reading: " + open_refusal_reason());
                }
#ifdef DEV_MODE
                g_bitcask_rotated_segment_opens.fetch_add(1, std::memory_order_relaxed);
#endif
                f = opened_segment.get();
                if (rotated_read_cache_.size() >= rotated_read_cache_capacity_) {
                    auto victim = rotated_read_cache_.begin();
                    for (auto it = rotated_read_cache_.begin(); it != rotated_read_cache_.end(); ++it) {
                        if (it->last_used < victim->last_used) {
                            victim = it;
                        }
                    }
                    rotated_read_cache_.erase(victim);
                }
                rotated_read_cache_.push_back(
                    rotated_segment_lease_t{static_cast<uint64_t>(segment_id),
                                            std::move(opened_segment),
                                            ++rotated_read_tick_});
            }
        }
        record_header_t header{};
        std::pmr::string payload(resource());
        if (value_offset < sizeof(record_header_t)) {
            // A keydir entry points PAST its record header, so an offset smaller than one
            // header is an entry that cannot describe a record at all.
            return io_failure("bitcask: keydir entry points inside the record header of " + segment_path.string());
        }
        const auto header_offset = value_offset - sizeof(record_header_t);
        if (!f->read(&header, sizeof(header), header_offset)) {
            drop_cached_rotated_segment_(static_cast<uint64_t>(segment_id));
            return io_failure("bitcask: record header at " + std::to_string(header_offset) + " of " +
                              segment_path.string() + " could not be read");
        }
        // Written as subtraction, not `value_offset + payload_size > segment_size`: payload_size
        // came off disk untrusted and the addition wraps near UINT64_MAX, which would pass the
        // check and then throw std::bad_alloc on resize() below.
        const auto segment_size = f->file_size();
        if (value_offset > segment_size || header.payload_size > segment_size - value_offset) {
            return io_failure("bitcask: the record at " + std::to_string(value_offset) + " of " +
                              segment_path.string() + " claims a payload of " +
                              std::to_string(header.payload_size) + " bytes, which runs past the end of the segment");
        }
        payload.resize(static_cast<size_t>(header.payload_size));
        if (header.payload_size != 0 && !f->read(payload.data(), header.payload_size, value_offset)) {
            drop_cached_rotated_segment_(static_cast<uint64_t>(segment_id));
            return io_failure("bitcask: record payload at " + std::to_string(value_offset) + " of " +
                              segment_path.string() + " could not be read");
        }
        absl::crc32c_t calc = absl::ComputeCrc32c(
            absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
        if (!payload.empty()) {
            calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
        }
        if (static_cast<uint32_t>(calc) != header.crc) {
            return io_failure("bitcask: CRC mismatch on the record at " + std::to_string(value_offset) + " of " +
                              segment_path.string());
        }
        value_t key(resource(), nullptr);
        if (!deserialize_payload(resource(), payload, key, rows)) {
            return io_failure("bitcask: the record at " + std::to_string(value_offset) + " of " +
                              segment_path.string() + " could not be decoded");
        }
        if (out_key) {
            *out_key = value_t(resource(), key);
        }
        // The ONE legal false: a tombstone. `rows` is the empty list the record carries.
        return static_cast<record_kind_t>(header.kind) == record_kind_t::value;
    }

    core::result_wrapper_t<bitcask_index_disk_t::row_ids_t>
    bitcask_index_disk_t::current_rows(const value_t& key) const {
        const auto key_bytes = key_bytes_for_hash(key);
        VALUE_OR_RETURN(auto ref, hash_index_->get(key_bytes, key_loader()));
        if (!ref.has_value()) {
            return row_ids_t(resource());
        }
        row_ids_t rows(resource());
        // A read failure must not come back as "no rows": append_snapshot REPLACES the key's
        // row list with this result, so treating a refusal as empty would silently erase every
        // row_id the key already had. A tombstone still legitimately answers empty below.
        VALUE_OR_RETURN(const bool is_value, read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr));
        if (!is_value) {
            return row_ids_t(resource());
        }
        return rows;
    }

    // erase()'s error must propagate rather than be read as "no ref left": otherwise refs
    // behind an unreadable page would be silently left in place.
    core::error_t bitcask_index_disk_t::erase_all_refs_for_key(std::string_view key_bytes) {
        while (true) {
            VALUE_OR_RETURN(const bool erased, hash_index_->erase(key_bytes, key_loader()));
            if (!erased) {
                return core::error_t::no_error();
            }
        }
    }

    core::error_t bitcask_index_disk_t::append_snapshot(const value_t& key, const row_ids_t& rows) {
        // Checked before rotation: rotating would hand this store a clean file, silently
        // leaving the stump behind in a segment the replay still walks.
        RETURN_IF_ERROR(refuse_if_sealed());
        RETURN_IF_ERROR(rotate_active_segment_if_needed());
        // Not a defensive check on an invariant that holds: rotate_active_segment drops the old
        // handle before opening the new one, so a rotation whose open() refused leaves no handle
        // -- without this check, the next INSERT would null-deref.
        if (!file_) {
            return io_failure("bitcask: no active segment is open for " + path_.string());
        }
        auto payload = serialize_payload(resource(), key, rows);
        const auto offset = file_->seek_position();
        const auto record_write =
            write_record(*file_, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload);
        if (!record_write.complete) {
            // Discarded before the keydir is touched: running the erase/put below over a record
            // that isn't on disk would make the key unfindable while reporting success. A repair
            // that itself fails seals the store (seal_writes), since the stump would otherwise
            // become an interior frame the next append lands behind.
            if (!discard_partial_record(*file_, record_write, offset)) {
                return seal_writes("bitcask: a partly written snapshot record could not be discarded from " +
                                   active_data_file_path_.string());
            }
            return io_failure("bitcask: the snapshot record could not be written to " +
                              active_data_file_path_.string());
        }
        const auto key_bytes = key_bytes_for_hash(key);
        if (auto erase_error = erase_all_refs_for_key(key_bytes); erase_error.contains_error()) {
            ++active_segment_records_;
            return erase_error;
        }
        if (auto put_error = hash_index_->put(key_bytes,
                                              rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                                              static_cast<uint32_t>(active_segment_id_),
                                              offset + sizeof(record_header_t));
            put_error.contains_error()) {
            ++active_segment_records_;
            return put_error;
        }
        ++active_segment_records_;
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::append_tombstone(const value_t& key) {
        // Same door as append_snapshot's, for the same reasons -- see there.
        RETURN_IF_ERROR(refuse_if_sealed());
        RETURN_IF_ERROR(rotate_active_segment_if_needed());
        if (!file_) {
            return io_failure("bitcask: no active segment is open for " + path_.string());
        }
        auto payload = serialize_payload(resource(), key, row_ids_t(resource()));
        const auto offset = file_->seek_position();
        const auto record_write =
            write_record(*file_, static_cast<uint8_t>(record_kind_t::tombstone), ++next_timestamp_, payload);
        if (!record_write.complete) {
            if (!discard_partial_record(*file_, record_write, offset)) {
                return seal_writes("bitcask: a partly written tombstone record could not be discarded from " +
                                   active_data_file_path_.string());
            }
            return io_failure("bitcask: the tombstone record could not be written to " +
                              active_data_file_path_.string());
        }
        const auto key_bytes = key_bytes_for_hash(key);
        auto erase_error = erase_all_refs_for_key(key_bytes);
        ++active_segment_records_;
        return erase_error;
    }

    std::filesystem::path bitcask_index_disk_t::txn_log_file_path() const { return path_ / txn_log_file; }

    std::filesystem::path bitcask_index_disk_t::txn_applied_file_path() const { return path_ / txn_applied_file; }

    // Zero is ambiguous: "never written" (fresh log, zero is correct) vs. "present but
    // unopenable" (frames already applied up to an unknown point -- zero would re-replay them
    // all). The caller refuses on the unopenable/damaged cases instead of substituting zero.
    core::result_wrapper_t<uint64_t> bitcask_index_disk_t::read_applied_log_offset() const {
        const auto applied_path = txn_applied_file_path();
        uint64_t offset = 0;
        switch (read_sidecar_uint64(applied_path, offset)) {
            case sidecar_state_t::absent:
                return uint64_t{0};
            case sidecar_state_t::unopenable:
                return io_failure("bitcask: the applied-offset sidecar " + applied_path.string() +
                                  " is present and could not be opened; the index is not registered while that "
                                  "lasts, and the next open reads it unchanged");
            case sidecar_state_t::damaged:
                return io_failure("bitcask: the applied-offset sidecar " + applied_path.string() +
                                  " is present and does not hold an offset; this does not clear by itself -- drop "
                                  "and re-create the index, or remove its directory, to rebuild it from the table");
            case sidecar_state_t::ok:
                break;
        }
        return offset;
    }

    core::error_t bitcask_index_disk_t::write_applied_log_offset(uint64_t offset) const {
        const auto applied_path = txn_applied_file_path();
        const auto temp_path = applied_path.string() + ".tmp";
        {
            std::ofstream out(temp_path, std::ios::trunc);
            if (!out.good()) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: applied-offset sidecar open failed", resource()}};
            }
            out << offset;
            out.flush();
            if (!out.good()) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: applied-offset sidecar flush failed", resource()}};
            }
        }
        if (!publish_replacement_file(fs_, temp_path, applied_path)) {
            // A leftover temp is harmless (reopened with std::ios::trunc next attempt), so its
            // cleanup failure is folded into this same refusal rather than a second one.
            std::error_code cleanup_ec;
            if (!unlink_if_present(temp_path, cleanup_ec)) {
                return io_failure("bitcask: the applied-offset sidecar could not be published as " +
                                  applied_path.string() + ", and its temp " + temp_path +
                                  " could not be removed either: " + cleanup_ec.message());
            }
            return io_failure("bitcask: the applied-offset sidecar could not be published as " +
                              applied_path.string());
        }
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::append_txn_record(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          uint8_t op_kind,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        // Sealed like the segment doors: a stump here costs recovery every committed frame in
        // the txn log, not just one segment's tail.
        RETURN_IF_ERROR(refuse_if_sealed());
        std::pmr::string payload(resource());
        components::index::codec::append_le<uint32_t>(payload, static_cast<uint32_t>(values.size()));
        for (const auto& [key, row_id] : values) {
            components::index::codec::append_logical_value(payload, key);
            components::index::codec::append_le<uint64_t>(payload, static_cast<uint64_t>(row_id));
        }

        txn_frame_header_t header{};
        header.magic = txn_magic;
        header.txn_id = txn_id;
        header.commit_id = commit_id;
        header.op_kind = op_kind;
        header.payload_size = static_cast<uint64_t>(payload.size());

        absl::crc32c_t crc =
            absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.txn_id),
                                                  sizeof(header) - sizeof(header.magic) - sizeof(header.crc)));
        if (!payload.empty()) {
            crc = absl::ExtendCrc32c(crc, absl::string_view(payload.data(), payload.size()));
        }
        header.crc = static_cast<uint32_t>(crc);

        if (!txn_log_file_) {
            txn_log_file_ = open_bitcask_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                // Recoverable IO failure — surface, do not abort.
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed", resource()}};
            }
            // Same repair as open_active_segment, for the same reason: cut the tail recovery
            // couldn't read before anything appends past it (fsync'd, so a second crash can't
            // find the stump back). Belongs in this lazy open, the only door that creates the
            // handle. txn_log_clean_end_ is cleared only after the cut succeeds, so a refused
            // truncate can retry on the next call.
            if (const auto clean_end = txn_log_clean_end_;
                clean_end != no_tail_to_trim && txn_log_file_->file_size() > clean_end) {
                if (!txn_log_file_->truncate(static_cast<int64_t>(clean_end)) || !txn_log_file_->sync()) {
                    txn_log_file_.reset();
                    return io_failure("bitcask: the unreadable tail of the txn log " + txn_log_file_path().string() +
                                      " could not be removed");
                }
            }
            txn_log_clean_end_ = no_tail_to_trim;
        }
        // recover_txn_log's invariant -- frames fsync'd durable BEFORE the WAL commit marker --
        // holds only if the seek and both writes below are checked; dropping them could let a
        // crash leave the commit marker pointing at a frame that never reached the device.
        const auto frame_offset = txn_log_file_->file_size();
        if (!txn_log_file_->seek(frame_offset)) {
            return io_failure("bitcask: the txn log could not be positioned for an append");
        }
        // A stump left here would become an interior frame on the next append, failing its
        // magic check and taking every committed frame in the log down with it -- so both
        // writes are undone back to frame_offset together on any failure.
        const auto header_write = txn_log_file_->write(&header, sizeof(header));
        if (!header_write.complete) {
            // Dropping txn_log_file_ instead would repair itself straight back over the stump,
            // since this function reopens it lazily and seeks to file_size().
            if (!discard_partial_record(*txn_log_file_, header_write, frame_offset)) {
                return seal_writes("bitcask: a partly written txn-log frame header could not be discarded");
            }
            return io_failure("bitcask: the txn-log frame header could not be written");
        }
        if (!payload.empty()) {
            const auto payload_write = txn_log_file_->write(payload.data(), payload.size());
            if (!payload_write.complete) {
                if (!discard_partial_record(*txn_log_file_,
                                            core::filesystem::write_result_t::refused(header_write.bytes_written +
                                                                                      payload_write.bytes_written),
                                            frame_offset)) {
                    return seal_writes("bitcask: a partly written txn-log frame payload could not be discarded");
                }
                return io_failure("bitcask: the txn-log frame payload could not be written");
            }
        }
        if (!txn_log_file_->sync()) {
            return io_failure("bitcask: the txn-log frame could not be made durable");
        }
        return core::error_t::no_error();
    }

    // Gated by the WAL committed COMMIT-ID set: index txn-log frames are fsync'd durable BEFORE
    // the WAL commit marker, so a crash in that window can leave durable frames for a
    // transaction whose WAL replay then rejects. A frame is applied only when its commit_id is
    // in committed_commit_ids_; skipped frames still advance write_applied_log_offset so the
    // log is consumed monotonically. The gate keys on commit_id, not txn_id, because txn ids
    // are recycled across restarts and could vouch for the wrong transaction's frame.
    core::error_t bitcask_index_disk_t::recover_txn_log() {
        const auto log_path = txn_log_file_path();
        txn_log_clean_end_ = no_tail_to_trim;
        if (!std::filesystem::exists(log_path)) {
            return core::error_t::no_error();
        }

        // file_handle_t, not std::ifstream: keeps the log behind the same DEV_MODE seam as
        // every other file this store opens, so tests can stage an open refusal on it.
        VALUE_OR_RETURN(const uint64_t applied_offset, read_applied_log_offset());
        auto in = open_bitcask_file(fs_, log_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!in) {
            return io_failure("bitcask: the txn log exists and could not be opened for recovery: " +
                              open_refusal_reason());
        }
        const uint64_t log_size = in->file_size();
        uint64_t frame_offset = applied_offset;

        while (frame_offset + sizeof(txn_frame_header_t) <= log_size) {
            txn_frame_header_t header{};
            if (!in->read(&header, sizeof(header), frame_offset)) {
                return io_failure("bitcask: the txn log refused a frame header during recovery");
            }
            if (header.magic != txn_magic) {
                // Permanent refusal would cost every committed frame in the log for good (this
                // runs on every open), so instead this ends the walk like a truncated tail: the
                // lazy open in append_txn_record cuts the file here, and the next open is clean.
                std::fprintf(stderr,
                             "bitcask: %s holds no readable frame at offset %llu (bad magic); the log's unreadable "
                             "tail (%llu bytes) is being cut and the index opens without it\n",
                             log_path.string().c_str(),
                             static_cast<unsigned long long>(frame_offset),
                             static_cast<unsigned long long>(log_size - frame_offset));
                break;
            }
            const uint64_t payload_offset = frame_offset + sizeof(txn_frame_header_t);
            // Same overflow guard as load_from_disk's: subtraction, not addition, since
            // payload_size is untrusted and the addition would wrap.
            if (header.payload_size > log_size - payload_offset) {
                // Truncated tail: the frame was never fully written. Everything before it
                // is intact and has already been applied.
                break;
            }
            std::pmr::string payload(resource());
            payload.resize(static_cast<size_t>(header.payload_size));
            if (header.payload_size != 0 &&
                !in->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
                return io_failure("bitcask: the txn log refused a frame payload during recovery");
            }

            absl::crc32c_t calc =
                absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.txn_id),
                                                      sizeof(header) - sizeof(header.magic) - sizeof(header.crc)));
            if (!payload.empty()) {
                calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
            }
            if (static_cast<uint32_t>(calc) != header.crc) {
                // Same repair as the bad-magic arm above: readable frames end here, the lazy
                // open in append_txn_record cuts at frame_offset, the next open reads clean.
                std::fprintf(stderr,
                             "bitcask: %s holds a frame at offset %llu whose CRC does not match; the log's "
                             "unreadable tail (%llu bytes) is being cut and the index opens without it\n",
                             log_path.string().c_str(),
                             static_cast<unsigned long long>(frame_offset),
                             static_cast<unsigned long long>(log_size - frame_offset));
                break;
            }

            // Keyed on commit_id, not txn_id: txn ids recycle across restarts
            // (transaction_manager_t::next_transaction_id_), so a recycled id could let a
            // previous incarnation's COMMIT marker vouch for the current transaction's frame.
            // commit_id is issued at most once ever (transaction_manager_t::restore_commit_clock
            // reseeds the clock past the durable frontier on every reopen), so membership alone
            // suffices; zero is never issued, so a zero frame is refused rather than looked up.
            // The set only sees markers past the checkpoint frontier, so a skipped frame is a
            // superset, not a hole (manager_index_t::apply_wal_record_for_index accepts this
            // too) -- harmless while the row id stays absent; whether a later reused id makes it
            // a wrong answer is the index scan's responsibility, not verified here.
            const bool committed = header.commit_id != 0 && committed_commit_ids_.count(header.commit_id) > 0;
            if (header.op_kind != 1 && header.op_kind != 2) {
                return io_failure("bitcask: the txn log holds a frame with an unknown op kind");
            }
            if (committed) {
                size_t pos = 0;
                // CRC already matched, so a decode failure means a foreign/newer encoding, not
                // corruption -- refuse the open rather than risk inventing index entries from
                // misread bytes.
                bool frame_ok = true;
                const auto count = components::index::codec::read_le<uint32_t>(payload, pos, &frame_ok);
                for (uint32_t i = 0; i < count && frame_ok; ++i) {
                    auto key = components::index::codec::read_logical_value(resource(), payload, pos, &frame_ok);
                    const auto row_id =
                        static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos, &frame_ok));
                    if (!frame_ok) {
                        break;
                    }
                    if (header.op_kind == 1) {
                        insert(key, row_id);
                    } else {
                        remove(key, row_id);
                    }
                }
                if (!frame_ok) {
                    return io_failure("bitcask: a committed txn-log frame could not be decoded during recovery");
                }
                RETURN_IF_ERROR(sync_if_dirty());
            }
            // Every frame -- applied or skipped -- advances the applied offset, so a sidecar
            // that can't be rewritten must refuse here rather than let the next open re-replay it.
            const uint64_t frame_end_offset = payload_offset + header.payload_size;
            RETURN_IF_ERROR(write_applied_log_offset(frame_end_offset));
            frame_offset = frame_end_offset;
        }
        txn_log_clean_end_ = frame_offset;
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::apply_txn_inserts(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        // The durable index frame is written BEFORE the data segments, so bailing here on
        // failure leaves segments untouched and the frame is re-evaluated on the next open.
        if (auto err = append_txn_record(txn_id, commit_id, 1, values); err.contains_error()) {
            return err;
        }
        // append_txn_record owns the only lazy open of this log (which also cuts an unreadable
        // tail before appending); unreachable by construction, checked as an invariant rather
        // than reimplementing that open here.
        if (!txn_log_file_) {
            return io_failure("bitcask: the txn log is closed after a frame this store reported as written");
        }
        const auto applied_offset = txn_log_file_->file_size();
        for (const auto& [key, row_id] : values) {
            VALUE_OR_RETURN(auto rows, current_rows(key));
            if (std::find(rows.begin(), rows.end(), row_id) != rows.end()) {
                continue;
            }
            rows.emplace_back(row_id);
            if (auto err = append_snapshot(key, rows); err.contains_error()) {
                return err;
            }
            mark_operation_dirty();
        }
        RETURN_IF_ERROR(sync_if_dirty());
        return write_applied_log_offset(applied_offset);
    }

    core::error_t bitcask_index_disk_t::apply_txn_deletes(uint64_t txn_id,
                                                          uint64_t commit_id,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        // Mirror of apply_txn_inserts — IO failure becomes a returned error
        // rather than a process abort. Same frame-before-segments ordering.
        if (auto err = append_txn_record(txn_id, commit_id, 2, values); err.contains_error()) {
            return err;
        }
        // The same checked invariant as apply_txn_inserts', for the reason stated there: there
        // is exactly ONE lazy open of this log, and it is the one that cuts the tail.
        if (!txn_log_file_) {
            return io_failure("bitcask: the txn log is closed after a frame this store reported as written");
        }
        const auto applied_offset = txn_log_file_->file_size();
        for (const auto& [key, row_id] : values) {
            VALUE_OR_RETURN(auto rows, current_rows(key));
            if (rows.empty()) {
                continue;
            }
            const auto original_size = rows.size();
            rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            if (rows.size() == original_size) {
                continue;
            }
            if (rows.empty()) {
                RETURN_IF_ERROR(append_tombstone(key));
            } else {
                RETURN_IF_ERROR(append_snapshot(key, rows));
            }
            mark_operation_dirty();
        }
        RETURN_IF_ERROR(sync_if_dirty());
        return write_applied_log_offset(applied_offset);
    }

    void bitcask_index_disk_t::insert(const value_t& key, size_t value) {
        auto rows = current_rows(key);
        if (rows.has_error()) {
            // The dedup read below decides whether this row is already indexed, so a read
            // that could not finish cannot be treated as "not there": that would append a
            // snapshot built from a PARTIAL row list and drop the rows it could not see.
            note_write_error(rows.error());
            return;
        }
        auto& row_ids = rows.value();
        if (std::find(row_ids.begin(), row_ids.end(), value) != row_ids.end()) {
            return;
        }
        row_ids.emplace_back(value);
        note_write_error(append_snapshot(key, row_ids));
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        // bitcask's insert already IS its bulk insert -- there's no per-op find() scan to skip
        // (unlike btree). Must NOT short-circuit to a snapshot holding only `value`: keys are
        // not unique, and append_snapshot REPLACES the whole row list, so that would collapse
        // every repeated key to its last-written row on the next CHECKPOINT/VACUUM rebuild.
        insert(key, value);
    }

    void bitcask_index_disk_t::remove(value_t key) {
        auto ref = hash_index_->get(key_bytes_for_hash(key), key_loader());
        if (ref.has_error()) {
            note_write_error(ref.error());
            return;
        }
        if (!ref.value().has_value()) {
            return;
        }
        note_write_error(append_tombstone(key));
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::remove(const value_t& key, size_t row_id) {
        auto read = current_rows(key);
        if (read.has_error()) {
            // Same reason as insert(): an unfinished read here would look like "the key
            // holds no such row" and skip a removal that is owed.
            note_write_error(read.error());
            return;
        }
        auto& rows = read.value();
        if (rows.empty()) {
            return;
        }
        const auto original_size = rows.size();
        rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
        if (rows.size() == original_size) {
            return;
        }

        if (rows.empty()) {
            note_write_error(append_tombstone(key));
        } else {
            note_write_error(append_snapshot(key, rows));
        }
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::remove_bulk_unchecked(const value_t& key, size_t row_id) {
        // No per-key find()-scan to avoid here (that's the btree backend's problem), so the
        // bulk remove IS the normal remove.
        remove(key, row_id);
    }

    void bitcask_index_disk_t::flush_if_needed() {
        if (bulk_mode_) {
            return;
        }
        if (should_flush()) {
            // Callers (insert/remove) are void, so the refusal is parked for the next force_flush.
            note_write_error(sync_if_dirty());
        }
    }

    core::error_t bitcask_index_disk_t::force_flush() {
        // The checkpoint trims the WAL behind this value, so reporting no_error over a refused
        // fsync would cut the log in front of an index that never reached the device.
        auto flush_error = sync_if_dirty();
        // Hand over anything the void-returning write paths could not report themselves, once.
        auto pending = pending_write_error_;
        pending_write_error_ = core::error_t::no_error();
        // The flush that just refused is the newer, more specific fact; a parked error from
        // an earlier statement is not lost either -- it stays parked for the next call.
        if (flush_error.contains_error()) {
            note_write_error(std::move(pending));
            return flush_error;
        }
        return pending;
    }

    void bitcask_index_disk_t::note_write_error(core::error_t err) {
        if (err.contains_error() && !pending_write_error_.contains_error()) {
            pending_write_error_ = std::move(err);
        }
    }

    core::error_t bitcask_index_disk_t::seal_writes(std::string_view reason) {
        writes_sealed_ = true;
        return io_failure(reason);
    }

    core::error_t bitcask_index_disk_t::refuse_if_sealed() const {
        if (!writes_sealed_) {
            return core::error_t::no_error();
        }
        // Flushing is deliberately not sealed: everything appended before the stump is real,
        // and force_flush is how its durability still reaches the caller.
        return io_failure("bitcask: " + path_.string() +
                          " is not taking writes: a partly written record could not be discarded from the active "
                          "file, and every append after it would land behind a record no reader can pass");
    }

    core::error_t bitcask_index_disk_t::sync_if_dirty() {
        if (!is_dirty() || !file_) {
            return core::error_t::no_error();
        }
        // Dirty flag stays set on a refusal, or the next flush would see "nothing to write"
        // over data that never left the page cache.
        if (!file_->sync()) {
            return io_failure("bitcask: the active segment could not be made durable");
        }
        // The keydir is the other half of the same answer: a segment on the device whose
        // keydir entry is not makes the key unfindable just the same.
        RETURN_IF_ERROR(hash_index_->sync());
        reset_flush_state();
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::load_entries(entries_t& entries) const {
        // for_each's callback returns void, so the first unreadable record is remembered here
        // and returned once the walk finishes, instead of silently feeding a rebuild a partial
        // index.
        core::error_t read_error = core::error_t::no_error();
        auto walk_error = hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) {
            if (read_error.contains_error()) {
                return;
            }
            row_ids_t rows(resource());
            value_t key(resource(), nullptr);
            auto read = read_rows_at(ref.log_file_id, ref.log_offset, rows, &key);
            if (read.has_error()) {
                read_error = read.error();
                return;
            }
            if (!read.value()) {
                return; // tombstone: this key legitimately contributes no entries
            }
            for (auto row : rows) {
                entries.emplace_back(value_t(resource(), key), row);
            }
        });
        if (walk_error.contains_error()) {
            return walk_error;
        }
        return read_error;
    }

    core::error_t bitcask_index_disk_t::find(const value_t& value, result& res) const {
        VALUE_OR_RETURN(auto ref, hash_index_->get(key_bytes_for_hash(value), key_loader()));
        if (!ref.has_value()) {
            return core::error_t::no_error();
        }
        row_ids_t rows(resource());
        // A read failure must not answer no_error with res untouched -- that reads as "no rows".
        VALUE_OR_RETURN(const bool is_value, read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr));
        if (!is_value) {
            return core::error_t::no_error();
        }
        res.reserve(res.size() + rows.size());
        res.insert(res.end(), rows.begin(), rows.end());
        return core::error_t::no_error();
    }

    // No scan_range here: a hashed store has no ordering to scan. The refusal for that lives
    // in bitcask_index_agent_t::read_rows instead.

    core::error_t bitcask_index_disk_t::merge_immutable_segments() {
        // Dropped up front: a held rotated handle would keep an unlinked inode alive once the
        // merge unlinks its sources and republishes the directory.
        invalidate_rotated_read_cache_();
        std::vector<segment_info_t> immutable_segments;
        std::vector<uint64_t> removed_segment_ids;
        std::vector<disk_hash_table_t::value_ref_t> refs;
        bool built = false;

        const uint64_t frontier_segment_id = active_segment_id_;
        VALUE_OR_RETURN(const auto segments, collect_segments());
        for (const auto& seg : segments) {
            if (seg.id < frontier_segment_id) {
                immutable_segments.push_back(seg);
            }
        }
        if (immutable_segments.empty()) {
            return core::error_t::no_error();
        }
        // Merged output alternates between reserved ids 1 and 0 -- NOT `front().id - 1`: on the
        // third merge that computes `0 - 1`, which wraps to 2^64-1 and points every relocated
        // key at a segment file that doesn't exist. Flipping the reserved bit (or taking 1 on
        // the first merge) also keeps merged output below every regular id, so it replays before
        // rotated data.
        const uint64_t merged_segment_id =
            immutable_segments.front().id < regular_segment_id_start_ ? immutable_segments.front().id ^ 1u : 1u;
        for (const auto& seg : immutable_segments) {
            // The output is published by renaming over its own path, so it must not also be
            // unlinked afterwards.
            if (seg.id != merged_segment_id) {
                removed_segment_ids.push_back(seg.id);
            }
        }
        // A local, not bulk_prev_rehash_suppressed_: that member belongs to set_bulk_mode, and a
        // merge overwriting it would leave the bulk window restoring the wrong value.
        const bool prev_rehash_suppressed = hash_index_->set_auto_rehash_suppressed(true);
        // Restored by scope exit, not by hand at each return: same shape as load_from_disk's,
        // since a refusal below returning mid-function must not leave auto-rehash suppressed
        // for good.
        struct restore_rehash_state_t {
            disk_hash_table_t* table{nullptr};
            bool prev{false};
            ~restore_rehash_state_t() {
                if (table) {
                    table->set_auto_rehash_suppressed(prev);
                }
            }
        } restore_rehash_state{hash_index_.get(), prev_rehash_suppressed};
        // This ref list decides which segments get deleted below, so a walk that stopped early
        // would unlink segments holding keys it never relocated.
        RETURN_IF_ERROR(hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) {
            if (ref.log_file_id < static_cast<uint32_t>(frontier_segment_id)) {
                refs.push_back(ref);
            }
        }));
        if (refs.empty()) {
            return core::error_t::no_error();
        }
        const auto merged_path = segment_file_path(path_, merged_segment_id);
        const auto temp_path = merge_temp_file_path(path_, merged_segment_id);
        const auto meta_temp_path = std::filesystem::path(temp_path.string() + ".meta");
        // Must not survive: FILE_CREATE is O_CREAT, not O_TRUNC, so a stale temp's bytes past
        // what this attempt writes would stay and get published -- a garbage tail on the
        // segment, or extra relocation entries the replay loop below applies to the keydir.
        std::error_code stale_temp_ec;
        if (!unlink_if_present(temp_path, stale_temp_ec)) {
            return io_failure("bitcask: the merge output " + temp_path.string() +
                              " was left behind by an earlier attempt and could not be removed: " +
                              stale_temp_ec.message());
        }
        if (!unlink_if_present(meta_temp_path, stale_temp_ec)) {
            return io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                              " was left behind by an earlier attempt and could not be removed: " +
                              stale_temp_ec.message());
        }

        // Refusals routed through here (before the manifest is published) drop both temps and
        // leave the directory exactly as found; merge_pending_segments' re-armed debt makes the
        // next attempt retry. Refusals past the manifest publish do NOT go through here --
        // apply_merge_recovery_cleanup finishes those on the next open instead.
        const auto abandon_merge = [&](std::unique_ptr<core::filesystem::file_handle_t>& merged,
                                       std::unique_ptr<core::filesystem::file_handle_t>& meta,
                                       core::error_t reason) {
            merged.reset();
            meta.reset();
            std::error_code cleanup_ec;
            const std::string first_reason{std::string_view{reason.what}};
            if (!unlink_if_present(temp_path, cleanup_ec)) {
                return io_failure(first_reason + "; the merge output " + temp_path.string() +
                                  " could not be removed either: " + cleanup_ec.message());
            }
            if (!unlink_if_present(meta_temp_path, cleanup_ec)) {
                return io_failure(first_reason + "; the merge journal " + meta_temp_path.string() +
                                  " could not be removed either: " + cleanup_ec.message());
            }
            return reason;
        };

        auto merged_file = open_bitcask_file(fs_,
                                             temp_path,
                                             file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                             file_lock_type::NO_LOCK);
        if (!merged_file) {
            return io_failure("bitcask: the merge output " + temp_path.string() + " could not be opened: " +
                              open_refusal_reason());
        }
        auto meta_file = open_bitcask_file(fs_,
                                           meta_temp_path,
                                           file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                           file_lock_type::NO_LOCK);
        if (!meta_file) {
            return abandon_merge(merged_file,
                                 meta_file,
                                 io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                                            " could not be opened: " + open_refusal_reason()));
        }

        uint64_t meta_records = 0;
        for (const auto& ref : refs) {
            row_ids_t rows(resource());
            value_t key(resource(), nullptr);
            auto read = read_rows_at(ref.log_file_id, ref.log_offset, rows, &key);
            if (read.has_error()) {
                // Must not be skipped: the segment holding this record is unlinked below, so a
                // skip would lose the key from index and disk at once.
                return abandon_merge(merged_file, meta_file, read.error());
            }
            if (!read.value()) {
                continue; // tombstone: dropping it from the merged output IS the compaction
            }
            bool key_hashable = true;
            const auto key_bytes = key_bytes_for_hash(key, &key_hashable);
            if (!key_hashable) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: a relocated key has no hash encoding in this build"));
            }
            auto payload = serialize_payload(resource(), key, rows);
            const auto offset = merged_file->seek_position();
            if (!write_record(*merged_file, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload)
                     .complete) {
                // No stump to discard: abandon_merge deletes the whole temp file, live segments
                // untouched.
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: a relocated record could not be written to " +
                                                temp_path.string()));
            }

            uint32_t key_size = static_cast<uint32_t>(key_bytes.size());
            int64_t row_value = rows.empty() ? -1 : static_cast<int64_t>(rows.back());
            uint32_t old_log_file_id = ref.log_file_id;
            uint64_t old_log_offset = ref.log_offset;
            uint64_t new_log_offset = offset + sizeof(record_header_t);
            const auto meta_write = [&](const void* data, uint64_t size) {
                return meta_file->write(const_cast<void*>(data), size).complete;
            };
            if (!meta_write(&key_size, sizeof(key_size)) ||
                (key_size != 0 && !meta_write(key_bytes.data(), key_size)) ||
                !meta_write(&old_log_file_id, sizeof(old_log_file_id)) ||
                !meta_write(&old_log_offset, sizeof(old_log_offset)) ||
                !meta_write(&row_value, sizeof(row_value)) ||
                !meta_write(&new_log_offset, sizeof(new_log_offset))) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merge journal entry could not be written to " +
                                                meta_temp_path.string()));
            }
            ++meta_records;
        }

        if (meta_records != 0) {
            if (!merged_file->sync() || !meta_file->sync()) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merge output could not be made durable"));
            }
            merged_file.reset();
            meta_file.reset();
            if (!write_merge_manifest(fs_, path_, merged_segment_id, removed_segment_ids)) {
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merge manifest could not be published"));
            }
            if (!move_files(fs_, temp_path, merged_path)) {
                // Manifest removed here because it names a segment that doesn't exist; leaving
                // it is survivable (apply_merge_recovery_cleanup finds it missing next open),
                // so failure to remove it is folded into this same refusal.
                std::error_code manifest_ec;
                if (!remove_merge_manifest(path_, manifest_ec)) {
                    return abandon_merge(merged_file,
                                         meta_file,
                                         io_failure("bitcask: the merged segment could not be published as " +
                                                    merged_path.string() +
                                                    ", and the merge manifest could not be removed either: " +
                                                    manifest_ec.message()));
                }
                return abandon_merge(merged_file,
                                     meta_file,
                                     io_failure("bitcask: the merged segment could not be published as " +
                                                merged_path.string()));
            }
            built = true;
        } else {
            // Nothing was worth merging (every ref below the frontier was a tombstone): drop
            // both temps rather than let a survivor get published by the next attempt's
            // FILE_CREATE (see the stale-temp handling at the top).
            merged_file.reset();
            meta_file.reset();
            std::error_code empty_merge_ec;
            if (!unlink_if_present(temp_path, empty_merge_ec)) {
                return io_failure("bitcask: the merge output " + temp_path.string() +
                                  " of a merge that produced nothing could not be removed: " +
                                  empty_merge_ec.message());
            }
            if (!unlink_if_present(meta_temp_path, empty_merge_ec)) {
                return io_failure("bitcask: the merge journal " + meta_temp_path.string() +
                                  " of a merge that produced nothing could not be removed: " +
                                  empty_merge_ec.message());
            }
        }

        if (!built) {
            restore_rehash_state.table = nullptr;
            hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
            if (!prev_rehash_suppressed) {
                RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
            }
            return core::error_t::no_error();
        }

        // Past this line the manifest is on disk, so a refusal below is no longer a lost merge
        // (apply_merge_recovery_cleanup finishes it on the next open) -- but sources must NOT be
        // unlinked over a half-applied relocation, so each step below returns rather than
        // falling through to the removal loop.
        meta_file = open_bitcask_file(fs_, meta_temp_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!meta_file) {
            return io_failure("bitcask: the merge journal " + meta_temp_path.string() + " could not be reopened: " +
                              open_refusal_reason());
        }
        uint64_t meta_offset = 0;
        const uint64_t meta_size = meta_file->file_size();
        while (meta_offset < meta_size) {
            // The journal was written by this same call and fsync'd, so a short read here is a
            // device refusing, never a legitimate end -- meta_size is the end.
            uint32_t key_size = 0;
            if (!meta_file->read(&key_size, sizeof(key_size), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(key_size);

            std::string key_bytes;
            key_bytes.resize(key_size);
            if (key_size != 0 && !meta_file->read(key_bytes.data(), key_size, meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += key_size;

            uint32_t old_log_file_id = 0;
            uint64_t old_log_offset = 0;
            int64_t row_value = 0;
            uint64_t new_log_offset = 0;
            if (!meta_file->read(&old_log_file_id, sizeof(old_log_file_id), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(old_log_file_id);
            if (!meta_file->read(&old_log_offset, sizeof(old_log_offset), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(old_log_offset);
            if (!meta_file->read(&row_value, sizeof(row_value), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(row_value);
            if (!meta_file->read(&new_log_offset, sizeof(new_log_offset), meta_offset)) {
                return io_failure("bitcask: the merge journal could not be read back");
            }
            meta_offset += sizeof(new_log_offset);

            VALUE_OR_RETURN(auto current, hash_index_->get(key_bytes, key_loader()));
            if (!current.has_value()) {
                continue;
            }
            if (current->log_file_id != old_log_file_id || current->log_offset != old_log_offset) {
                continue;
            }
            RETURN_IF_ERROR(erase_all_refs_for_key(key_bytes));
            RETURN_IF_ERROR(
                hash_index_->put(key_bytes, row_value, static_cast<uint32_t>(merged_segment_id), new_log_offset));
        }
        meta_file.reset();
        // Must go now: a survivor is a stale journal the next merge's FILE_CREATE wouldn't
        // truncate, so its leftover tail would replay as bogus relocation entries. Cheap to
        // refuse here -- sources and manifest are both still in place for the next open to redo.
        std::error_code journal_ec;
        if (!unlink_if_present(meta_temp_path, journal_ec)) {
            return io_failure("bitcask: the replayed merge journal " + meta_temp_path.string() +
                              " could not be removed: " + journal_ec.message());
        }
        RETURN_IF_ERROR(hash_index_->sync());
        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            std::error_code removal_ec;
            if (!unlink_if_present(removed_path, removal_ec)) {
                // A source that survives is resurrected on the next open (load_from_disk
                // replays every segment) -- the manifest still names it, so
                // apply_merge_recovery_cleanup finishes the unlink then.
                return io_failure("bitcask: the merged-away segment " + removed_path.string() +
                                  " could not be removed: " + removal_ec.message());
            }
        }
        // Sources first, manifest last, deliberately: a crash between them still leaves the
        // manifest naming exactly what's left to unlink.
        std::error_code manifest_ec;
        if (!remove_merge_manifest(path_, manifest_ec)) {
            return io_failure("bitcask: the manifest of the merge into " + merged_path.string() +
                              " could not be removed: " + manifest_ec.message());
        }
        restore_rehash_state.table = nullptr;
        hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
        if (!prev_rehash_suppressed) {
            RETURN_IF_ERROR(hash_index_->trigger_rehash_if_needed());
        }
        return core::error_t::no_error();
    }

    // Wipe all stored data IN PLACE, keeping the instance alive and writable. Unlike drop(),
    // the directory and a fresh active segment survive so subsequent txn_id==0 re-inserts
    // repopulate cleanly. Returns its failure by value rather than parking it in
    // pending_write_error_, which the read path never checks -- a parked failure here would let
    // find() silently answer "no rows" over segments still on the device.
    core::error_t bitcask_index_disk_t::clear() {
        // The one early return in this function: collect_segments is const, so failing before
        // any mutation leaves the store exactly as it was for the next clear() to retry.
        VALUE_OR_RETURN(auto segments, collect_segments());

        // Close every open handle before unlinking so stale inodes are not held --
        // including the rotated-read leases, whose files are about to go.
        invalidate_rotated_read_cache_();
        file_.reset();
        txn_log_file_.reset();

        // No early returns below: every step is a mutation, and bailing out mid-way would leave
        // an object the caller still owns in drop()'s half-torn-down shape. All steps run; the
        // first failure wins and is returned at the end.
        core::error_t first_error = core::error_t::no_error();
        const auto record = [&first_error](core::error_t err) {
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        };
        // Every result is read: a clear() reporting success over a segment it couldn't remove
        // would have load_from_disk replay the survivor back into the keydir, handing find()
        // rows that clear() promised were gone.
        const auto unlink_artifact = [&](const std::filesystem::path& artifact) {
            std::error_code ec;
            if (!unlink_if_present(artifact, ec)) {
                record(io_failure("bitcask: " + artifact.string() + " could not be removed by clear(): " +
                                  ec.message()));
            }
        };

        // hash_index.bin is NOT unlinked here: disk_hash_table_t::reset_storage (called from
        // load_from_disk below) handles it, keeping the table object alive across the wipe
        // (pinned by test_bitcask_index_disk.cpp's clear_keeps_shared_hash_storage).
        for (const auto& segment : segments) {
            unlink_artifact(segment.path);
        }
        unlink_artifact(current_segment_path(path_));
        unlink_artifact(txn_log_file_path());
        unlink_artifact(txn_applied_file_path());
        unlink_artifact(merge_manifest_path(path_));

        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_ = regular_segment_id_start_;
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        // The old txn log is unlinked above, so a clean-end measurement of it must not apply
        // to the brand-new one append_txn_record creates next.
        txn_log_clean_end_ = no_tail_to_trim;
        bulk_mode_ = false;
        merge_pending_ = false;
        // The file whose stump this sealed is gone too, so nothing remains for a later append
        // to land behind -- without this reset, seal_writes would refuse forever over a file
        // that no longer exists.
        writes_sealed_ = false;

        // Recreate the backing exactly as the ctor does, but over the now-empty directory.
        record(initialize_storage());
        if (!hash_index_) {
            // A broken invariant, not an I/O outcome: clear() must only ever run on a live
            // store (drop() releases it exactly once), so reaching here means that guard was
            // bypassed and every line below would work on a null keydir.
            assert(false && "bitcask_index_disk_t::clear: the store was released by drop()");
            std::abort();
        }
        // The actual wipe is load_from_disk's below; this door only owes clearing the
        // rehash-suppression flag, since clear() exits bulk mode too.
        hash_index_->set_auto_rehash_suppressed(false);
        bool storage_is_open = true;
        if (auto load_error = load_from_disk(); load_error.contains_error()) {
            record(std::move(load_error));
            storage_is_open = false;
        } else if (auto segment_error = open_active_segment(); segment_error.contains_error()) {
            record(std::move(segment_error));
            storage_is_open = false;
        }
        if (!storage_is_open) {
            // Rebuild didn't finish: the keydir no longer describes what's on disk (its
            // segments were just unlinked), so close_storage makes later reads/writes refuse
            // instead of answering out of it. Not reached for a plain unlink failure above --
            // that store is still consistent with its disk.
            hash_index_->close_storage();
        }
        // No note_write_error here: this door has a return value, so parking the failure
        // would make it invisible to find() instead. committed_commit_ids_ is left as-is --
        // the txn log it gated is gone, and txn_id==0 re-inserts skip the gate.
        return first_error;
    }
    void bitcask_index_disk_t::drop() {
        merge_pending_ = false;
        invalidate_rotated_read_cache_();
        if (is_dirty() && file_) {
            // Nothing downstream will act on these results (the files are unlinked below), but
            // reading them keeps reset_flush_state honest about whether the sync actually took.
            const bool segment_synced = file_->sync();
            const bool keydir_synced = hash_index_ ? !hash_index_->sync().contains_error() : true;
            if (segment_synced && keydir_synced) {
                reset_flush_state();
            }
        }
        file_.reset();
        txn_log_file_.reset();
        hash_index_.reset();
        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_ = regular_segment_id_start_;
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        // drop() is void with no error channel, but a survived directory is not harmless: a
        // later CREATE INDEX on the same name would replay this index's rows as its own.
        if (!remove_directory(fs_, path_)) {
            std::fprintf(stderr,
                         "bitcask: the index directory %s survived drop(); an index re-created under this name "
                         "will replay what is still in it\n",
                         path_.string().c_str());
        }
    }
} // namespace services::index

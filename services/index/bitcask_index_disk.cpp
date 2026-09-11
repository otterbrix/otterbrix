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
        // errno must be captured immediately after open_file, before the DEV_MODE interposer below can clobber it.
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

        // The recover gate compares commit_id, not txn_id: txn_id is reused across restarts, commit_id never is.
        // No version field -- an old build's header just fails CRC instead.
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

        // The key codec leaves `pos` unmoved on refusal, so ignoring a false return here would misread the row count.
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

        // unopenable (open refused) is transient and clears itself; damaged (opened but unparseable) does not.
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
                return sidecar_state_t::unopenable;
            }
            std::size_t removed_count = 0;
            input >> merged_segment_id >> removed_count;
            if (input.fail()) {
                return sidecar_state_t::damaged;
            }
            removed_segment_ids.clear();
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

        [[nodiscard]] bool unlink_if_present(const std::filesystem::path& artifact, std::error_code& ec) {
            ec.clear();
            std::filesystem::remove(artifact, ec);
            return !ec;
        }

        [[nodiscard]] bool remove_merge_manifest(const std::filesystem::path& directory, std::error_code& ec) {
            return unlink_if_present(merge_manifest_path(directory), ec);
        }

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

        // fsync temp, then rename over target; never unlink the target first, or a crash leaves no file at all.
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
            report_undroppable_temp(temp_path);
            return false;
        }

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

        [[nodiscard]] core::filesystem::write_result_t write_record(core::filesystem::file_handle_t& file,
                                                                    uint8_t kind,
                                                                    uint64_t timestamp,
                                                                    const std::pmr::string& payload) {
            // Value-init (`{}`) zeroes the padding bytes before they're CRC'd (same for txn_frame_header_t below).
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

        // A half-landed record would corrupt replay if left; each truncate step is checked since it's itself unsynced.
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

    // No I/O here: construction must be the step that cannot fail; open() is the step that can.
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

    // open() writes to its directory (CURRENT via temp+rename); no lock, so it must own the directory alone.
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
        return force_flush();
    }

    // Test-only construct-and-open ctor: aborts on any open() failure since a ctor has no error channel.
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
            return;
        }
        auto ignored_flush_error = force_flush();
    }

    core::result_wrapper_t<std::pmr::string> bitcask_index_disk_t::load_hash_key_at(uint32_t segment_id,
                                                                                    uint64_t value_offset) const {
        row_ids_t rows(resource());
        value_t key(resource(), nullptr);
        auto read = read_rows_at(segment_id, value_offset, rows, &key);
        if (read.has_error()) {
            return read.error();
        }
        // Not a hot path -- called ZERO times in the randomized stress profile (integer keys stay inline).
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

    std::string bitcask_index_disk_t::key_bytes_for_hash(const value_t& key, bool* ok) const {
        auto normalized = normalize_hash_key(key, core::date::timezone_offset_t{});
        return components::index::codec::encode_disk_hash_key(normalized, ok);
    }

    // None of the three refusal paths below may be swallowed: an unresolved manifest would resurrect dropped keys.
    core::error_t bitcask_index_disk_t::apply_merge_recovery_cleanup() {
        const auto manifest_path = merge_manifest_path(path_);
        std::error_code ec;

        uint64_t merged_segment_id = 0;
        std::vector<uint64_t> removed_segment_ids;
        switch (read_merge_manifest(path_, merged_segment_id, removed_segment_ids)) {
            case sidecar_state_t::absent:
                return core::error_t::no_error();
            case sidecar_state_t::unopenable:
                return io_failure("bitcask: the merge manifest " + manifest_path.string() +
                                  " is present and could not be opened; the index is not registered while that "
                                  "lasts, and the next open retries it unchanged");
            case sidecar_state_t::damaged:
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
            if (!remove_merge_manifest(path_, ec)) {
                return io_failure("bitcask: the manifest of a merge that published nothing could not be removed from " +
                                  manifest_path.string() + ": " + ec.message());
            }
            return core::error_t::no_error();
        }

        for (const auto removed_id : removed_segment_ids) {
            const auto removed_path = segment_file_path(path_, removed_id);
            if (!unlink_if_present(removed_path, ec)) {
                return io_failure("bitcask: the merged-away segment " + removed_path.string() +
                                  " could not be removed: " + ec.message());
            }
        }
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

        // The only author of the keydir: reset unconditionally, or an emptied index answers find() from a stale one.
        RETURN_IF_ERROR(hash_index_->reset_storage());

        VALUE_OR_RETURN(auto segments, collect_segments());
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

        uint64_t configured_active_segment_id = 0;
        switch (read_current_segment_id(path_, configured_active_segment_id)) {
            case sidecar_state_t::absent:
                configured_active_segment_id = segments.back().id;
                break;
            case sidecar_state_t::unopenable:
                // Must not stand in the newest segment: if CURRENT names an older one, replay would reorder appends.
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
        const size_t active_segment_index =
            active_it == segments.end() ? segments.size() - 1 : static_cast<size_t>(active_it - segments.begin());
        const uint64_t active_segment_id = segments[active_segment_index].id;

        for (auto& segment : segments) {
            auto f = open_bitcask_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!f) {
                return io_failure("bitcask: segment " + segment.path.string() +
                                  " could not be opened for recovery: " + open_refusal_reason());
            }
            const auto file_size = f->file_size();
            uint64_t offset = 0;
            while (offset + sizeof(record_header_t) <= file_size) {
                record_header_t header{};
                if (!f->read(&header, sizeof(header), offset)) {
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " refused a record header during recovery");
                }

                const auto payload_offset = offset + sizeof(record_header_t);
                // Subtraction, not addition: payload_size is untrusted and addition would wrap near UINT64_MAX.
                if (header.payload_size > file_size - payload_offset) {
                    break;
                }

                std::pmr::string payload(resource());
                payload.resize(static_cast<size_t>(header.payload_size));
                if (header.payload_size != 0 &&
                    !f->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
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
                        // A CRC mismatch in the active tail is a torn write, not fatal: open_active_segment heals it.
                        std::fprintf(stderr,
                                     "bitcask: %s holds a record at offset %llu whose CRC does not match; the "
                                     "active segment's unreadable tail (%llu bytes) is being cut and the index "
                                     "opens without it\n",
                                     segment.path.string().c_str(),
                                     static_cast<unsigned long long>(offset),
                                     static_cast<unsigned long long>(file_size - offset));
                        break;
                    }
                    // A rotated segment never gets appended to again, so a mismatch here is damage, not a torn write.
                    crc_failure_ = true;
                    return core::error_t::no_error();
                }
                value_t key(resource(), nullptr);
                row_ids_t rows(resource());
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
                    return io_failure("bitcask: segment " + segment.path.string() +
                                      " holds a record of an unknown kind");
                }
                next_timestamp_ = std::max(next_timestamp_, header.timestamp);
                ++segment.record_count;
                offset = payload_offset + header.payload_size;
            }
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
            return segments;
        }
        const bool is_directory = std::filesystem::is_directory(path_, ec);
        if (ec) {
            return listing_failure(ec);
        }
        if (!is_directory) {
            // Not "no segments yet": an empty list here would let the rebuild wipe the keydir over a bad layout.
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
        // The crash half of discard_partial_record: a stump is cut here, fsync'd so a crash can't bring it back.
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
            // Must refuse rather than write into a segment CURRENT doesn't name, or a restart replays out of order.
            return io_failure("bitcask: the CURRENT segment pointer could not be published");
        }
        return core::error_t::no_error();
    }

    uint64_t bitcask_index_disk_t::allocate_next_segment_id() { return next_segment_id_++; }

    core::error_t bitcask_index_disk_t::rotate_active_segment() {
        RETURN_IF_ERROR(sync_if_dirty());
        file_.reset();
        active_segment_id_ = allocate_next_segment_id();
        active_segment_records_ = 0;
        active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        RETURN_IF_ERROR(open_active_segment());
        // Record the debt, don't pay it here: bitcask_index_agent_t pays it once at the end of the write handler.
        merge_pending_ = true;
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::merge_pending_segments() {
        if (!merge_pending_) {
            return core::error_t::no_error();
        }
        merge_pending_ = false;
        auto merge_error = merge_immutable_segments();
        if (merge_error.contains_error()) {
            merge_pending_ = true;
        }
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
        // Reuses this store's own descriptor: a fresh open() per read refused spuriously roughly once in ten runs.
        core::filesystem::file_handle_t* f = nullptr;
        if (file_ && static_cast<uint64_t>(segment_id) == active_segment_id_) {
            f = file_.get();
        } else {
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
            return io_failure("bitcask: keydir entry points inside the record header of " + segment_path.string());
        }
        const auto header_offset = value_offset - sizeof(record_header_t);
        if (!f->read(&header, sizeof(header), header_offset)) {
            drop_cached_rotated_segment_(static_cast<uint64_t>(segment_id));
            return io_failure("bitcask: record header at " + std::to_string(header_offset) + " of " +
                              segment_path.string() + " could not be read");
        }
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
        // False means a tombstone; `rows` is then the empty list the record carries, not left untouched.
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
        // A read failure must not read as "no rows": append_snapshot replaces the list, erasing every row_id.
        VALUE_OR_RETURN(const bool is_value, read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr));
        if (!is_value) {
            return row_ids_t(resource());
        }
        return rows;
    }

    core::error_t bitcask_index_disk_t::erase_all_refs_for_key(std::string_view key_bytes) {
        while (true) {
            VALUE_OR_RETURN(const bool erased, hash_index_->erase(key_bytes, key_loader()));
            if (!erased) {
                return core::error_t::no_error();
            }
        }
    }

    core::error_t bitcask_index_disk_t::append_snapshot(const value_t& key, const row_ids_t& rows) {
        RETURN_IF_ERROR(refuse_if_sealed());
        RETURN_IF_ERROR(rotate_active_segment_if_needed());
        // Not just defensive: a rotation whose open() refused leaves no handle, and the write below would null-deref.
        if (!file_) {
            return io_failure("bitcask: no active segment is open for " + path_.string());
        }
        auto payload = serialize_payload(resource(), key, rows);
        const auto offset = file_->seek_position();
        const auto record_write =
            write_record(*file_, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload);
        if (!record_write.complete) {
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

    // Zero is ambiguous between "never written" and "present but unopenable", so the caller refuses instead.
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
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed", resource()}};
            }
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
        const auto frame_offset = txn_log_file_->file_size();
        if (!txn_log_file_->seek(frame_offset)) {
            return io_failure("bitcask: the txn log could not be positioned for an append");
        }
        const auto header_write = txn_log_file_->write(&header, sizeof(header));
        if (!header_write.complete) {
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

    // Txn-log frames land durable before the WAL commit marker; a frame applies only if committed_commit_ids_ has it.
    core::error_t bitcask_index_disk_t::recover_txn_log() {
        const auto log_path = txn_log_file_path();
        txn_log_clean_end_ = no_tail_to_trim;
        if (!std::filesystem::exists(log_path)) {
            return core::error_t::no_error();
        }

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
                std::fprintf(stderr,
                             "bitcask: %s holds no readable frame at offset %llu (bad magic); the log's unreadable "
                             "tail (%llu bytes) is being cut and the index opens without it\n",
                             log_path.string().c_str(),
                             static_cast<unsigned long long>(frame_offset),
                             static_cast<unsigned long long>(log_size - frame_offset));
                break;
            }
            const uint64_t payload_offset = frame_offset + sizeof(txn_frame_header_t);
            if (header.payload_size > log_size - payload_offset) {
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
                std::fprintf(stderr,
                             "bitcask: %s holds a frame at offset %llu whose CRC does not match; the log's "
                             "unreadable tail (%llu bytes) is being cut and the index opens without it\n",
                             log_path.string().c_str(),
                             static_cast<unsigned long long>(frame_offset),
                             static_cast<unsigned long long>(log_size - frame_offset));
                break;
            }

            // commit_id (unlike txn_id) is issued at most once ever, so set membership alone decides a frame;
            // zero is never issued and is refused rather than looked up.
            const bool committed = header.commit_id != 0 && committed_commit_ids_.count(header.commit_id) > 0;
            if (header.op_kind != 1 && header.op_kind != 2) {
                return io_failure("bitcask: the txn log holds a frame with an unknown op kind");
            }
            if (committed) {
                size_t pos = 0;
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
        // The durable index frame is written before the data segments, so bailing here leaves segments untouched.
        if (auto err = append_txn_record(txn_id, commit_id, 1, values); err.contains_error()) {
            return err;
        }
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
        if (auto err = append_txn_record(txn_id, commit_id, 2, values); err.contains_error()) {
            return err;
        }
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
        // Must not short-circuit to a snapshot holding only `value`: append_snapshot replaces the whole row list.
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
        remove(key, row_id);
    }

    void bitcask_index_disk_t::flush_if_needed() {
        if (bulk_mode_) {
            return;
        }
        if (should_flush()) {
            note_write_error(sync_if_dirty());
        }
    }

    core::error_t bitcask_index_disk_t::force_flush() {
        // The checkpoint trims the WAL behind this value, so a refused fsync must not report no_error.
        auto flush_error = sync_if_dirty();
        auto pending = pending_write_error_;
        pending_write_error_ = core::error_t::no_error();
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
        // Flushing is deliberately not sealed: force_flush is how durability still reaches the caller.
        return io_failure("bitcask: " + path_.string() +
                          " is not taking writes: a partly written record could not be discarded from the active "
                          "file, and every append after it would land behind a record no reader can pass");
    }

    core::error_t bitcask_index_disk_t::sync_if_dirty() {
        if (!is_dirty() || !file_) {
            return core::error_t::no_error();
        }
        if (!file_->sync()) {
            return io_failure("bitcask: the active segment could not be made durable");
        }
        RETURN_IF_ERROR(hash_index_->sync());
        reset_flush_state();
        return core::error_t::no_error();
    }

    core::error_t bitcask_index_disk_t::load_entries(entries_t& entries) const {
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
        VALUE_OR_RETURN(const bool is_value, read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr));
        if (!is_value) {
            return core::error_t::no_error();
        }
        res.reserve(res.size() + rows.size());
        res.insert(res.end(), rows.begin(), rows.end());
        return core::error_t::no_error();
    }

    // No scan_range here: a hashed store has no ordering to scan (bitcask_index_agent_t::read_rows refuses it).

    core::error_t bitcask_index_disk_t::merge_immutable_segments() {
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
        // Alternates between reserved ids 1 and 0, not `front().id - 1`, which wraps to 2^64-1 on the third merge.
        const uint64_t merged_segment_id =
            immutable_segments.front().id < regular_segment_id_start_ ? immutable_segments.front().id ^ 1u : 1u;
        for (const auto& seg : immutable_segments) {
            if (seg.id != merged_segment_id) {
                removed_segment_ids.push_back(seg.id);
            }
        }
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
        // Must not survive: FILE_CREATE is O_CREAT, not O_TRUNC, so a stale temp's bytes would get published.
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

        // Past this line the manifest is on disk, so sources must not be unlinked over a half-applied relocation.
        meta_file = open_bitcask_file(fs_, meta_temp_path, file_flags::READ, file_lock_type::NO_LOCK);
        if (!meta_file) {
            return io_failure("bitcask: the merge journal " + meta_temp_path.string() + " could not be reopened: " +
                              open_refusal_reason());
        }
        uint64_t meta_offset = 0;
        const uint64_t meta_size = meta_file->file_size();
        while (meta_offset < meta_size) {
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
                // A source that survives is resurrected on the next open; the manifest still names it.
                return io_failure("bitcask: the merged-away segment " + removed_path.string() +
                                  " could not be removed: " + removal_ec.message());
            }
        }
        // Sources first, manifest last: a crash between them still leaves the manifest naming what's left.
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

    // Unlike drop(), wipes in place, instance stays alive; failure returns by value, not via pending_write_error_.
    core::error_t bitcask_index_disk_t::clear() {
        VALUE_OR_RETURN(auto segments, collect_segments());

        invalidate_rotated_read_cache_();
        file_.reset();
        txn_log_file_.reset();

        // No early returns below: every step is a mutation, so all run and the first failure wins.
        core::error_t first_error = core::error_t::no_error();
        const auto record = [&first_error](core::error_t err) {
            if (err.contains_error() && !first_error.contains_error()) {
                first_error = std::move(err);
            }
        };
        const auto unlink_artifact = [&](const std::filesystem::path& artifact) {
            std::error_code ec;
            if (!unlink_if_present(artifact, ec)) {
                record(io_failure("bitcask: " + artifact.string() + " could not be removed by clear(): " +
                                  ec.message()));
            }
        };

        // hash_index.bin is not unlinked: load_from_disk's reset_storage keeps the table object alive across the wipe.
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
        txn_log_clean_end_ = no_tail_to_trim;
        bulk_mode_ = false;
        merge_pending_ = false;
        writes_sealed_ = false;

        record(initialize_storage());
        if (!hash_index_) {
            // A broken invariant, not an I/O outcome: clear() must only ever run on a live store.
            assert(false && "bitcask_index_disk_t::clear: the store was released by drop()");
            std::abort();
        }
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
            hash_index_->close_storage();
        }
        return first_error;
    }
    void bitcask_index_disk_t::drop() {
        merge_pending_ = false;
        invalidate_rotated_read_cache_();
        if (is_dirty() && file_) {
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
        // drop() has no error channel, but a survived directory isn't harmless: a later CREATE INDEX would replay it.
        if (!remove_directory(fs_, path_)) {
            std::fprintf(stderr,
                         "bitcask: the index directory %s survived drop(); an index re-created under this name "
                         "will replay what is still in it\n",
                         path_.string().c_str());
        }
    }
} // namespace services::index

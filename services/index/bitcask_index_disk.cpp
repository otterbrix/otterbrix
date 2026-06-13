#include "bitcask_index_disk.hpp"

#include "bitcask_hash_key_loader.hpp"
#include "absl/crc/crc32c.h"
#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
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
    using core::filesystem::remove_file;

    namespace {
        components::types::logical_value_t normalize_hash_key(const components::types::logical_value_t& key,
                                                              core::date::timezone_offset_t session_tz) {
            using namespace components::types;
            switch (key.type().type()) {
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT:
                    return key.cast_as(complex_logical_type(logical_type::BIGINT), session_tz);
                case logical_type::UTINYINT:
                case logical_type::USMALLINT:
                case logical_type::UINTEGER:
                case logical_type::UBIGINT:
                    return key.cast_as(complex_logical_type(logical_type::UBIGINT), session_tz);
                default:
                    return key;
            }
        }

        constexpr const char* segment_prefix = "bitcask.";
        constexpr const char* segment_suffix = ".data";
        constexpr const char* current_segment_file = "CURRENT";
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

        struct txn_frame_header_t {
            uint32_t magic;
            uint32_t crc;
            uint64_t txn_id;
            uint8_t op_kind; // 1=insert, 2=delete(row)
            uint64_t payload_size;
        };

        std::pmr::string serialize_payload(std::pmr::memory_resource* resource,
                                           const services::index::index_disk_t::value_t& key,
                                           const std::pmr::vector<size_t>& rows) {
            std::pmr::string out(resource);
            components::index::codec::append_logical_value(out, key);
            components::index::codec::append_le<uint32_t>(out, static_cast<uint32_t>(rows.size()));
            for (auto row : rows) {
                components::index::codec::append_le<uint64_t>(out, static_cast<uint64_t>(row));
            }
            return out;
        }

        void deserialize_payload(std::pmr::memory_resource* resource,
                                 const std::pmr::string& payload,
                                 services::index::index_disk_t::value_t& key,
                                 std::pmr::vector<size_t>& rows) {
            size_t pos = 0;
            key = components::index::codec::read_logical_value(resource, payload, pos);
            const auto n = components::index::codec::read_le<uint32_t>(payload, pos);
            rows.clear();
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                rows.emplace_back(static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos)));
            }
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

        bool read_current_segment_id(const std::filesystem::path& directory, uint64_t& segment_id) {
            std::ifstream input(current_segment_path(directory));
            if (!input.good()) {
                return false;
            }
            input >> segment_id;
            return !input.fail();
        }

        void write_current_segment_id(const std::filesystem::path& directory, uint64_t segment_id) {
            const auto current_path = current_segment_path(directory);
            const auto temp_path = current_path.string() + ".tmp";
            {
                std::ofstream output(temp_path, std::ios::trunc);
                if (!output.good()) {
                    // -fno-exceptions build: abort instead of throw on I/O failure.
                    assert(false && "bitcask I/O failure");
                    std::abort();
                }
                output << segment_id;
                output.flush();
                if (!output.good()) {
                    // -fno-exceptions build: abort instead of throw on I/O failure.
                    assert(false && "bitcask I/O failure");
                    std::abort();
                }
            }
            std::error_code ec;
            std::filesystem::remove(current_path, ec);
            std::filesystem::rename(temp_path, current_path);
        }

        void write_record(core::filesystem::file_handle_t& file,
                          uint8_t kind,
                          uint64_t timestamp,
                          const std::pmr::string& payload) {
            record_header_t header{0, kind, static_cast<uint64_t>(payload.size()), timestamp};

            absl::crc32c_t crc = absl::ComputeCrc32c(
                absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
            if (!payload.empty()) {
                crc = absl::ExtendCrc32c(crc, absl::string_view(payload.data(), payload.size()));
            }
            header.crc = static_cast<uint32_t>(crc);

            file.write(&header, sizeof(header));
            if (!payload.empty()) {
                file.write(const_cast<char*>(payload.data()), payload.size());
            }
        }
    } // namespace

    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit,
                                               std::pmr::set<std::uint64_t> committed_txn_ids,
                                               disk_hash_table_ptr shared_hash_index)
        : index_disk_t(flush_threshold)
        , path_(path)
        , hash_index_file_path_(path_ / hash_index_file)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , segment_record_limit_(segment_record_limit)
        , task_executor_(std::make_unique<bitcask_task_executor_t>())
        , committed_txn_ids_(committed_txn_ids.begin(), committed_txn_ids.end(), resource) {
        initialize_storage();
        if (shared_hash_index) {
            hash_index_ = std::move(shared_hash_index);
        } else {
            hash_index_ = boost::intrusive_ptr(new disk_hash_table_t(hash_index_file_path_,
                                                                     disk_hash_table_t::default_bucket_count,
                                                                     resource_));
        }
        install_hash_key_loader();
        load_from_disk();
        if (crc_failure_) {
            // Direct ctor aborts on corruption; only create() tolerates a CRC
            // mismatch and turns it into a core::error_t.
            assert(false && "bitcask I/O failure: direct ctor saw CRC mismatch");
            std::abort();
        }
        open_active_segment();
        recover_txn_log_unlocked();
    }

    core::result_wrapper_t<std::unique_ptr<bitcask_index_disk_t>>
    bitcask_index_disk_t::create(const path_t& path,
                                 std::pmr::memory_resource* resource,
                                 uint64_t flush_threshold,
                                 uint64_t segment_record_limit,
                                 std::pmr::set<std::uint64_t> committed_txn_ids) {
        // skip_load ctor does no I/O, so we can run load_from_disk() and check
        // crc_failure_ before open_active_segment(). committed_txn_ids is stored
        // by the skip_load ctor so the recover gate is armed before recovery.
        auto instance = std::unique_ptr<bitcask_index_disk_t>(new bitcask_index_disk_t(path,
                                                                                       resource,
                                                                                       flush_threshold,
                                                                                       segment_record_limit,
                                                                                       std::move(committed_txn_ids),
                                                                                       skip_load_tag{}));
        instance->load_from_disk();
        if (instance->crc_failure_) {
            return core::error_t{core::error_code_t::index_create_fail,
                                 std::pmr::string{"bitcask: CRC mismatch during recovery", resource}};
        }
        instance->open_active_segment();
        instance->recover_txn_log_unlocked();
        return instance;
    }

    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit,
                                               std::pmr::set<std::uint64_t> committed_txn_ids,
                                               skip_load_tag)
        : index_disk_t(flush_threshold)
        , path_(path)
        , hash_index_file_path_(path_ / hash_index_file)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , segment_record_limit_(segment_record_limit)
        , task_executor_(std::make_unique<bitcask_task_executor_t>())
        , committed_txn_ids_(committed_txn_ids.begin(), committed_txn_ids.end(), resource) {
        initialize_storage();
        hash_index_ = boost::intrusive_ptr(new disk_hash_table_t(hash_index_file_path_,
                                                                 disk_hash_table_t::default_bucket_count,
                                                                 resource_));
        install_hash_key_loader();
        // Caller (factory) is responsible for load_from_disk +
        // open_active_segment + recover_txn_log_unlocked.
    }

    bitcask_index_disk_t::~bitcask_index_disk_t() {
        if (hash_index_) {
            hash_index_->set_full_key_loader(nullptr);
        }
        force_flush();
    }

    void bitcask_index_disk_t::install_hash_key_loader() {
        hash_index_->set_full_key_loader(
            [this](uint32_t segment_id, uint64_t value_offset, std::string& out_key, bool lock_bitcask) {
                return lock_bitcask ? load_hash_key_at(segment_id, value_offset, out_key)
                                    : load_hash_key_at_unlocked(segment_id, value_offset, out_key);
            });
    }

    bool bitcask_index_disk_t::load_hash_key_at_unlocked(uint32_t segment_id,
                                                         uint64_t value_offset,
                                                         std::string& out_key) const {
        row_ids_t rows(resource_);
        value_t key(resource_, nullptr);
        if (!read_rows_at(segment_id, value_offset, rows, &key)) {
            return false;
        }
        out_key = key_bytes_for_hash(key);
        return true;
    }

    bool bitcask_index_disk_t::load_hash_key_at(uint32_t segment_id,
                                                uint64_t value_offset,
                                                std::string& out_key) const {
        std::shared_lock lock(mutex_);
        return load_hash_key_at_unlocked(segment_id, value_offset, out_key);
    }

    void bitcask_index_disk_t::enqueue_task(std::function<void()> task) { task_executor_->enqueue(std::move(task)); }

    void bitcask_index_disk_t::set_bulk_mode(bool enabled) {
        std::unique_lock lock(mutex_);
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
                hash_index_->trigger_rehash_if_needed();
            }
        }
        bulk_mode_ = enabled;
    }

    void bitcask_index_disk_t::initialize_storage() {
        if (!std::filesystem::exists(path_)) {
            std::filesystem::create_directories(path_);
        }
    }

    std::string bitcask_index_disk_t::key_bytes_for_hash(const value_t& key) const {
        auto normalized = normalize_hash_key(key, core::date::timezone_offset_t{});
        return components::index::codec::encode_disk_hash_key(normalized);
    }

    void bitcask_index_disk_t::load_from_disk() {
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

        auto segments = collect_segments();
        if (segments.empty()) {
            active_segment_id_ = 1;
            next_segment_id_.store(2);
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
            restore_rehash_state.table = nullptr;
            hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
            if (!prev_rehash_suppressed) {
                hash_index_->trigger_rehash_if_needed();
            }
            return;
        }

        for (auto& segment : segments) {
            auto f = open_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!f) {
                // -fno-exceptions build: abort instead of throw on I/O failure.
                assert(false && "bitcask I/O failure");
                std::abort();
            }
            const auto file_size = f->file_size();
            uint64_t offset = 0;
            while (offset + sizeof(record_header_t) <= file_size) {
                record_header_t header{};
                if (!f->read(&header, sizeof(header), offset)) {
                    break;
                }

                const auto payload_offset = offset + sizeof(record_header_t);
                if (payload_offset + header.payload_size > file_size) {
                    break;
                }

                std::pmr::string payload(resource_);
                payload.resize(static_cast<size_t>(header.payload_size));
                if (header.payload_size != 0 &&
                    !f->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
                    break;
                }
                absl::crc32c_t calc = absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                                                            sizeof(header) - sizeof(header.crc)));
                if (!payload.empty()) {
                    calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
                }
                if (static_cast<uint32_t>(calc) != header.crc) {
                    // Segment corruption: flag and return rather than abort, so
                    // create() can report a core::error_t. The direct ctor checks
                    // this flag post-load and asserts.
                    crc_failure_ = true;
                    return;
                }
                value_t key(resource_, nullptr);
                row_ids_t rows(resource_);
                deserialize_payload(resource_, payload, key, rows);
                const auto key_bytes = key_bytes_for_hash(key);
                if (static_cast<record_kind_t>(header.kind) == record_kind_t::tombstone) {
                    erase_all_refs_for_key(key_bytes);
                } else if (static_cast<record_kind_t>(header.kind) == record_kind_t::value) {
                    erase_all_refs_for_key(key_bytes);
                    hash_index_->put(key_bytes,
                                     rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                                     static_cast<uint32_t>(segment.id),
                                     payload_offset);
                } else {
                    break;
                }
                next_timestamp_ = std::max(next_timestamp_, header.timestamp);
                ++segment.record_count;
                offset = payload_offset + header.payload_size;
            }
        }

        uint64_t configured_active_segment_id = 0;
        const bool has_configured_active_segment = read_current_segment_id(path_, configured_active_segment_id);

        const auto active_it = std::find_if(segments.begin(), segments.end(), [&](const auto& segment) {
            return has_configured_active_segment && segment.id == configured_active_segment_id;
        });
        const auto& active_segment = active_it == segments.end() ? segments.back() : *active_it;
        active_segment_id_ = active_segment.id;
        next_segment_id_.store(segments.back().id + 1);
        active_segment_records_ = active_segment.record_count;
        active_data_file_path_ = active_segment.path;

        restore_rehash_state.table = nullptr;
        hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
        if (!prev_rehash_suppressed) {
            hash_index_->trigger_rehash_if_needed();
        }
    }

    std::vector<bitcask_index_disk_t::segment_info_t> bitcask_index_disk_t::collect_segments() const {
        std::vector<segment_info_t> segments;
        if (!std::filesystem::exists(path_) || !std::filesystem::is_directory(path_)) {
            return segments;
        }
        for (const auto& entry : std::filesystem::directory_iterator(path_)) {
            if (!entry.is_regular_file()) {
                continue;
            }

            uint64_t segment_id = 0;
            if (parse_segment_id(entry.path(), segment_id)) {
                segments.push_back(segment_info_t{segment_id, entry.path(), 0});
            }
        }

        std::sort(segments.begin(), segments.end(), [](const auto& lhs, const auto& rhs) { return lhs.id < rhs.id; });
        return segments;
    }

    void bitcask_index_disk_t::open_active_segment() {
        if (active_data_file_path_.empty()) {
            active_segment_id_ = active_segment_id_ == 0 ? allocate_next_segment_id() : active_segment_id_;
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        }

        file_ = open_file(fs_,
                          active_data_file_path_,
                          file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK);
        if (!file_) {
            // -fno-exceptions build: abort instead of throw on I/O failure.
            assert(false && "bitcask I/O failure");
            std::abort();
        }
        file_->seek(file_->file_size());
        write_current_segment_id(path_, active_segment_id_);
    }

    uint64_t bitcask_index_disk_t::allocate_next_segment_id() { return next_segment_id_.fetch_add(1); }

    void bitcask_index_disk_t::rotate_active_segment() {
        force_flush_unlocked();
        file_.reset();
        active_segment_id_ = allocate_next_segment_id();
        active_segment_records_ = 0;
        active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        open_active_segment();
        enqueue_task([this]() { merge_immutable_segments(); });
    }

    void bitcask_index_disk_t::rotate_active_segment_if_needed() {
        if (bulk_mode_) {
            return;
        }
        if (active_segment_records_ >= segment_record_limit_) {
            rotate_active_segment();
        }
    }

    bool bitcask_index_disk_t::read_rows_at(uint32_t segment_id,
                                            uint64_t value_offset,
                                            row_ids_t& rows,
                                            value_t* out_key) const {
        auto f = open_file(fs_, segment_file_path(path_, segment_id), file_flags::READ, file_lock_type::NO_LOCK);
        if (!f) {
            return false;
        }
        record_header_t header{};
        std::pmr::string payload(resource_);
        if (value_offset < sizeof(record_header_t)) {
            return false;
        }
        const auto header_offset = value_offset - sizeof(record_header_t);
        if (!f->read(&header, sizeof(header), header_offset)) {
            return false;
        }
        payload.resize(static_cast<size_t>(header.payload_size));
        if (header.payload_size != 0 && !f->read(payload.data(), header.payload_size, value_offset)) {
            return false;
        }
        absl::crc32c_t calc = absl::ComputeCrc32c(
            absl::string_view(reinterpret_cast<const char*>(&header.kind), sizeof(header) - sizeof(header.crc)));
        if (!payload.empty()) {
            calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
        }
        if (static_cast<uint32_t>(calc) != header.crc) {
            return false;
        }
        value_t key(resource_, nullptr);
        deserialize_payload(resource_, payload, key, rows);
        if (out_key) {
            *out_key = value_t(resource_, key);
        }
        return static_cast<record_kind_t>(header.kind) == record_kind_t::value;
    }

    bitcask_index_disk_t::row_ids_t bitcask_index_disk_t::current_rows(const value_t& key) const {
        const auto key_bytes = key_bytes_for_hash(key);
        auto ref = hash_index_->get(key_bytes, false);
        if (!ref.has_value()) {
            return row_ids_t(resource_);
        }
        row_ids_t rows(resource_);
        if (!read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr)) {
            return row_ids_t(resource_);
        }
        return rows;
    }

    void bitcask_index_disk_t::erase_all_refs_for_key(std::string_view key_bytes) {
        while (hash_index_->erase(key_bytes, false)) {
        }
    }

    void bitcask_index_disk_t::append_snapshot(const value_t& key, const row_ids_t& rows) {
        rotate_active_segment_if_needed();
        auto payload = serialize_payload(resource_, key, rows);
        const auto offset = file_->seek_position();
        write_record(*file_, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload);
        const auto key_bytes = key_bytes_for_hash(key);
        erase_all_refs_for_key(key_bytes);
        hash_index_->put(key_bytes,
                         rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                         static_cast<uint32_t>(active_segment_id_),
                         offset + sizeof(record_header_t));
        ++active_segment_records_;
    }

    void bitcask_index_disk_t::append_tombstone(const value_t& key) {
        rotate_active_segment_if_needed();
        auto payload = serialize_payload(resource_, key, row_ids_t(resource_));
        write_record(*file_, static_cast<uint8_t>(record_kind_t::tombstone), ++next_timestamp_, payload);
        const auto key_bytes = key_bytes_for_hash(key);
        erase_all_refs_for_key(key_bytes);
        ++active_segment_records_;
    }

    std::filesystem::path bitcask_index_disk_t::txn_log_file_path() const { return path_ / txn_log_file; }

    std::filesystem::path bitcask_index_disk_t::txn_applied_file_path() const { return path_ / txn_applied_file; }

    uint64_t bitcask_index_disk_t::read_applied_log_offset() const {
        std::ifstream in(txn_applied_file_path());
        uint64_t offset = 0;
        if (!in.good()) {
            return 0;
        }
        in >> offset;
        return in.fail() ? 0 : offset;
    }

    core::error_t bitcask_index_disk_t::write_applied_log_offset(uint64_t offset) const {
        const auto applied_path = txn_applied_file_path();
        const auto temp_path = applied_path.string() + ".tmp";
        {
            std::ofstream out(temp_path, std::ios::trunc);
            if (!out.good()) {
                // M3.5: a failed sidecar open is a recoverable IO failure now —
                // surface it instead of aborting the process.
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: applied-offset sidecar open failed", resource_}};
            }
            out << offset;
            out.flush();
            if (!out.good()) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: applied-offset sidecar flush failed", resource_}};
            }
        }
        std::error_code ec;
        std::filesystem::remove(applied_path, ec);
        std::filesystem::rename(temp_path, applied_path);
        return core::error_t::no_error();
    }

    core::error_t
    bitcask_index_disk_t::append_txn_record_unlocked(uint64_t txn_id,
                                                     uint8_t op_kind,
                                                     const std::vector<std::pair<value_t, size_t>>& values) {
        std::pmr::string payload(resource_);
        components::index::codec::append_le<uint32_t>(payload, static_cast<uint32_t>(values.size()));
        for (const auto& [key, row_id] : values) {
            components::index::codec::append_logical_value(payload, key);
            components::index::codec::append_le<uint64_t>(payload, static_cast<uint64_t>(row_id));
        }

        txn_frame_header_t header{};
        header.magic = txn_magic;
        header.txn_id = txn_id;
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
            txn_log_file_ = open_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                // M3.5: recoverable IO failure — surface, do not abort.
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed", resource_}};
            }
        }
        txn_log_file_->seek(txn_log_file_->file_size());
        txn_log_file_->write(&header, sizeof(header));
        if (!payload.empty()) {
            txn_log_file_->write(payload.data(), payload.size());
        }
        txn_log_file_->sync();
        return core::error_t::no_error();
    }

    // Replay the index txn log, gated by the WAL committed-txn set (M1.1).
    //
    // Invariant: index txn-log frames are fsync'd durable BEFORE the WAL commit
    // marker is written. A crash inside that window leaves durable index frames
    // for a transaction whose WAL replay rejects (no COMMIT marker). Replaying
    // such a frame would resurrect an uncommitted transaction's index entries
    // (phantom entries). The gate therefore APPLIES a frame only when its txn_id
    // is in committed_txn_ids_; every frame (applied or skipped) still advances
    // write_applied_log_offset(frame_end) so the log is consumed monotonically.
    // There is no txn_id==0 frame class — both writers are guarded txn_id!=0.
    void bitcask_index_disk_t::recover_txn_log_unlocked() {
        const auto log_path = txn_log_file_path();
        if (!std::filesystem::exists(log_path)) {
            return;
        }

        const uint64_t applied_offset = read_applied_log_offset();
        std::ifstream in(log_path, std::ios::binary);
        if (!in.good()) {
            return;
        }
        in.seekg(static_cast<std::streamoff>(applied_offset), std::ios::beg);

        while (true) {
            txn_frame_header_t header{};
            in.read(reinterpret_cast<char*>(&header), sizeof(header));
            if (!in.good()) {
                break;
            }
            if (header.magic != txn_magic) {
                // -fno-exceptions build: abort instead of throw on I/O failure.
                assert(false && "bitcask I/O failure");
                std::abort();
            }
            std::pmr::string payload(resource_);
            payload.resize(static_cast<size_t>(header.payload_size));
            if (header.payload_size != 0) {
                in.read(payload.data(), static_cast<std::streamsize>(header.payload_size));
                if (!in.good()) {
                    break; // truncated tail: ignore
                }
            }

            absl::crc32c_t calc =
                absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.txn_id),
                                                      sizeof(header) - sizeof(header.magic) - sizeof(header.crc)));
            if (!payload.empty()) {
                calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
            }
            if (static_cast<uint32_t>(calc) != header.crc) {
                // -fno-exceptions build: abort instead of throw on I/O failure.
                assert(false && "bitcask I/O failure");
                std::abort();
            }

            // Gate: apply only frames of committed transactions. A frame whose
            // txn_id never committed (its WAL commit marker did not land) is
            // skipped to avoid phantom index entries. op_kind is still validated
            // for every frame so a corrupt log still aborts.
            const bool committed = committed_txn_ids_.count(header.txn_id) > 0;
            if (header.op_kind != 1 && header.op_kind != 2) {
                // -fno-exceptions build: abort instead of throw on I/O failure.
                assert(false && "bitcask I/O failure");
                std::abort();
            }
            if (committed) {
                size_t pos = 0;
                const auto count = components::index::codec::read_le<uint32_t>(payload, pos);
                for (uint32_t i = 0; i < count; ++i) {
                    auto key = components::index::codec::read_logical_value(resource_, payload, pos);
                    const auto row_id = static_cast<size_t>(components::index::codec::read_le<uint64_t>(payload, pos));
                    if (header.op_kind == 1) {
                        insert(key, row_id);
                    } else {
                        remove(key, row_id);
                    }
                }
                force_flush_unlocked();
            }
            // Every frame — applied or skipped — advances the applied offset so
            // the log is consumed monotonically and never re-replayed. This
            // runs only inside ctor/factory recovery, which has no error
            // channel: a sidecar IO failure mid-construction is terminal, so
            // bind the result and abort on it rather than silently continuing
            // (which would re-replay the frame on the next open).
            const auto frame_end_offset = static_cast<uint64_t>(in.tellg());
            const auto offset_err = write_applied_log_offset(frame_end_offset);
            if (offset_err.contains_error()) {
                // -fno-exceptions build: abort instead of throw on I/O failure.
                assert(false && "bitcask I/O failure: applied-offset write during recovery");
                std::abort();
            }
        }
    }

    core::error_t bitcask_index_disk_t::apply_txn_inserts(uint64_t txn_id,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        std::unique_lock lock(mutex_);
        // M3.5: a txn-log append/open/sidecar IO failure is recoverable — return
        // it so the manager turns it into an index-side abort. The durable index
        // frame is written BEFORE the data segments are touched, so bailing here
        // leaves the data segments untouched and the frame is re-evaluated by the
        // recover gate on the next open (gated on the WAL commit marker).
        if (auto err = append_txn_record_unlocked(txn_id, 1, values); err.contains_error()) {
            return err;
        }
        if (!txn_log_file_) {
            txn_log_file_ = open_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed (inserts)", resource_}};
            }
        }
        const auto applied_offset = txn_log_file_->file_size();
        for (const auto& [key, row_id] : values) {
            auto rows = current_rows(key);
            if (std::find(rows.begin(), rows.end(), row_id) != rows.end()) {
                continue;
            }
            rows.emplace_back(row_id);
            append_snapshot(key, rows);
            mark_operation_dirty();
        }
        force_flush_unlocked();
        return write_applied_log_offset(applied_offset);
    }

    core::error_t bitcask_index_disk_t::apply_txn_deletes(uint64_t txn_id,
                                                          const std::vector<std::pair<value_t, size_t>>& values) {
        std::unique_lock lock(mutex_);
        // M3.5: mirror of apply_txn_inserts — IO failure becomes a returned error
        // rather than a process abort. Same frame-before-segments ordering.
        if (auto err = append_txn_record_unlocked(txn_id, 2, values); err.contains_error()) {
            return err;
        }
        if (!txn_log_file_) {
            txn_log_file_ = open_file(fs_,
                                      txn_log_file_path(),
                                      file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                      file_lock_type::NO_LOCK);
            if (!txn_log_file_) {
                return core::error_t{core::error_code_t::index_create_fail,
                                     std::pmr::string{"bitcask: txn-log open failed (deletes)", resource_}};
            }
        }
        const auto applied_offset = txn_log_file_->file_size();
        for (const auto& [key, row_id] : values) {
            auto rows = current_rows(key);
            if (rows.empty()) {
                continue;
            }
            const auto original_size = rows.size();
            rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
            if (rows.size() == original_size) {
                continue;
            }
            if (rows.empty()) {
                append_tombstone(key);
            } else {
                append_snapshot(key, rows);
            }
            mark_operation_dirty();
        }
        force_flush_unlocked();
        return write_applied_log_offset(applied_offset);
    }

    void bitcask_index_disk_t::insert(const value_t& key, size_t value) {
        std::unique_lock lock(mutex_);
        auto rows = current_rows(key);
        if (std::find(rows.begin(), rows.end(), value) != rows.end()) {
            return;
        }
        rows.emplace_back(value);
        append_snapshot(key, rows);
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::insert_bulk_unchecked(const value_t& key, size_t value) {
        std::unique_lock lock(mutex_);
        // Fast path for bulk loading: skip duplicate check and disk read.
        // Caller must ensure bulk_mode is enabled and keys are unique.
        row_ids_t rows(resource_);
        rows.emplace_back(value);
        append_snapshot(key, rows);
        mark_operation_dirty();
        // flush_if_needed() is skipped in bulk_mode anyway
    }

    void bitcask_index_disk_t::remove(value_t key) {
        std::unique_lock lock(mutex_);
        if (!hash_index_->get(key_bytes_for_hash(key), false).has_value()) {
            return;
        }
        append_tombstone(key);
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::remove(const value_t& key, size_t row_id) {
        std::unique_lock lock(mutex_);
        auto rows = current_rows(key);
        if (rows.empty()) {
            return;
        }
        const auto original_size = rows.size();
        rows.erase(std::remove(rows.begin(), rows.end(), row_id), rows.end());
        if (rows.size() == original_size) {
            return;
        }

        if (rows.empty()) {
            append_tombstone(key);
        } else {
            append_snapshot(key, rows);
        }
        mark_operation_dirty();
        flush_if_needed();
    }

    void bitcask_index_disk_t::flush_if_needed() {
        if (bulk_mode_) {
            return;
        }
        if (should_flush()) {
            force_flush_unlocked();
        }
    }

    void bitcask_index_disk_t::force_flush() {
        std::unique_lock lock(mutex_);
        force_flush_unlocked();
    }

    void bitcask_index_disk_t::force_flush_unlocked() {
        if (is_dirty() && file_) {
            file_->sync();
            hash_index_->sync();
            reset_flush_state();
        }
    }

    void bitcask_index_disk_t::load_entries(entries_t& entries) const {
        std::shared_lock lock(mutex_);
        hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) {
            row_ids_t rows(resource_);
            value_t key(resource_, nullptr);
            if (!read_rows_at(ref.log_file_id, ref.log_offset, rows, &key)) {
                return;
            }
            for (auto row : rows) {
                entries.emplace_back(value_t(resource_, key), row);
            }
        });
    }

    void bitcask_index_disk_t::find(const value_t& value, result& res) const {
        std::shared_lock lock(mutex_);
        auto ref = hash_index_->get(key_bytes_for_hash(value), false);
        if (!ref.has_value()) {
            return;
        }
        row_ids_t rows(resource_);
        if (!read_rows_at(ref->log_file_id, ref->log_offset, rows, nullptr)) {
            return;
        }
        res.reserve(res.size() + rows.size());
        res.insert(res.end(), rows.begin(), rows.end());
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::find(const value_t& value) const {
        result res;
        find(value, res);
        return res;
    }

    void bitcask_index_disk_t::lower_bound(const value_t& /*value*/, result& /*res*/) const {
        // Hashed index has no ordering — lower_bound/upper_bound are unreachable.
        assert(false && "not supported");
        std::abort();
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::lower_bound(const value_t& /*value*/) const {
        assert(false && "not supported");
        std::abort();
    }

    void bitcask_index_disk_t::upper_bound(const value_t& /*value*/, result& /*res*/) const {
        assert(false && "not supported");
        std::abort();
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::upper_bound(const value_t& /*value*/) const {
        assert(false && "not supported");
        std::abort();
    }

    void bitcask_index_disk_t::merge_immutable_segments() {
        std::unique_lock lock(mutex_);
        const uint64_t last_active_segment_id = active_segment_id_;
        auto segments = collect_segments();
        std::vector<segment_info_t> immutable_segments;
        for (const auto& segment : segments) {
            if (segment.id < last_active_segment_id) {
                immutable_segments.push_back(segment);
            }
        }
        if (immutable_segments.empty()) {
            return;
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

        const auto merged_segment_id = allocate_next_segment_id();
        const auto merged_path = segment_file_path(path_, merged_segment_id);
        const auto temp_path = merge_temp_file_path(path_, merged_segment_id);
        remove_file(fs_, temp_path);

        auto merged_file = open_file(fs_,
                                     temp_path,
                                     file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                     file_lock_type::NO_LOCK);
        if (!merged_file) {
            // -fno-exceptions build: abort instead of throw on I/O failure.
            assert(false && "bitcask I/O failure");
            std::abort();
        }

        struct merged_ref_t {
            std::string key_bytes;
            int64_t row;
            uint64_t offset;
        };

        std::vector<disk_hash_table_t::value_ref_t> refs;
        hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) { refs.push_back(ref); });
        std::vector<merged_ref_t> merged_refs;
        merged_refs.reserve(refs.size());
        for (const auto& ref : refs) {
            if (ref.log_file_id >= last_active_segment_id) {
                continue;
            }
            row_ids_t rows(resource_);
            value_t key(resource_, nullptr);
            if (!read_rows_at(ref.log_file_id, ref.log_offset, rows, &key)) {
                continue;
            }
            auto payload = serialize_payload(resource_, key, rows);
            const auto offset = merged_file->seek_position();
            write_record(*merged_file, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload);
            merged_refs.push_back(merged_ref_t{key_bytes_for_hash(key),
                                               rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                                               offset + sizeof(record_header_t)});
        }

        merged_file->sync();
        merged_file.reset();
        if (!move_files(fs_, temp_path, merged_path)) {
            // -fno-exceptions build: abort instead of throw on I/O failure.
            assert(false && "bitcask I/O failure");
            std::abort();
        }

        for (const auto& merged_ref : merged_refs) {
            erase_all_refs_for_key(merged_ref.key_bytes);
            hash_index_->put(merged_ref.key_bytes,
                             merged_ref.row,
                             static_cast<uint32_t>(merged_segment_id),
                             merged_ref.offset);
        }

        for (const auto& segment : immutable_segments) {
            remove_file(fs_, segment.path);
        }

        restore_rehash_state.table = nullptr;
        hash_index_->set_auto_rehash_suppressed(prev_rehash_suppressed);
        if (!prev_rehash_suppressed) {
            hash_index_->trigger_rehash_if_needed();
        }
    }

    void bitcask_index_disk_t::clear() {
        // Wipe all stored data IN PLACE, keeping the instance alive and
        // writable (re-initialized empty). Unlike drop(), the directory and a
        // fresh active segment survive so the subsequent txn_id==0 re-inserts
        // (direct, non-txn-log path) repopulate cleanly. Mirrors the ctor's
        // construction sequence on a wiped directory.
        //
        // Drain the background merge executor first: a queued
        // merge_immutable_segments() would otherwise race the wipe by reading
        // segments we are about to remove.
        if (task_executor_) {
            task_executor_->stop();
        }

        std::unique_lock lock(mutex_);

        // Close every open handle before unlinking so stale inodes are not held.
        file_.reset();
        txn_log_file_.reset();
        hash_index_.reset();

        // Remove all on-disk bitcask artifacts: data segments, the CURRENT
        // pointer, the txn log and its applied-offset sidecar, and the shared
        // hash_index.bin (the buckets the engine-side disk_hash facade reads).
        // std::error_code overloads — no exceptions on a -fno-exceptions build.
        for (const auto& segment : collect_segments()) {
            remove_file(fs_, segment.path);
        }
        {
            std::error_code ec;
            std::filesystem::remove(current_segment_path(path_), ec);
        }
        remove_file(fs_, txn_log_file_path());
        remove_file(fs_, txn_applied_file_path());
        remove_file(fs_, hash_index_file_path_);

        // Reset all in-memory cursors to the fresh-directory state.
        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_.store(1);
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        bulk_mode_ = false;

        // Recreate the backing exactly as the ctor does, but over the now-empty
        // directory. A fresh executor replaces the stopped one.
        task_executor_ = std::make_unique<bitcask_task_executor_t>();
        initialize_storage();
        hash_index_ = boost::intrusive_ptr(new disk_hash_table_t(hash_index_file_path_,
                                                                 disk_hash_table_t::default_bucket_count,
                                                                 resource_));
        load_from_disk();
        open_active_segment();
        // committed_txn_ids_ is intentionally left as-is: the txn log it gated
        // is gone, and txn_id==0 re-inserts take the direct path (no gate).
    }

    void bitcask_index_disk_t::drop() {
        if (task_executor_) {
            task_executor_->stop();
        }

        std::unique_lock lock(mutex_);
        if (is_dirty() && file_) {
            file_->sync();
            if (hash_index_) {
                hash_index_->sync();
            }
            reset_flush_state();
        }
        file_.reset();
        txn_log_file_.reset();
        hash_index_.reset();
        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_.store(1);
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        remove_directory(fs_, path_);
    }
} // namespace services::index

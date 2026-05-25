#include "bitcask_index_disk.hpp"

#include "absl/crc/crc32c.h"
#include <components/index/logical_value_binary_codec.hpp>

#include <algorithm>
#include <charconv>
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
        constexpr const char* segment_prefix = "bitcask.";
        constexpr const char* segment_suffix = ".data";
        constexpr const char* current_segment_file = "CURRENT";
        constexpr const char* hash_index_file = "hash_index.bin";
        constexpr unsigned segment_id_width = 6;

        struct record_header_t {
            uint32_t crc;
            uint8_t kind;
            uint64_t payload_size;
            uint64_t timestamp;
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
                    throw std::runtime_error("failed to write CURRENT temp file");
                }
                output << segment_id;
                output.flush();
                if (!output.good()) {
                    throw std::runtime_error("failed to flush CURRENT temp file");
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
                                               uint64_t segment_record_limit)
        : index_disk_t(flush_threshold)
        , path_(path)
        , hash_index_file_path_(path_ / hash_index_file)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , segment_record_limit_(segment_record_limit)
        , task_executor_(std::make_unique<bitcask_task_executor_t>()) {
        initialize_storage();
        hash_index_ = std::make_unique<disk_hash_table_t>(hash_index_file_path_);
        load_from_disk();
        open_active_segment();
    }

    bitcask_index_disk_t::~bitcask_index_disk_t() { force_flush(); }

    void bitcask_index_disk_t::enqueue_task(std::function<void()> task) { task_executor_->enqueue(std::move(task)); }

    void bitcask_index_disk_t::initialize_storage() {
        if (!std::filesystem::exists(path_)) {
            std::filesystem::create_directories(path_);
        }
    }

    std::string bitcask_index_disk_t::key_bytes_for_hash(const value_t& key) const {
        auto payload = serialize_payload(resource_, key, row_ids_t(resource_));
        return std::string(payload.data(), payload.data() + payload.size());
    }

    uint32_t bitcask_index_disk_t::segment_id_from_path(const std::filesystem::path& path) {
        uint64_t id = 0;
        if (!parse_segment_id(path, id)) {
            throw std::runtime_error("invalid bitcask segment path");
        }
        return static_cast<uint32_t>(id);
    }

    void bitcask_index_disk_t::load_from_disk() {
        auto segments = collect_segments();
        if (segments.empty()) {
            active_segment_id_ = 1;
            next_segment_id_.store(2);
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
            return;
        }

        for (auto& segment : segments) {
            auto f = open_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!f) {
                throw std::runtime_error("failed to open bitcask data file: " + segment.path.string());
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
                absl::crc32c_t calc =
                    absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                                          sizeof(header) - sizeof(header.crc)));
                if (!payload.empty()) {
                    calc = absl::ExtendCrc32c(calc, absl::string_view(payload.data(), payload.size()));
                }
                if (static_cast<uint32_t>(calc) != header.crc) {
                    throw std::runtime_error("CRC mismatch in segment " + std::to_string(segment.id));
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
            throw std::runtime_error("failed to open bitcask data file: " + active_data_file_path_.string());
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
        absl::crc32c_t calc =
            absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                                  sizeof(header) - sizeof(header.crc)));
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
        auto ref = hash_index_->get(key_bytes);
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
        while (hash_index_->erase(key_bytes)) {
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

    void bitcask_index_disk_t::remove(value_t key) {
        std::unique_lock lock(mutex_);
        if (!hash_index_->get(key_bytes_for_hash(key)).has_value()) {
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
        auto ref = hash_index_->get(key_bytes_for_hash(value));
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

    void bitcask_index_disk_t::lower_bound(const value_t& value, result& res) const {
        throw "not supported"; // not supported
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::lower_bound(const value_t& value) const {
        throw "not supported"; // not supported
    }

    void bitcask_index_disk_t::upper_bound(const value_t& value, result& res) const {
        throw "not supported"; // not supported
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::upper_bound(const value_t& value) const {
        throw "not supported"; // not supported
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
        const auto merged_segment_id = allocate_next_segment_id();
        const auto merged_path = segment_file_path(path_, merged_segment_id);
        const auto temp_path = merge_temp_file_path(path_, merged_segment_id);
        remove_file(fs_, temp_path);

        auto merged_file = open_file(fs_,
                                     temp_path,
                                     file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                     file_lock_type::NO_LOCK);
        if (!merged_file) {
            throw std::runtime_error("failed to create merged bitcask data file");
        }

        std::vector<disk_hash_table_t::value_ref_t> refs;
        hash_index_->for_each([&](const disk_hash_table_t::value_ref_t& ref) {
            refs.push_back(ref);
        });
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
            hash_index_->put(key_bytes_for_hash(key),
                             rows.empty() ? -1 : static_cast<int64_t>(rows.back()),
                             static_cast<uint32_t>(merged_segment_id),
                             offset + sizeof(record_header_t));
        }

        merged_file->sync();
        merged_file.reset();
        if (!move_files(fs_, temp_path, merged_path)) {
            throw std::runtime_error("failed to publish merged segment");
        }
        for (const auto& segment : immutable_segments) {
            remove_file(fs_, segment.path);
        }
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

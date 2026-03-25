#include "bitcask_index_disk.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <algorithm>
#include <charconv>
#include <cstring>
#include <shared_mutex>
#include <stdexcept>
#include <vector>
#include "absl/crc/crc32c.h"

namespace services::index {

    using components::serializer::msgpack_deserializer_t;
    using components::serializer::msgpack_serializer_t;
    using core::filesystem::create_directory;
    using core::filesystem::directory_exists;
    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;
    using core::filesystem::move_files;
    using core::filesystem::open_file;
    using core::filesystem::remove_directory;
    using core::filesystem::remove_file;

    namespace {
        constexpr const char* segment_prefix = "bitcask.";
        constexpr const char* segment_suffix = ".data";

        struct record_header_t {
            uint32_t crc;
            uint8_t kind;
            uint64_t payload_size;
            uint64_t timestamp;
        };

        std::pmr::string serialize_payload(std::pmr::memory_resource* resource,
                                           const services::index::index_disk_t::value_t& key,
                                           const std::pmr::vector<size_t>& rows) {
            msgpack_serializer_t serializer(resource);
            serializer.start_array(2);
            key.serialize(&serializer);
            serializer.start_array(rows.size());
            for (auto row : rows) {
                serializer.append(static_cast<uint64_t>(row));
            }
            serializer.end_array();
            serializer.end_array();
            return serializer.result();
        }

        void deserialize_payload(std::pmr::memory_resource* resource,
                                 const std::pmr::string& payload,
                                 services::index::index_disk_t::value_t& key,
                                 std::pmr::vector<size_t>& rows) {
            msgpack_deserializer_t deserializer(payload);

            deserializer.advance_array(0);
            key = services::index::index_disk_t::value_t::deserialize(resource, &deserializer);
            deserializer.pop_array();

            rows.clear();
            deserializer.advance_array(1);
            rows.reserve(deserializer.current_array_size());
            for (size_t i = 0; i < deserializer.current_array_size(); ++i) {
                rows.emplace_back(static_cast<size_t>(deserializer.deserialize_uint64(i)));
            }
            deserializer.pop_array();
        }

        std::filesystem::path segment_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            std::ostringstream oss;
            oss << segment_prefix << std::setw(6) << std::setfill('0') << segment_id << segment_suffix;
            return directory / oss.str();
        }

        std::filesystem::path merge_temp_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            return segment_file_path(directory, segment_id).string() + ".merge";
        }

        bool parse_segment_id(const std::filesystem::path& path, uint64_t& segment_id) {
            const auto filename = path.filename().string();
            const std::string_view filename_sv{filename};
            constexpr std::string_view prefix = segment_prefix;
            constexpr std::string_view suffix = segment_suffix;

            if (!filename_sv.starts_with(prefix) || !filename_sv.ends_with(suffix))
                return false;

            const std::string_view digits = filename_sv.substr(prefix.size(), filename_sv.size() - prefix.size() - suffix.size());
            if (digits.empty())
                return false;

            const auto [ptr, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), segment_id);
            return ec == std::errc() && ptr == digits.data() + digits.size();
        }

        void write_record(core::filesystem::file_handle_t& file,
                  uint8_t kind,
                  uint64_t timestamp,
                  const std::pmr::string& payload) {
            record_header_t header{0, kind, static_cast<uint64_t>(payload.size()), timestamp};

            absl::crc32c_t crc = absl::ComputeCrc32c(
                absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                  sizeof(header) - sizeof(header.crc))
            );
            if (!payload.empty()) {
                crc = absl::ExtendCrc32c(crc,
                                         absl::string_view(payload.data(), payload.size()));
            }
            header.crc = static_cast<uint32_t>(crc);

            file.write(&header, sizeof(header));
            if (!payload.empty()) {
                file.write(const_cast<char*>(payload.data()), payload.size());
            }
        }
    }

    bitcask_index_disk_t::bitcask_index_disk_t(const path_t& path,
                                               std::pmr::memory_resource* resource,
                                               uint64_t flush_threshold,
                                               uint64_t segment_record_limit)
        : index_disk_t(flush_threshold)
        , path_(path)
        , active_data_file_path_()
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , file_(nullptr)
        , index_()
        , keydir_()
        , segment_record_limit_(segment_record_limit) {
        initialize_storage();
        load_from_disk();
        open_active_segment();
    }

    bitcask_index_disk_t::~bitcask_index_disk_t() { force_flush(); }

    void bitcask_index_disk_t::initialize_storage() {
        if (!std::filesystem::exists(path_)) {
            std::filesystem::create_directories(path_);
        }
    }

    void bitcask_index_disk_t::load_from_disk() {
        auto segments = collect_segments();
        if (segments.empty()) {
            active_segment_id_ = 1;
            active_segment_records_ = 0;
            active_data_file_path_ = segment_file_path(path_, active_segment_id_);
            return;
        }

        for (auto& segment : segments) {
            auto segment_file = open_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!segment_file) {
                throw std::runtime_error("failed to open bitcask data file: " + segment.path.string());
            }

            const auto file_size = segment_file->file_size();
            uint64_t offset = 0;
            while (offset + sizeof(record_header_t) <= file_size) {
                record_header_t header{};
                if (!segment_file->read(&header, sizeof(header), offset)) {
                    break;
                }

                const auto payload_offset = offset + sizeof(record_header_t);
                if (payload_offset + header.payload_size > file_size) {
                    break;
                }

                std::pmr::string payload(resource_);
                payload.resize(static_cast<size_t>(header.payload_size));
                if (header.payload_size != 0 &&
                    !segment_file->read(payload.data(), static_cast<uint64_t>(header.payload_size), payload_offset)) {
                    break;
                }

                {
                    absl::crc32c_t calc_crc = absl::ComputeCrc32c(
                        absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                          sizeof(header) - sizeof(header.crc))
                    );
                    if (!payload.empty()) {
                        calc_crc = absl::ExtendCrc32c(calc_crc,
                                                      absl::string_view(payload.data(), payload.size()));
                    }
                    if (static_cast<uint32_t>(calc_crc) != header.crc) {
                        throw std::runtime_error("CRC mismatch in segment " + std::to_string(segment.id) +
                                                 " at offset " + std::to_string(offset));
                    }
                }
                value_t key(resource_, nullptr);
                row_ids_t rows(resource_);
                deserialize_payload(resource_, payload, key, rows);

                next_timestamp_ = std::max(next_timestamp_, header.timestamp);
                if (static_cast<record_kind_t>(header.kind) == record_kind_t::tombstone) {
                    erase_state(key);
                } else if (static_cast<record_kind_t>(header.kind) == record_kind_t::value) {
                    upsert_state(key,
                                 rows,
                                 keydir_entry_t{segment.id,
                                                payload_offset,
                                                static_cast<uint64_t>(header.payload_size),
                                                header.timestamp});
                } else {
                    break;
                }

                ++segment.record_count;
                offset = payload_offset + header.payload_size;
            }
        }

        const auto& last_segment = segments.back();
        active_segment_id_ = last_segment.id;
        active_segment_records_ = last_segment.record_count;
        active_data_file_path_ = last_segment.path;
    }

    std::vector<bitcask_index_disk_t::segment_info_t> bitcask_index_disk_t::collect_segments() const {
        std::vector<segment_info_t> segments;
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
            active_segment_id_ = active_segment_id_ == 0 ? 1 : active_segment_id_;
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
    }

    void bitcask_index_disk_t::rotate_active_segment() {
        force_flush_unlocked();
        file_.reset();
        ++active_segment_id_;
        active_segment_records_ = 0;
        active_data_file_path_ = segment_file_path(path_, active_segment_id_);
        open_active_segment();
        merge_immutable_segments();
    }

    void bitcask_index_disk_t::rotate_active_segment_if_needed() {
        if (active_segment_records_ >= segment_record_limit_) {
            rotate_active_segment();
        }
    }

    void bitcask_index_disk_t::merge_immutable_segments() {
        auto segments = collect_segments();
        std::vector<segment_info_t> immutable_segments;
        immutable_segments.reserve(segments.size());
        for (const auto& segment : segments) {
            if (segment.id != active_segment_id_) {
                immutable_segments.push_back(segment);
            }
        }
        if (immutable_segments.empty()) {
            return;
        }

        const auto merged_segment_id = immutable_segments.front().id;
        const auto merged_path = segment_file_path(path_, merged_segment_id);
        const auto temp_path = merge_temp_file_path(path_, merged_segment_id);
        remove_file(fs_, temp_path);

        auto merged_file = open_file(fs_,
                                     temp_path,
                                     file_flags::READ | file_flags::WRITE | file_flags::FILE_CREATE,
                                     file_lock_type::NO_LOCK);
        if (!merged_file) {
            throw std::runtime_error("failed to create merged bitcask data file: " + temp_path.string());
        }

        std::map<uint64_t, std::unique_ptr<core::filesystem::file_handle_t>> segment_files;
        for (const auto& segment : immutable_segments) {
            auto segment_file = open_file(fs_, segment.path, file_flags::READ, file_lock_type::NO_LOCK);
            if (!segment_file) {
                throw std::runtime_error("failed to open immutable bitcask data file: " + segment.path.string());
            }
            segment_files.emplace(segment.id, std::move(segment_file));
        }

        std::vector<std::pair<value_t, keydir_entry_t>> live_entries;
        live_entries.reserve(keydir_.size());
        for (const auto& [key, entry] : keydir_) {
            if (entry.segment_id == active_segment_id_) {
                continue;
            }
            if (!segment_files.contains(entry.segment_id)) {
                continue;
            }
            live_entries.emplace_back(value_t(resource_, key), entry);
        }

        std::map<value_t, keydir_entry_t, std::less<>> updated_entries;
        for (const auto& [key, entry] : live_entries) {
            auto segment_it = segment_files.find(entry.segment_id);
            if (segment_it == segment_files.end()) {
                continue;
            }

            std::pmr::string payload(resource_);
            payload.resize(static_cast<size_t>(entry.value_size));
            if (entry.value_size != 0 &&
                !segment_it->second->read(payload.data(), entry.value_size, entry.value_offset)) {
                throw std::runtime_error("failed to read immutable bitcask payload during merge");
            }

            const auto offset = merged_file->seek_position();
            write_record(*merged_file,
                         static_cast<uint8_t>(record_kind_t::value),
                         entry.timestamp,
                         payload);

            updated_entries.emplace(value_t(resource_, key),
                                    keydir_entry_t{merged_segment_id,
                                                   offset + sizeof(record_header_t),
                                                   entry.value_size,
                                                   entry.timestamp});
        }

        merged_file->sync();
        merged_file.reset();

        for (const auto& segment : immutable_segments) {
            remove_file(fs_, segment.path);
        }
        if (!move_files(fs_, temp_path, merged_path)) {
            throw std::runtime_error("failed to publish merged bitcask data file: " + merged_path.string());
        }

        for (auto& [key, entry] : updated_entries) {
            auto keydir_it = keydir_.find(key);
            if (keydir_it != keydir_.end()) {
                keydir_it->second = entry;
            }
        }
    }

    bitcask_index_disk_t::row_ids_t bitcask_index_disk_t::clone_rows(const row_ids_t& rows) const {
        row_ids_t clone(resource_);
        clone.reserve(rows.size());
        clone.insert(clone.end(), rows.begin(), rows.end());
        return clone;
    }

    bitcask_index_disk_t::row_ids_t bitcask_index_disk_t::current_rows(const value_t& key) const {
        const auto it = index_.find(key);
        if (it == index_.end()) {
            return row_ids_t(resource_);
        }
        return clone_rows(it->second);
    }

    void bitcask_index_disk_t::append_snapshot(const value_t& key, const row_ids_t& rows) {
        if (!file_) {
            return;
        }

        rotate_active_segment_if_needed();
        auto payload = serialize_payload(resource_, key, rows);
        const auto offset = file_->seek_position();

        write_record(*file_,
                     static_cast<uint8_t>(record_kind_t::value),
                     ++next_timestamp_,
                     payload);

        upsert_state(key,
                     rows,
                     keydir_entry_t{active_segment_id_,
                                    offset + sizeof(record_header_t),
                                    static_cast<uint64_t>(payload.size()),
                                    next_timestamp_});
        ++active_segment_records_;
    }

    void bitcask_index_disk_t::append_tombstone(const value_t& key) {
        if (!file_) {
            return;
        }

        rotate_active_segment_if_needed();
        auto payload = serialize_payload(resource_, key, row_ids_t(resource_));

        write_record(*file_,
                     static_cast<uint8_t>(record_kind_t::tombstone),
                     ++next_timestamp_,
                     payload);

        erase_state(key);
        ++active_segment_records_;
    }

    void bitcask_index_disk_t::upsert_state(const value_t& key, const row_ids_t& rows, const keydir_entry_t& entry) {
        auto index_it = index_.find(key);
        if (index_it == index_.end()) {
            index_it = index_.emplace(value_t(resource_, key), clone_rows(rows)).first;
        } else {
            index_it->second = clone_rows(rows);
        }

        auto keydir_it = keydir_.find(index_it->first);
        if (keydir_it == keydir_.end()) {
            keydir_.emplace(value_t(resource_, key), entry);
        } else {
            keydir_it->second = entry;
        }
    }

    void bitcask_index_disk_t::erase_state(const value_t& key) {
        index_.erase(key);
        keydir_.erase(key);
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
        if (index_.find(key) == index_.end()) {
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
            reset_flush_state();
        }
    }

    void bitcask_index_disk_t::load_entries(entries_t& entries) const {
        std::shared_lock lock(mutex_);
        for (const auto& [key, rows] : index_) {
            entries.reserve(entries.size() + rows.size());
            for (auto row_id : rows) {
                entries.emplace_back(value_t(resource_, key), row_id);
            }
        }
    }

    void bitcask_index_disk_t::find(const value_t& value, result& res) const {
        std::shared_lock lock(mutex_);
        const auto it = index_.find(value);
        if (it == index_.end()) {
            return;
        }
        res.reserve(res.size() + it->second.size());
        res.insert(res.end(), it->second.begin(), it->second.end());
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::find(const value_t& value) const {
        bitcask_index_disk_t::result res;
        find(value, res);
        return res;
    }

    void bitcask_index_disk_t::lower_bound(const value_t& value, result& res) const {
        std::shared_lock lock(mutex_);
        for (auto it = index_.begin(); it != index_.lower_bound(value); ++it) {
            res.reserve(res.size() + it->second.size());
            res.insert(res.end(), it->second.begin(), it->second.end());
        }
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::lower_bound(const value_t& value) const {
        bitcask_index_disk_t::result res;
        lower_bound(value, res);
        return res;
    }

    void bitcask_index_disk_t::upper_bound(const value_t& value, result& res) const {
        std::shared_lock lock(mutex_);
        for (auto it = index_.upper_bound(value); it != index_.end(); ++it) {
            res.reserve(res.size() + it->second.size());
            res.insert(res.end(), it->second.begin(), it->second.end());
        }
    }

    bitcask_index_disk_t::result bitcask_index_disk_t::upper_bound(const value_t& value) const {
        bitcask_index_disk_t::result res;
        upper_bound(value, res);
        return res;
    }

    void bitcask_index_disk_t::drop() {
        std::unique_lock lock(mutex_);
        force_flush_unlocked();
        file_.reset();
        index_.clear();
        keydir_.clear();
        reset_flush_state();
        next_timestamp_ = 0;
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        remove_directory(fs_, path_);
    }

}

#include "bitcask_index_disk.hpp"

#include "absl/crc/crc32c.h"
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
        constexpr const char* current_segment_file = "CURRENT";
        constexpr unsigned SEGMENT_ID_WIDTH = 6;

        struct record_header_t {
            uint32_t crc;
            uint8_t kind;
            uint64_t payload_size;
            uint64_t timestamp;
        };

        // Minimal binary codec for bitcask payloads. The HEAD branch removed the
        // msgpack_serializer_t / msgpack_deserializer_t wrappers in P2.8/P3.7;
        // bitcask only stores scalar keys (single-column index) + a row-id list,
        // so we ship a self-contained typed-byte codec here instead of pulling
        // the whole serializer back. Format:
        //   [key_type: uint8][key_payload: type-dependent]
        //   [row_count: uint32 LE]
        //   [row_id_1..N: uint64 LE]
        // Composite/complex logical_types are not persisted by bitcask (single-
        // column hash index only); attempts to serialize them are rejected with
        // an exception so a higher layer can fall back to btree.
        using lt = components::types::logical_type;

        template<typename T>
        void buf_append_le(std::pmr::string& out, T v) {
            unsigned char bytes[sizeof(T)];
            std::memcpy(bytes, &v, sizeof(T));
            out.append(reinterpret_cast<const char*>(bytes), sizeof(T));
        }

        template<typename T>
        T buf_read_le(const std::pmr::string& in, size_t& pos) {
            if (pos + sizeof(T) > in.size()) {
                throw std::runtime_error("bitcask payload: short read");
            }
            T v{};
            std::memcpy(&v, in.data() + pos, sizeof(T));
            pos += sizeof(T);
            return v;
        }

        void write_value(std::pmr::string& out, const services::index::index_disk_t::value_t& key) {
            const auto t = key.type().type();
            buf_append_le<uint8_t>(out, static_cast<uint8_t>(t));
            switch (t) {
                case lt::NA:
                    break;
                case lt::BOOLEAN:
                    buf_append_le<uint8_t>(out, key.value<bool>() ? 1 : 0);
                    break;
                case lt::TINYINT:
                    buf_append_le<int8_t>(out, key.value<int8_t>());
                    break;
                case lt::UTINYINT:
                    buf_append_le<uint8_t>(out, key.value<uint8_t>());
                    break;
                case lt::SMALLINT:
                    buf_append_le<int16_t>(out, key.value<int16_t>());
                    break;
                case lt::USMALLINT:
                    buf_append_le<uint16_t>(out, key.value<uint16_t>());
                    break;
                case lt::INTEGER:
                    buf_append_le<int32_t>(out, key.value<int32_t>());
                    break;
                case lt::UINTEGER:
                    buf_append_le<uint32_t>(out, key.value<uint32_t>());
                    break;
                case lt::BIGINT:
                    buf_append_le<int64_t>(out, key.value<int64_t>());
                    break;
                case lt::UBIGINT:
                    buf_append_le<uint64_t>(out, key.value<uint64_t>());
                    break;
                case lt::FLOAT:
                    buf_append_le<float>(out, key.value<float>());
                    break;
                case lt::DOUBLE:
                    buf_append_le<double>(out, key.value<double>());
                    break;
                case lt::STRING_LITERAL: {
                    auto s = key.value<std::string_view>();
                    buf_append_le<uint32_t>(out, static_cast<uint32_t>(s.size()));
                    out.append(s.data(), s.size());
                    break;
                }
                default:
                    throw std::runtime_error("bitcask: unsupported key type for binary codec");
            }
        }

        services::index::index_disk_t::value_t
        read_value(std::pmr::memory_resource* resource, const std::pmr::string& in, size_t& pos) {
            using value_t = services::index::index_disk_t::value_t;
            const auto t = static_cast<lt>(buf_read_le<uint8_t>(in, pos));
            switch (t) {
                case lt::NA:
                    return value_t(resource, components::types::complex_logical_type{lt::NA});
                case lt::BOOLEAN:
                    return value_t(resource, buf_read_le<uint8_t>(in, pos) != 0);
                case lt::TINYINT:
                    return value_t(resource, buf_read_le<int8_t>(in, pos));
                case lt::UTINYINT:
                    return value_t(resource, buf_read_le<uint8_t>(in, pos));
                case lt::SMALLINT:
                    return value_t(resource, buf_read_le<int16_t>(in, pos));
                case lt::USMALLINT:
                    return value_t(resource, buf_read_le<uint16_t>(in, pos));
                case lt::INTEGER:
                    return value_t(resource, buf_read_le<int32_t>(in, pos));
                case lt::UINTEGER:
                    return value_t(resource, buf_read_le<uint32_t>(in, pos));
                case lt::BIGINT:
                    return value_t(resource, buf_read_le<int64_t>(in, pos));
                case lt::UBIGINT:
                    return value_t(resource, buf_read_le<uint64_t>(in, pos));
                case lt::FLOAT:
                    return value_t(resource, buf_read_le<float>(in, pos));
                case lt::DOUBLE:
                    return value_t(resource, buf_read_le<double>(in, pos));
                case lt::STRING_LITERAL: {
                    const auto n = buf_read_le<uint32_t>(in, pos);
                    if (pos + n > in.size()) {
                        throw std::runtime_error("bitcask payload: string overrun");
                    }
                    std::pmr::string s(in.data() + pos, n, resource);
                    pos += n;
                    return value_t(resource, std::move(s));
                }
                default:
                    throw std::runtime_error("bitcask: unsupported key type during decode");
            }
        }

        std::pmr::string serialize_payload(std::pmr::memory_resource* resource,
                                           const services::index::index_disk_t::value_t& key,
                                           const std::pmr::vector<size_t>& rows) {
            std::pmr::string out(resource);
            write_value(out, key);
            buf_append_le<uint32_t>(out, static_cast<uint32_t>(rows.size()));
            for (auto row : rows) {
                buf_append_le<uint64_t>(out, static_cast<uint64_t>(row));
            }
            return out;
        }

        void deserialize_payload(std::pmr::memory_resource* resource,
                                 const std::pmr::string& payload,
                                 services::index::index_disk_t::value_t& key,
                                 std::pmr::vector<size_t>& rows) {
            size_t pos = 0;
            key = read_value(resource, payload, pos);
            const auto n = buf_read_le<uint32_t>(payload, pos);
            rows.clear();
            rows.reserve(n);
            for (uint32_t i = 0; i < n; ++i) {
                rows.emplace_back(static_cast<size_t>(buf_read_le<uint64_t>(payload, pos)));
            }
        }

        std::filesystem::path segment_file_path(const std::filesystem::path& directory, uint64_t segment_id) {
            std::ostringstream oss;
            oss << segment_prefix << std::setw(SEGMENT_ID_WIDTH) << std::setfill('0') << segment_id << segment_suffix;
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

            if (!filename_sv.starts_with(prefix) || !filename_sv.ends_with(suffix))
                return false;

            const std::string_view digits =
                filename_sv.substr(prefix.size(), filename_sv.size() - prefix.size() - suffix.size());
            if (digits.empty())
                return false;

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
        , active_data_file_path_()
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , file_(nullptr)
        , index_()
        , keydir_()
        , next_timestamp_(0)
        , active_segment_id_(0)
        , active_segment_records_(0)
        , segment_record_limit_(segment_record_limit)
        , task_executor_(std::make_unique<bitcask_task_executor_t>()) {
        initialize_storage();
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

    void bitcask_index_disk_t::load_from_disk() {
        auto segments = collect_segments();
        if (segments.empty()) {
            active_segment_id_ = 1;
            next_segment_id_.store(2);
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
                    absl::crc32c_t calc_crc =
                        absl::ComputeCrc32c(absl::string_view(reinterpret_cast<const char*>(&header.kind),
                                                              sizeof(header) - sizeof(header.crc)));
                    if (!payload.empty()) {
                        calc_crc = absl::ExtendCrc32c(calc_crc, absl::string_view(payload.data(), payload.size()));
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
                    upsert_state(key, rows, keydir_entry_t{segment.id, payload_offset, header.timestamp});
                } else {
                    break;
                }

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

    void bitcask_index_disk_t::merge_immutable_segments() {
        std::unique_lock lock(mutex_);
        uint64_t last_active_segment_id = active_segment_id_;
        std::map<value_t, keydir_entry_t, std::less<>> last_keydir = keydir_;
        lock.unlock();

        auto segments = collect_segments();
        std::vector<segment_info_t> immutable_segments;
        immutable_segments.reserve(segments.size());
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

        live_entries.reserve(last_keydir.size());
        for (const auto& [key, entry] : last_keydir) {
            if (entry.segment_id >= last_active_segment_id) {
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

            if (entry.value_offset < sizeof(record_header_t)) {
                throw std::runtime_error("invalid payload offset in keydir during merge");
            }
            const uint64_t header_offset = entry.value_offset - sizeof(record_header_t);
            record_header_t source_header{};
            if (!segment_it->second->read(&source_header, sizeof(source_header), header_offset)) {
                throw std::runtime_error("failed to read immutable bitcask record header during merge");
            }

            std::pmr::string payload(resource_);
            payload.resize(static_cast<size_t>(source_header.payload_size));
            if (source_header.payload_size != 0 &&
                !segment_it->second->read(payload.data(), source_header.payload_size, entry.value_offset)) {
                throw std::runtime_error("failed to read immutable bitcask payload during merge");
            }

            const auto offset = merged_file->seek_position();
            write_record(*merged_file, static_cast<uint8_t>(record_kind_t::value), entry.timestamp, payload);

            updated_entries.emplace(
                value_t(resource_, key),
                keydir_entry_t{merged_segment_id, offset + sizeof(record_header_t), entry.timestamp});
        }

        merged_file->sync();
        merged_file.reset();

        lock.lock();
        if (!move_files(fs_, temp_path, merged_path)) {
            throw std::runtime_error("failed to publish merged bitcask data file: " + merged_path.string());
        }
        for (const auto& segment : immutable_segments) {
            remove_file(fs_, segment.path);
        }

        for (auto& [key, entry] : updated_entries) {
            auto keydir_it = keydir_.find(key);
            if (keydir_it != keydir_.end() && keydir_it->second.segment_id < last_active_segment_id) {
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

        write_record(*file_, static_cast<uint8_t>(record_kind_t::value), ++next_timestamp_, payload);

        upsert_state(key, rows, keydir_entry_t{active_segment_id_, offset + sizeof(record_header_t), next_timestamp_});
        ++active_segment_records_;
    }

    void bitcask_index_disk_t::append_tombstone(const value_t& key) {
        if (!file_) {
            return;
        }

        rotate_active_segment_if_needed();
        auto payload = serialize_payload(resource_, key, row_ids_t(resource_));

        write_record(*file_, static_cast<uint8_t>(record_kind_t::tombstone), ++next_timestamp_, payload);

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
        size_t total = entries.size();
        for (const auto& [_, rows] : index_) {
            total += rows.size();
        }
        entries.reserve(total);
        for (const auto& [key, rows] : index_) {
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
        if (task_executor_) {
            task_executor_->stop();
        }

        std::unique_lock lock(mutex_);
        if (is_dirty() && file_) {
            file_->sync();
            reset_flush_state();
        }
        file_.reset();
        index_.clear();
        keydir_.clear();
        reset_flush_state();
        next_timestamp_ = 0;
        next_segment_id_.store(1);
        active_segment_id_ = 0;
        active_segment_records_ = 0;
        active_data_file_path_.clear();
        remove_directory(fs_, path_);
    }
} // namespace services::index

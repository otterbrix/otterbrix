#include "wal.hpp"
#include <absl/crc/crc32c.h>
#include <algorithm>
#include <cstdio>
#include <unistd.h>
#include <utility>

#include "dto.hpp"
#include "manager_wal_replicate.hpp"
#include "wal_utils.hpp"

#include <components/serialization/deserializer.hpp>

namespace services::wal {

    using core::filesystem::file_flags;
    using core::filesystem::file_lock_type;

    static std::string wal_segment_name(int worker_index, int segment_idx) {
        char buf[32];
        std::snprintf(buf, sizeof(buf), ".wal_%d_%06d", worker_index, segment_idx);
        return std::string(buf);
    }

    bool file_exist_(const std::filesystem::path& path) {
        std::filesystem::file_status s = std::filesystem::file_status{};
        return std::filesystem::status_known(s) ? std::filesystem::exists(s) : std::filesystem::exists(path);
    }

    std::size_t next_index(std::size_t index, size_tt size) { return index + size + sizeof(size_tt) + sizeof(crc32_t); }

    wal_replicate_t::wal_replicate_t(std::pmr::memory_resource* resource,
                                     manager_wal_replicate_t* /*manager*/,
                                     log_t& log,
                                     configuration::config_wal config,
                                     int worker_index,
                                     int worker_count)
        : actor_zeta::basic_actor<wal_replicate_t>(resource)
        , log_(log.clone())
        , config_(std::move(config))
        , fs_(core::filesystem::local_file_system_t())
        , worker_index_(worker_index)
        , worker_count_(worker_count)
        , pending_load_(resource)
        , pending_id_(resource) {
        if (config_.sync_to_disk) {
            std::filesystem::create_directories(config_.path);

            // Discover existing segments, find highest segment index
            auto segments = discover_segments_();
            if (!segments.empty()) {
                // Parse segment index from last segment filename
                auto last_name = segments.back().filename().string();
                // Format: .wal_N_SSSSSS — extract SSSSSS
                auto last_underscore = last_name.rfind('_');
                if (last_underscore != std::string::npos) {
                    current_segment_idx_ = std::stoi(last_name.substr(last_underscore + 1));
                }
            }

            // Open current segment file
            file_ = open_file(fs_,
                              config_.path / wal_segment_name(worker_index_, current_segment_idx_),
                              file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                              file_lock_type::NO_LOCK);
            file_->seek(file_->file_size());
            init_id();
        }
    }

    void wal_replicate_t::poll_pending() {
        for (auto it = pending_load_.begin(); it != pending_load_.end();) {
            if (it->available()) {
                it = pending_load_.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = pending_id_.begin(); it != pending_id_.end();) {
            if (it->available()) {
                it = pending_id_.erase(it);
            } else {
                ++it;
            }
        }
    }

    actor_zeta::behavior_t wal_replicate_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::load>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::commit_txn>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::commit_txn, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::truncate_before>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::truncate_before, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::write_physical_insert>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::write_physical_insert, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::write_physical_delete>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::write_physical_delete, msg);
                break;
            }
            case actor_zeta::msg_id<wal_replicate_t, &wal_replicate_t::write_physical_update>: {
                co_await actor_zeta::dispatch(this, &wal_replicate_t::write_physical_update, msg);
                break;
            }
            default:
                break;
        }
    }

    auto wal_replicate_t::make_type() const noexcept -> const char* { return "wal"; }

    void wal_replicate_t::write_buffer(buffer_t& buffer) {
        if (file_->file_size() + buffer.size() > config_.max_segment_size) {
            rotate_segment_();
        }
        file_->write(buffer.data(), buffer.size());
    }

    void wal_replicate_t::read_buffer(buffer_t& buffer, size_t start_index, size_t size) const {
        buffer.resize(size);
        file_->read(buffer.data(), size, uint64_t(start_index));
    }

    wal_replicate_t::~wal_replicate_t() { trace(log_, "delete wal_replicate_t"); }

    size_tt wal_replicate_t::read_size(size_t start_index) const {
        auto size_read = sizeof(size_tt);
        buffer_t buffer;
        read_buffer(buffer, start_index, size_read);
        return read_size_raw(buffer.data(), 0);
    }

    buffer_t wal_replicate_t::read(size_t start_index, size_t finish_index) const {
        auto size_read = finish_index - start_index;
        buffer_t buffer;
        read_buffer(buffer, start_index, size_read);
        return buffer;
    }

    wal_replicate_t::unique_future<std::vector<record_t>> wal_replicate_t::load(session_id_t session,
                                                                                services::wal::id_t wal_id) {
        trace(log_, "wal_replicate_t::load, session: {}, id: {}", session.data(), wal_id);
        std::size_t start_index = 0;
        next_id(wal_id, 1);
        std::vector<record_t> records;
        if (find_start_record(wal_id, start_index)) {
            std::size_t size = 0;
            do {
                records.emplace_back(read_record(start_index));
                start_index = next_index(start_index, records[size].size);
            } while (records[size++].is_valid());
            records.erase(records.end() - 1);
        }
        co_return records;
    }

    wal_replicate_t::unique_future<services::wal::id_t> wal_replicate_t::commit_txn(session_id_t session,
                                                                                    uint64_t transaction_id) {
        trace(log_, "wal_replicate_t::commit_txn txn_id={}, session: {}", transaction_id, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_commit_marker(buffer, last_crc32_, id_, transaction_id);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
    }

    void wal_replicate_t::init_id() {
        // Scan all segment files to find the highest WAL ID
        auto segments = discover_segments_();
        for (const auto& seg_path : segments) {
            auto seg_file = open_file(fs_, seg_path, file_flags::READ, file_lock_type::NO_LOCK);
            std::size_t start_index = 0;
            while (true) {
                auto size_read = sizeof(size_tt);
                buffer_t size_buf;
                size_buf.resize(size_read);
                if (!seg_file->read(size_buf.data(), size_read, uint64_t(start_index))) {
                    break;
                }
                auto size = read_size_raw(size_buf.data(), 0);
                if (size == 0)
                    break;

                auto start = start_index + sizeof(size_tt);
                auto finish = start + size;
                buffer_t data_buf;
                data_buf.resize(finish - start);
                seg_file->read(data_buf.data(), data_buf.size(), uint64_t(start));
                auto id = unpack_wal_id(data_buf);
                if (id > 0) {
                    id_ = id;
                }
                start_index = next_index(start_index, size);
            }
        }
        // Align id_ so that next next_id() call produces the correct worker partition
        if (static_cast<services::wal::id_t>(id_) == 0) {
            id_ = static_cast<services::wal::id_t>(worker_index_);
        }
    }

    bool wal_replicate_t::find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const {
        if (wal_id == 0) {
            return false;
        }
        start_index = 0;
        auto id = read_id(start_index);
        while (id > 0 && id < wal_id) {
            auto size = read_size(start_index);
            if (size > 0) {
                start_index = next_index(start_index, size);
                id = read_id(start_index);
            } else {
                return false;
            }
        }
        return id > 0 && id >= wal_id;
    }

    services::wal::id_t wal_replicate_t::read_id(std::size_t start_index) const {
        auto size = read_size(start_index);
        if (size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + size;
            auto output = read(start, finish);
            return unpack_wal_id(output);
        }
        return 0;
    }

    record_t wal_replicate_t::read_record(std::size_t start_index) const {
        record_t record;
        record.size = read_size(start_index);
        if (record.size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + record.size + sizeof(crc32_t);
            auto output = read(start, finish);
            record.crc32 = read_crc32_raw(output, record.size);
            if (record.crc32 == static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), record.size}))) {
                components::serializer::msgpack_deserializer_t deserializer(output);
                auto arr_size = deserializer.root_array_size();
                record.last_crc32 = static_cast<uint32_t>(deserializer.deserialize_uint64(0));
                record.id = deserializer.deserialize_uint64(1);

                if (arr_size == 3) {
                    // COMMIT marker: array(3) = [last_crc32, wal_id, txn_id]
                    record.transaction_id = deserializer.deserialize_uint64(2);
                    record.record_type = wal_record_type::COMMIT;
                } else if (arr_size >= 8) {
                    // Check if element[3] is a physical record type
                    auto type_val = deserializer.deserialize_uint64(3);
                    auto phys_type = static_cast<wal_record_type>(type_val);
                    if (phys_type == wal_record_type::PHYSICAL_INSERT ||
                        phys_type == wal_record_type::PHYSICAL_DELETE ||
                        phys_type == wal_record_type::PHYSICAL_UPDATE) {
                        record.transaction_id = deserializer.deserialize_uint64(2);
                        record.record_type = phys_type;
                        record.collection_name = collection_full_name_t(deserializer.deserialize_string(4),
                                                                        deserializer.deserialize_string(5));

                        if (phys_type == wal_record_type::PHYSICAL_INSERT) {
                            // array(9): [..., data_chunk, row_start, row_count]
                            deserializer.advance_array(6);
                            auto chunk = components::vector::data_chunk_t::deserialize(&deserializer);
                            record.physical_data = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
                            deserializer.pop_array();
                            record.physical_row_start = deserializer.deserialize_uint64(7);
                            record.physical_row_count = deserializer.deserialize_uint64(8);
                        } else if (phys_type == wal_record_type::PHYSICAL_DELETE) {
                            // array(8): [..., row_ids_array, count]
                            deserializer.advance_array(6);
                            auto ids_count = deserializer.current_array_size();
                            record.physical_row_ids.reserve(ids_count);
                            for (std::size_t ri = 0; ri < ids_count; ++ri) {
                                record.physical_row_ids.push_back(
                                    static_cast<int64_t>(deserializer.deserialize_int64(ri)));
                            }
                            deserializer.pop_array();
                            record.physical_row_count = deserializer.deserialize_uint64(7);
                        } else {
                            // PHYSICAL_UPDATE: array(9): [..., row_ids_array, data_chunk, count]
                            deserializer.advance_array(6);
                            auto ids_count = deserializer.current_array_size();
                            record.physical_row_ids.reserve(ids_count);
                            for (std::size_t ri = 0; ri < ids_count; ++ri) {
                                record.physical_row_ids.push_back(
                                    static_cast<int64_t>(deserializer.deserialize_int64(ri)));
                            }
                            deserializer.pop_array();
                            deserializer.advance_array(7);
                            auto chunk = components::vector::data_chunk_t::deserialize(&deserializer);
                            record.physical_data = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
                            deserializer.pop_array();
                            record.physical_row_count = deserializer.deserialize_uint64(8);
                        }
                    } else {
                        error(log_, "wal: unknown record type {} at offset {}", type_val, start_index);
                        record.is_corrupt = true;
                        record.size = 0;
                    }
                } else {
                    error(log_, "wal: unexpected array size {} at offset {}", arr_size, start_index);
                    record.is_corrupt = true;
                    record.size = 0;
                }
            } else {
                auto computed_crc = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), record.size}));
                error(log_,
                      "wal: CRC32 mismatch at offset {}, expected={:#x}, computed={:#x}",
                      start_index,
                      record.crc32,
                      computed_crc);
                record.is_corrupt = true;
                record.size = 0;
            }
        }
        return record;
    }

    wal_replicate_t::unique_future<void> wal_replicate_t::truncate_before(session_id_t session,
                                                                          services::wal::id_t checkpoint_wal_id) {
        trace(log_, "wal_replicate_t::truncate_before session: {}, wal_id: {}", session.data(), checkpoint_wal_id);
        if (!file_ || checkpoint_wal_id == 0) {
            co_return;
        }
        // Delete old segment files whose last record ID <= checkpoint_wal_id
        auto segments = discover_segments_();
        for (auto& seg_path : segments) {
            // Never delete the segment we're currently writing to
            if (seg_path == config_.path / wal_segment_name_(current_segment_idx_)) {
                continue;
            }
            auto last_id = last_id_in_file_(seg_path);
            if (last_id > 0 && last_id <= checkpoint_wal_id) {
                trace(log_, "wal_replicate_t::truncate_before deleting segment: {}", seg_path.string());
                std::filesystem::remove(seg_path);
            }
        }
        trace(log_, "wal_replicate_t::truncate_before WAL trimmed up to id {}", checkpoint_wal_id);
        co_return;
    }

    wal_replicate_t::unique_future<services::wal::id_t>
    wal_replicate_t::write_physical_insert(session_id_t session,
                                           std::string database,
                                           std::string collection,
                                           std::unique_ptr<components::vector::data_chunk_t> data_chunk,
                                           uint64_t row_start,
                                           uint64_t row_count,
                                           uint64_t txn_id) {
        trace(log_, "wal_replicate_t::write_physical_insert {}::{}, session: {}", database, collection, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_physical_insert(buffer,
                                           resource(),
                                           last_crc32_,
                                           id_,
                                           txn_id,
                                           database,
                                           collection,
                                           *data_chunk,
                                           row_start,
                                           row_count);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t>
    wal_replicate_t::write_physical_delete(session_id_t session,
                                           std::string database,
                                           std::string collection,
                                           std::pmr::vector<int64_t> row_ids,
                                           uint64_t count,
                                           uint64_t txn_id) {
        trace(log_, "wal_replicate_t::write_physical_delete {}::{}, session: {}", database, collection, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_physical_delete(buffer, last_crc32_, id_, txn_id, database, collection, row_ids, count);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
    }

    wal_replicate_t::unique_future<services::wal::id_t>
    wal_replicate_t::write_physical_update(session_id_t session,
                                           std::string database,
                                           std::string collection,
                                           std::pmr::vector<int64_t> row_ids,
                                           std::unique_ptr<components::vector::data_chunk_t> new_data,
                                           uint64_t count,
                                           uint64_t txn_id) {
        trace(log_, "wal_replicate_t::write_physical_update {}::{}, session: {}", database, collection, session.data());
        next_id(id_, static_cast<services::wal::id_t>(worker_count_));
        buffer_t buffer;
        last_crc32_ = pack_physical_update(buffer,
                                           resource(),
                                           last_crc32_,
                                           id_,
                                           txn_id,
                                           database,
                                           collection,
                                           row_ids,
                                           *new_data,
                                           count);
        write_buffer(buffer);
        co_return services::wal::id_t(id_);
    }

    std::string wal_replicate_t::wal_segment_name_(int segment_idx) const {
        return wal_segment_name(worker_index_, segment_idx);
    }

    void wal_replicate_t::rotate_segment_() {
        file_.reset();
        ++current_segment_idx_;
        trace(log_, "wal: rotating to segment {}", wal_segment_name_(current_segment_idx_));
        file_ = open_file(fs_,
                          config_.path / wal_segment_name_(current_segment_idx_),
                          file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK);
        // last_crc32_ carries over (chain is not reset)
    }

    std::vector<std::filesystem::path> wal_replicate_t::discover_segments_() const {
        std::vector<std::filesystem::path> result;
        if (!std::filesystem::exists(config_.path)) {
            return result;
        }
        std::string prefix = ".wal_" + std::to_string(worker_index_) + "_";
        for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
            if (!entry.is_regular_file())
                continue;
            auto name = entry.path().filename().string();
            if (name.size() > prefix.size() && name.substr(0, prefix.size()) == prefix) {
                result.push_back(entry.path());
            }
        }
        std::sort(result.begin(), result.end());
        return result;
    }

    services::wal::id_t wal_replicate_t::last_id_in_file_(const std::filesystem::path& path) {
        services::wal::id_t last_id = 0;
        auto seg_file = open_file(fs_, path, file_flags::READ, file_lock_type::NO_LOCK);
        std::size_t start_index = 0;
        while (true) {
            buffer_t size_buf;
            size_buf.resize(sizeof(size_tt));
            if (!seg_file->read(size_buf.data(), sizeof(size_tt), uint64_t(start_index))) {
                break;
            }
            auto size = read_size_raw(size_buf.data(), 0);
            if (size == 0)
                break;

            auto start = start_index + sizeof(size_tt);
            auto finish = start + size;
            buffer_t data_buf;
            data_buf.resize(finish - start);
            seg_file->read(data_buf.data(), data_buf.size(), uint64_t(start));
            auto id = unpack_wal_id(data_buf);
            if (id > 0) {
                last_id = id;
            }
            start_index = next_index(start_index, size);
        }
        return last_id;
    }

#ifdef DEV_MODE
    bool wal_replicate_t::test_find_start_record(services::wal::id_t wal_id, std::size_t& start_index) const {
        return find_start_record(wal_id, start_index);
    }

    services::wal::id_t wal_replicate_t::test_read_id(std::size_t start_index) const { return read_id(start_index); }

    std::size_t wal_replicate_t::test_next_record(std::size_t start_index) const {
        return next_index(start_index, read_size(start_index));
    }

    record_t wal_replicate_t::test_read_record(std::size_t start_index) const { return read_record(start_index); }

    size_tt wal_replicate_t::test_read_size(size_t start_index) const { return read_size(start_index); }

    buffer_t wal_replicate_t::test_read(size_t start_index, size_t finish_index) const {
        return read(start_index, finish_index);
    }
#endif

    wal_replicate_without_disk_t::wal_replicate_without_disk_t(std::pmr::memory_resource* resource,
                                                               manager_wal_replicate_t* manager,
                                                               log_t& log,
                                                               configuration::config_wal config,
                                                               int worker_index,
                                                               int worker_count)
        : wal_replicate_t(resource, manager, log, std::move(config), worker_index, worker_count) {}

    wal_replicate_t::unique_future<std::vector<record_t>> wal_replicate_without_disk_t::load(session_id_t /*session*/,
                                                                                             services::wal::id_t) {
        co_return std::vector<record_t>{};
    }

    void wal_replicate_without_disk_t::write_buffer(buffer_t&) {}

    void wal_replicate_without_disk_t::read_buffer(buffer_t& buffer, size_t, size_t size) const {
        buffer.resize(size);
        std::fill(buffer.begin(), buffer.end(), '\0');
    }

} //namespace services::wal

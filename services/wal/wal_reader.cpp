#include "wal_reader.hpp"

#include <algorithm>
#include <unordered_set>
#include <absl/crc/crc32c.h>
#include <components/serialization/deserializer.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/wal/dto.hpp>

namespace services::wal {

    using namespace core::filesystem;

    wal_reader_t::wal_reader_t(const configuration::config_wal& config,
                               std::pmr::memory_resource* resource,
                               log_t& log)
        : resource_(resource)
        , log_(log.clone())
        , fs_(local_file_system_t()) {
        if (config.path.empty()) {
            return;
        }
        for (int i = 0; i < config.agent; ++i) {
            auto wal_file_path = config.path / (".wal_" + std::to_string(i));
            if (std::filesystem::exists(wal_file_path)) {
                trace(log_, "wal_reader_t: opening WAL at {}", wal_file_path.string());
                wal_files_.push_back(open_file(fs_,
                                               wal_file_path,
                                               file_flags::READ,
                                               file_lock_type::NO_LOCK));
            }
        }
        // Legacy single .wal file for backward compatibility
        auto legacy_wal_path = config.path / ".wal";
        if (wal_files_.empty() && std::filesystem::exists(legacy_wal_path)) {
            trace(log_, "wal_reader_t: opening legacy WAL at {}", legacy_wal_path.string());
            wal_files_.push_back(open_file(fs_,
                                           legacy_wal_path,
                                           file_flags::READ,
                                           file_lock_type::NO_LOCK));
        }
    }

    std::vector<record_t> wal_reader_t::read_committed_records(id_t after_id) {
        if (wal_files_.empty()) {
            return {};
        }

        // Pass 1: Read all records, collect committed txn_ids
        std::vector<record_t> all_records;
        std::unordered_set<uint64_t> committed_txn_ids;

        for (auto& wal_file : wal_files_) {
            std::size_t start_index = 0;
            while (true) {
                auto record = read_wal_record(wal_file.get(), start_index);
                if (!record.is_valid()) {
                    break;
                }
                if (record.is_commit_marker()) {
                    if (record.transaction_id != 0) {
                        committed_txn_ids.insert(record.transaction_id);
                    }
                    start_index = next_wal_index(start_index, record.size);
                    continue;
                }
                // Skip non-physical DATA records entirely
                if (!record.is_physical()) {
                    start_index = next_wal_index(start_index, record.size);
                    continue;
                }
                if (record.id > after_id) {
                    all_records.push_back(std::move(record));
                }
                start_index = next_wal_index(start_index, record.size);
            }
        }

        // Pass 2: Filter by committed transactions
        std::vector<record_t> committed;
        for (auto& record : all_records) {
            if (record.transaction_id == 0 || committed_txn_ids.count(record.transaction_id) > 0) {
                committed.push_back(std::move(record));
            }
        }

        // Sort by WAL ID for correct replay order
        std::sort(committed.begin(), committed.end(),
            [](const record_t& a, const record_t& b) { return a.id < b.id; });

        debug(log_, "wal_reader_t: read {} committed physical WAL records (after id {})",
              committed.size(), after_id);
        return committed;
    }

    size_tt wal_reader_t::read_wal_size(core::filesystem::file_handle_t* file, std::size_t start_index) const {
        char buf[4];
        if (!file->read(buf, sizeof(size_tt), start_index)) {
            return 0;
        }
        size_tt size = 0;
        size = (size_tt(uint8_t(buf[0])) << 24) |
               (size_tt(uint8_t(buf[1])) << 16) |
               (size_tt(uint8_t(buf[2])) << 8) |
               (size_tt(uint8_t(buf[3])));
        return size;
    }

    std::pmr::string wal_reader_t::read_wal_data(core::filesystem::file_handle_t* file, std::size_t start, std::size_t finish) const {
        auto size = finish - start;
        std::pmr::string output(resource_);
        output.resize(size);
        file->read(output.data(), size, start);
        return output;
    }

    record_t wal_reader_t::read_wal_record(core::filesystem::file_handle_t* file, std::size_t start_index) const {
        record_t record;
        record.size = read_wal_size(file, start_index);
        if (record.size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + record.size + sizeof(crc32_t);
            auto output = read_wal_data(file, start, finish);

            const char* crc_ptr = output.data() + record.size;
            record.crc32 = (crc32_t(uint8_t(crc_ptr[0])) << 24) |
                           (crc32_t(uint8_t(crc_ptr[1])) << 16) |
                           (crc32_t(uint8_t(crc_ptr[2])) << 8) |
                           (crc32_t(uint8_t(crc_ptr[3])));

            auto computed_crc = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), record.size}));
            if (record.crc32 == computed_crc) {
                components::serializer::msgpack_deserializer_t deserializer(output);
                auto arr_size = deserializer.root_array_size();
                record.last_crc32 = static_cast<uint32_t>(deserializer.deserialize_uint64(0));
                record.id = deserializer.deserialize_uint64(1);

                if (arr_size == 3) {
                    // COMMIT marker
                    record.transaction_id = deserializer.deserialize_uint64(2);
                    record.record_type = wal_record_type::COMMIT;
                    record.data = nullptr;
                } else if (arr_size >= 8) {
                    auto type_val = deserializer.deserialize_uint64(3);
                    auto phys_type = static_cast<wal_record_type>(type_val);
                    if (phys_type == wal_record_type::PHYSICAL_INSERT
                        || phys_type == wal_record_type::PHYSICAL_DELETE
                        || phys_type == wal_record_type::PHYSICAL_UPDATE) {
                        record.transaction_id = deserializer.deserialize_uint64(2);
                        record.record_type = phys_type;
                        record.data = nullptr;
                        record.collection_name = collection_full_name_t(
                            deserializer.deserialize_string(4),
                            deserializer.deserialize_string(5));

                        if (phys_type == wal_record_type::PHYSICAL_INSERT) {
                            deserializer.advance_array(6);
                            auto chunk = components::vector::data_chunk_t::deserialize(&deserializer);
                            record.physical_data = std::make_unique<components::vector::data_chunk_t>(std::move(chunk));
                            deserializer.pop_array();
                            record.physical_row_start = deserializer.deserialize_uint64(7);
                            record.physical_row_count = deserializer.deserialize_uint64(8);
                        } else if (phys_type == wal_record_type::PHYSICAL_DELETE) {
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
                            // PHYSICAL_UPDATE
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
                        // Legacy logical DATA — skip (mark as DATA so caller ignores)
                        record.transaction_id = 0;
                        record.record_type = wal_record_type::DATA;
                        record.data = nullptr;
                    }
                } else {
                    // Legacy logical DATA — skip
                    record.transaction_id = 0;
                    record.record_type = wal_record_type::DATA;
                    record.data = nullptr;
                }
            } else {
                record.data = nullptr;
            }
        } else {
            record.data = nullptr;
        }
        return record;
    }

    std::size_t wal_reader_t::next_wal_index(std::size_t start_index, size_tt size) const {
        return start_index + sizeof(size_tt) + size + sizeof(crc32_t);
    }

} // namespace services::wal

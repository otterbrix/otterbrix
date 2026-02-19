#include "loader.hpp"

#include <algorithm>
#include <unordered_set>
#include <absl/crc/crc32c.h>
#include <boost/polymorphic_pointer_cast.hpp>
#include <components/serialization/deserializer.hpp>
#include <services/wal/dto.hpp>

namespace services::loader {

    using namespace core::filesystem;

    loader_t::loader_t(const configuration::config_disk& config,
                       const configuration::config_wal& wal_config,
                       std::pmr::memory_resource* resource,
                       log_t& log)
        : resource_(resource)
        , log_(log.clone())
        , config_(config)
        , wal_config_(wal_config)
        , disk_(nullptr)
        , fs_(local_file_system_t())
        , metafile_indexes_(nullptr) {
        trace(log_, "loader_t: initializing");

        if (!config_.path.empty() && std::filesystem::exists(config_.path)) {
            trace(log_, "loader_t: opening disk at {}", config_.path.string());
            disk_ = std::make_unique<disk::disk_t>(config_.path, resource_);

            auto indexes_path = config_.path / "indexes_METADATA";
            if (std::filesystem::exists(indexes_path)) {
                trace(log_, "loader_t: opening indexes metafile at {}", indexes_path.string());
                metafile_indexes_ = open_file(fs_,
                                              indexes_path,
                                              file_flags::READ,
                                              file_lock_type::NO_LOCK);
            }
        }

        // Open per-worker WAL files (.wal_0, .wal_1, ...)
        if (!wal_config_.path.empty()) {
            for (int i = 0; i < wal_config_.agent; ++i) {
                auto wal_file_path = wal_config_.path / (".wal_" + std::to_string(i));
                debug(log_, "loader_t: WAL file path: {}, exists: {}",
                      wal_file_path.string(),
                      std::filesystem::exists(wal_file_path));
                if (std::filesystem::exists(wal_file_path)) {
                    trace(log_, "loader_t: opening WAL at {}", wal_file_path.string());
                    wal_files_.push_back(open_file(fs_,
                                                   wal_file_path,
                                                   file_flags::READ,
                                                   file_lock_type::NO_LOCK));
                }
            }
            // Also check legacy single .wal file for backward compatibility
            auto legacy_wal_path = wal_config_.path / ".wal";
            if (wal_files_.empty() && std::filesystem::exists(legacy_wal_path)) {
                trace(log_, "loader_t: opening legacy WAL at {}", legacy_wal_path.string());
                wal_files_.push_back(open_file(fs_,
                                               legacy_wal_path,
                                               file_flags::READ,
                                               file_lock_type::NO_LOCK));
            }
        }

        trace(log_, "loader_t: initialization complete");
    }

    loader_t::~loader_t() {
        trace(log_, "loader_t: destructor");
    }

    bool loader_t::has_data() const {
        if (!disk_) {
            return false;
        }
        auto dbs = disk_->databases();
        return !dbs.empty();
    }

    loaded_state_t loader_t::load() {
        trace(log_, "loader_t::load: PHASE 1 - Loading data from disk WITHOUT actors");

        loaded_state_t state(resource_);

        if (!disk_) {
            trace(log_, "loader_t::load: no disk configured, returning empty state");
            return state;
        }

        read_databases_and_collections(state);

        read_documents(state);

        read_index_definitions(state);

        read_wal_checkpoint(state);

        read_wal_records(state);

        trace(log_, "loader_t::load: PHASE 1 complete - loaded {} databases, {} collections, {} index definitions, {} WAL records, {} sequences, {} views, {} macros",
              state.databases.size(),
              state.collections.size(),
              state.index_definitions.size(),
              state.wal_records.size(),
              state.sequences.size(),
              state.views.size(),
              state.macros.size());

        return state;
    }

    void loader_t::read_databases_and_collections(loaded_state_t& state) {
        trace(log_, "loader_t: reading databases and collections");

        auto databases = disk_->databases();
        for (const auto& db_name : databases) {
            debug(log_, "loader_t: found database: {}", db_name);
            state.databases.insert(db_name);

            auto table_entries = disk_->table_entries(db_name);
            for (const auto& entry : table_entries) {
                debug(log_, "loader_t: found collection: {}.{} (mode={})", db_name, entry.name,
                      static_cast<int>(entry.storage_mode));
                collection_full_name_t full_name(db_name, entry.name);
                state.collections.insert(full_name);

                collection_load_info_t info;
                info.name = full_name;
                info.storage_mode = entry.storage_mode;
                info.columns = entry.columns;
                state.collection_infos.push_back(std::move(info));
            }

            // Load catalog DDL objects
            for (auto& seq : disk_->catalog().sequences(db_name)) {
                debug(log_, "loader_t: found sequence: {}.{}", db_name, seq.name);
                state.sequences.emplace_back(db_name, std::move(seq));
            }
            for (auto& view : disk_->catalog().views(db_name)) {
                debug(log_, "loader_t: found view: {}.{}", db_name, view.name);
                state.views.emplace_back(db_name, std::move(view));
            }
            for (auto& macro : disk_->catalog().macros(db_name)) {
                debug(log_, "loader_t: found macro: {}.{}", db_name, macro.name);
                state.macros.emplace_back(db_name, std::move(macro));
            }
        }
    }

    void loader_t::read_documents(loaded_state_t& /*state*/) {
        trace(log_, "loader_t: reading documents - placeholder (columnar persistence TBD)");
        // TODO: Implement columnar data persistence
    }

    void loader_t::read_index_definitions(loaded_state_t& state) {
        trace(log_, "loader_t: reading index definitions");

        if (!metafile_indexes_) {
            trace(log_, "loader_t: no indexes metafile, skipping");
            return;
        }

        constexpr auto count_byte_by_size = sizeof(size_t);
        size_t size;
        size_t offset = 0;
        std::unique_ptr<char[]> size_str(new char[count_byte_by_size]);

        while (true) {
            metafile_indexes_->seek(offset);
            auto bytes_read = metafile_indexes_->read(size_str.get(), count_byte_by_size);
            if (bytes_read == count_byte_by_size) {
                offset += count_byte_by_size;
                std::memcpy(&size, size_str.get(), count_byte_by_size);

                std::pmr::string buf(resource_);
                buf.resize(size);
                metafile_indexes_->read(buf.data(), size, offset);
                offset += size;

                components::serializer::msgpack_deserializer_t deserializer(buf);
                deserializer.advance_array(0);
                auto index = components::logical_plan::node_t::deserialize(&deserializer);
                deserializer.pop_array();

                auto index_ptr = boost::polymorphic_pointer_downcast<
                    components::logical_plan::node_create_index_t>(index);

                auto index_path = config_.path / index_ptr->collection_full_name().database /
                                  index_ptr->collection_full_name().collection / index_ptr->name();
                if (is_index_valid(index_path)) {
                    debug(log_, "loader_t: found valid index definition: {} on {}",
                          index_ptr->name(), index_ptr->collection_full_name().to_string());
                    state.index_definitions.push_back(std::move(index_ptr));
                } else {
                    warn(log_, "loader_t: skipping corrupted index: {} on {}",
                         index_ptr->name(), index_ptr->collection_full_name().to_string());
                }
            } else {
                break;
            }
        }

        trace(log_, "loader_t: read {} index definitions", state.index_definitions.size());
    }

    void loader_t::read_wal_checkpoint(loaded_state_t& state) {
        trace(log_, "loader_t: reading WAL checkpoint");

        if (disk_) {
            state.last_wal_id = disk_->wal_id();
            debug(log_, "loader_t: last WAL id: {}", state.last_wal_id);
        }
    }

    void loader_t::read_wal_records(loaded_state_t& state) {
        trace(log_, "loader_t: reading WAL records for replay from {} WAL files", wal_files_.size());

        if (wal_files_.empty()) {
            trace(log_, "loader_t: no WAL files, skipping");
            return;
        }

        debug(log_, "loader_t: last_wal_id from disk checkpoint: {}", state.last_wal_id);

        // Pass 1: Read all records, collect committed txn_ids
        std::vector<wal::record_t> all_records;
        std::unordered_set<uint64_t> committed_txn_ids;
        std::size_t total_records = 0;
        std::size_t skipped_records = 0;

        for (auto& wal_file : wal_files_) {
            std::size_t start_index = 0;

            while (true) {
                auto record = read_wal_record(wal_file.get(), start_index);
                if (!record.is_valid()) {
                    break;
                }

                if (record.is_commit_marker()) {
                    // Collect committed txn_id
                    if (record.transaction_id != 0) {
                        committed_txn_ids.insert(record.transaction_id);
                    }
                    start_index = next_wal_index(start_index, record.size);
                    continue;
                }

                if (!record.data) {
                    debug(log_, "loader_t: skipping WAL record at index {} - CRC mismatch (stored={:#x}, computed={:#x})",
                          start_index, record.crc32, record.last_crc32);
                    start_index = next_wal_index(start_index, record.size);
                    continue;
                }

                total_records++;

                if (record.id > state.last_wal_id) {
                    all_records.push_back(std::move(record));
                } else {
                    skipped_records++;
                }
                start_index = next_wal_index(start_index, record.size);
            }
        }

        // Pass 2: Filter by committed transactions
        for (auto& record : all_records) {
            // Legacy records (txn_id=0) are always replayed
            // Records with txn_id != 0 are only replayed if committed
            if (record.transaction_id == 0 || committed_txn_ids.count(record.transaction_id) > 0) {
                debug(log_, "loader_t: read WAL record id {} type {} (will replay)",
                      record.id, record.data->to_string());
                state.wal_records.push_back(std::move(record));
            } else {
                debug(log_, "loader_t: skipping uncommitted WAL record id {} txn_id={}",
                      record.id, record.transaction_id);
            }
        }

        // Sort all records from all WAL files by ID for correct replay order
        std::sort(state.wal_records.begin(), state.wal_records.end(),
            [](const wal::record_t& a, const wal::record_t& b) { return a.id < b.id; });

        debug(log_, "loader_t: scanned {} WAL records, skipped {} (already on disk), {} committed txns, {} to replay",
              total_records, skipped_records, committed_txn_ids.size(), state.wal_records.size());
        trace(log_, "loader_t: read {} WAL records for replay", state.wal_records.size());
    }

    loader_t::size_tt loader_t::read_wal_size(core::filesystem::file_handle_t* file, std::size_t start_index) const {
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

    std::pmr::string loader_t::read_wal_data(core::filesystem::file_handle_t* file, std::size_t start, std::size_t finish) const {
        auto size = finish - start;
        std::pmr::string output(resource_);
        output.resize(size);
        file->read(output.data(), size, start);
        return output;
    }

    wal::id_t loader_t::read_wal_id(core::filesystem::file_handle_t* file, std::size_t start_index) const {
        auto size = read_wal_size(file, start_index);
        if (size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + size;
            auto output = read_wal_data(file, start, finish);
            return wal::unpack_wal_id(output);
        }
        return 0;
    }

    wal::record_t loader_t::read_wal_record(core::filesystem::file_handle_t* file, std::size_t start_index) const {
        wal::record_t record;
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
                    record.record_type = wal::wal_record_type::COMMIT;
                    record.data = nullptr;
                } else if (arr_size >= 5) {
                    record.transaction_id = deserializer.deserialize_uint64(2);
                    record.record_type = wal::wal_record_type::DATA;
                    deserializer.advance_array(3);
                    record.data = components::logical_plan::node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                    deserializer.advance_array(4);
                    record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                } else {
                    record.transaction_id = 0;
                    record.record_type = wal::wal_record_type::DATA;
                    deserializer.advance_array(2);
                    record.data = components::logical_plan::node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                    deserializer.advance_array(3);
                    record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                    deserializer.pop_array();
                }
            } else {
                record.data = nullptr;
                record.last_crc32 = computed_crc;
            }
        } else {
            record.data = nullptr;
        }
        return record;
    }

    std::size_t loader_t::next_wal_index(std::size_t start_index, size_tt size) const {
        return start_index + sizeof(size_tt) + size + sizeof(crc32_t);
    }

    bool loader_t::is_index_valid(const std::filesystem::path& index_path) const {
        if (!std::filesystem::exists(index_path) || !std::filesystem::is_directory(index_path)) {
            return false;
        }

        auto metadata_path = index_path / "metadata";
        if (!std::filesystem::exists(metadata_path)) {
            return false;
        }
        if (std::filesystem::file_size(metadata_path) == 0) {
            return false;
        }

        for (const auto& entry : std::filesystem::directory_iterator(index_path)) {
            if (entry.is_regular_file() && entry.path().filename() != "metadata") {
                if (std::filesystem::file_size(entry.path()) == 0) {
                    return false;  // Corrupted segment file
                }
            }
        }

        return true;
    }

} // namespace services::loader

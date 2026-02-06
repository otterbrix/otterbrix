#include "loader.hpp"

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
        , metafile_indexes_(nullptr)
        , wal_file_(nullptr) {
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

        auto wal_file_path = wal_config_.path / ".wal";
        debug(log_, "loader_t: WAL file path: {}, exists: {}",
              wal_file_path.string(),
              std::filesystem::exists(wal_file_path));
        if (!wal_config_.path.empty() && std::filesystem::exists(wal_file_path)) {
            trace(log_, "loader_t: opening WAL at {}", wal_file_path.string());
            wal_file_ = open_file(fs_,
                                  wal_file_path,
                                  file_flags::READ,
                                  file_lock_type::NO_LOCK);
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

        trace(log_, "loader_t::load: PHASE 1 complete - loaded {} databases, {} collections, {} index definitions, {} WAL records",
              state.databases.size(),
              state.documents.size(),
              state.index_definitions.size(),
              state.wal_records.size());

        return state;
    }

    void loader_t::read_databases_and_collections(loaded_state_t& state) {
        trace(log_, "loader_t: reading databases and collections");

        auto databases = disk_->databases();
        for (const auto& db_name : databases) {
            debug(log_, "loader_t: found database: {}", db_name);
            state.databases.insert(db_name);

            auto collections = disk_->collections(db_name);
            for (const auto& coll_name : collections) {
                debug(log_, "loader_t: found collection: {}.{}", db_name, coll_name);
                collection_full_name_t full_name(db_name, coll_name);
                state.documents.emplace(full_name, std::pmr::vector<document_ptr>(resource_));
            }
        }
    }

    void loader_t::read_documents(loaded_state_t& state) {
        trace(log_, "loader_t: reading documents");

        for (auto& [full_name, docs] : state.documents) {
            debug(log_, "loader_t: loading documents for {}.{}", full_name.database, full_name.collection);
            disk_->load_documents(full_name.database, full_name.collection, docs);
            debug(log_, "loader_t: loaded {} documents for {}.{}", docs.size(), full_name.database, full_name.collection);
        }
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
        trace(log_, "loader_t: reading WAL records for replay");

        if (!wal_file_) {
            trace(log_, "loader_t: no WAL file, skipping");
            return;
        }

        debug(log_, "loader_t: WAL file exists, last_wal_id from disk checkpoint: {}", state.last_wal_id);

        auto first_size = read_wal_size(0);
        debug(log_, "loader_t: first WAL record size at index 0 = {}", first_size);

        std::size_t start_index = 0;
        std::size_t total_records = 0;
        std::size_t skipped_records = 0;

        while (true) {
            auto record = read_wal_record(start_index);
            if (!record.is_valid()) {
                break;
            }

            if (!record.data) {
                debug(log_, "loader_t: skipping WAL record at index {} - CRC mismatch (stored={:#x}, computed={:#x})",
                      start_index, record.crc32, record.last_crc32);
                start_index = next_wal_index(start_index, record.size);
                continue;
            }

            total_records++;

            if (record.id > state.last_wal_id) {
                debug(log_, "loader_t: read WAL record id {} type {} (will replay)",
                      record.id, record.data->to_string());
                state.wal_records.push_back(std::move(record));
            } else {
                skipped_records++;
            }
            start_index = next_wal_index(start_index, record.size);
        }

        debug(log_, "loader_t: scanned {} WAL records, skipped {} (already on disk), {} to replay",
              total_records, skipped_records, state.wal_records.size());
        trace(log_, "loader_t: read {} WAL records for replay", state.wal_records.size());
    }

    loader_t::size_tt loader_t::read_wal_size(std::size_t start_index) const {
        char buf[4];
        if (!wal_file_->read(buf, sizeof(size_tt), start_index)) {
            return 0;
        }
        size_tt size = 0;
        size = (size_tt(uint8_t(buf[0])) << 24) |
               (size_tt(uint8_t(buf[1])) << 16) |
               (size_tt(uint8_t(buf[2])) << 8) |
               (size_tt(uint8_t(buf[3])));
        return size;
    }

    std::pmr::string loader_t::read_wal_data(std::size_t start, std::size_t finish) const {
        auto size = finish - start;
        std::pmr::string output(resource_);
        output.resize(size);
        wal_file_->read(output.data(), size, start);
        return output;
    }

    wal::id_t loader_t::read_wal_id(std::size_t start_index) const {
        auto size = read_wal_size(start_index);
        if (size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + size;
            auto output = read_wal_data(start, finish);
            return wal::unpack_wal_id(output);
        }
        return 0;
    }

    wal::record_t loader_t::read_wal_record(std::size_t start_index) const {
        wal::record_t record;
        record.size = read_wal_size(start_index);
        if (record.size > 0) {
            auto start = start_index + sizeof(size_tt);
            auto finish = start + record.size + sizeof(crc32_t);
            auto output = read_wal_data(start, finish);

            const char* crc_ptr = output.data() + record.size;
            record.crc32 = (crc32_t(uint8_t(crc_ptr[0])) << 24) |
                           (crc32_t(uint8_t(crc_ptr[1])) << 16) |
                           (crc32_t(uint8_t(crc_ptr[2])) << 8) |
                           (crc32_t(uint8_t(crc_ptr[3])));

            auto computed_crc = static_cast<uint32_t>(absl::ComputeCrc32c({output.data(), record.size}));
            if (record.crc32 == computed_crc) {
                components::serializer::msgpack_deserializer_t deserializer(output);
                record.last_crc32 = static_cast<uint32_t>(deserializer.deserialize_uint64(0));
                record.id = deserializer.deserialize_uint64(1);

                deserializer.advance_array(2);
                record.data = components::logical_plan::node_t::deserialize(&deserializer);
                deserializer.pop_array();
                deserializer.advance_array(3);
                record.params = components::logical_plan::parameter_node_t::deserialize(&deserializer);
                deserializer.pop_array();
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
                    return false;
                }
            }
        }

        return true;
    }

} // namespace services::loader

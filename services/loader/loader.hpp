#pragma once

#include "loaded_state.hpp"

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <services/disk/disk.hpp>

#include <filesystem>
#include <memory>

namespace services::loader {

    // External Loader - reads all data from disk WITHOUT using actors
    // This breaks the deadlock cycle by loading data before actors are started
    //
    // Usage:
    //   1. Create loader with config
    //   2. Call load() to read all data from disk
    //   3. Use returned state to initialize actors
    //   4. Create indexes AFTER actors are started (dispatcher is free)
    class loader_t {
    public:
        explicit loader_t(const configuration::config_disk& config,
                          const configuration::config_wal& wal_config,
                          std::pmr::memory_resource* resource,
                          log_t& log);
        ~loader_t();

        // Check if there is any data to load
        [[nodiscard]] bool has_data() const;

        // Load all data from disk (PHASE 1 - no actors involved)
        // Returns populated state that can be used to initialize memory storage
        [[nodiscard]] loaded_state_t load();

    private:
        // Read databases and collections from disk
        void read_databases_and_collections(loaded_state_t& state);

        // Read documents for all collections
        void read_documents(loaded_state_t& state);

        // Read index definitions from metafile
        void read_index_definitions(loaded_state_t& state);

        // Read WAL checkpoint
        void read_wal_checkpoint(loaded_state_t& state);

        // Read WAL records for replay
        void read_wal_records(loaded_state_t& state);

        // WAL reading helpers
        using size_tt = wal::size_tt;
        using crc32_t = wal::crc32_t;
        size_tt read_wal_size(std::size_t start_index) const;
        std::pmr::string read_wal_data(std::size_t start, std::size_t finish) const;
        wal::id_t read_wal_id(std::size_t start_index) const;
        wal::record_t read_wal_record(std::size_t start_index) const;
        std::size_t next_wal_index(std::size_t start_index, size_tt size) const;

        // Index validation helper
        bool is_index_valid(const std::filesystem::path& index_path) const;

        std::pmr::memory_resource* resource_;
        log_t log_;
        configuration::config_disk config_;
        configuration::config_wal wal_config_;
        std::unique_ptr<disk::disk_t> disk_;
        core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> metafile_indexes_;
        std::unique_ptr<core::filesystem::file_handle_t> wal_file_;
    };

} // namespace services::loader

#pragma once

#include <filesystem>
#include <string>
#include <vector>

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

namespace services::wal {

    /// Standalone WAL reader for startup recovery.
    ///
    /// Used by base_spaces.cpp (and similar bootstrap code) to replay committed
    /// WAL records across all databases without requiring the actor system to be
    /// running. This is a non-actor utility class.
    class wal_reader_t {
    public:
        wal_reader_t(const configuration::config_wal& config, log_t& log);

        /// Read all committed records across all databases whose wal_id > after_wal_id.
        ///
        /// Scans config_.path for database subdirectories, reads all segment files
        /// in each, applies the 2-pass committed-transaction filter, and returns
        /// the merged result sorted by wal_id ascending.
        std::vector<record_t> read_committed_records(id_t after_wal_id);

    private:
        /// Read all records from segment files in a single database directory.
        std::vector<record_t> read_database_segments(const std::filesystem::path& db_dir,
                                                     id_t after_wal_id);

        configuration::config_wal config_;
        log_t log_;
    };

} // namespace services::wal

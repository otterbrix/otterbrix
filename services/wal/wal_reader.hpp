#pragma once

#include <cstdint>
#include <filesystem>
#include <set>
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
        ///
        /// When committed_out is non-null, the union of committed transaction ids
        /// across all scanned databases is written into it. The bitcask index
        /// txn-log recover gate (M1.1) needs this set to discard frames of
        /// transactions whose WAL commit marker never landed: index txn-log frames
        /// are fsync'd durable BEFORE the WAL commit marker, so a crash inside that
        /// window would otherwise resurrect uncommitted transactions' index
        /// entries. The set is threaded out (not derived in the index layer) so it
        /// stays byte-identical with the filter applied here.
        std::vector<record_t> read_committed_records(id_t after_wal_id,
                                                     std::set<std::uint64_t>* committed_out = nullptr);

    private:
        /// Read all records from segment files in a single database directory.
        /// committed_out, when non-null, receives this database's committed txn ids.
        std::vector<record_t> read_database_segments(const std::filesystem::path& db_dir,
                                                     id_t after_wal_id,
                                                     std::set<std::uint64_t>* committed_out);

        configuration::config_wal config_;
        log_t log_;
    };

} // namespace services::wal

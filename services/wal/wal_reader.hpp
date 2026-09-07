#pragma once

#include <cstdint>
#include <filesystem>
#include <set>
#include <string>
#include <vector>

#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <core/result_wrapper.hpp>
#include <memory_resource>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

namespace services::wal {

    /// Standalone, non-actor WAL reader: used by base_spaces.cpp during startup recovery, before
    /// the actor system is running.
    class wal_reader_t {
    public:
        /// resource backs the decoded records and every diagnostic below.
        wal_reader_t(std::pmr::memory_resource* resource, const configuration::config_wal& config, log_t& log);

        /// Returns records from every database under config_.path, sorted by wal_id ascending. When
        /// committed_out is non-null, it receives the COMMIT IDS of every durable COMMIT marker
        /// scanned, for the bitcask index txn-log recover gate to discard frames whose WAL commit
        /// marker never landed (index frames are fsync'd durable before the WAL marker). Commit ids,
        /// not txn ids: txn ids recycle per-process (restart at TRANSACTION_ID_START), so an earlier
        /// incarnation's COMMIT marker could vouch for a later transaction reusing the same id.
        ///
        /// REFUSES when a segment cannot be opened — an empty list would be indistinguishable from
        /// "nothing to replay" (see base_spaces.cpp's caller for why that stops startup).
        core::result_wrapper_t<std::vector<record_t>>
        read_committed_records(id_t after_wal_id, std::set<std::uint64_t>* committed_out = nullptr);

    private:
        /// committed_out, when non-null, receives this database's committed COMMIT IDS.
        core::result_wrapper_t<std::vector<record_t>> read_database_segments(const std::filesystem::path& db_dir,
                                                                             id_t after_wal_id,
                                                                             std::set<std::uint64_t>* committed_out);

        std::pmr::memory_resource* resource_;
        configuration::config_wal config_;
        log_t log_;
    };

} // namespace services::wal

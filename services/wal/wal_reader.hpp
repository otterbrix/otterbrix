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

    /// Standalone WAL reader for startup recovery.
    ///
    /// Used by base_spaces.cpp (and similar bootstrap code) to replay committed
    /// WAL records across all databases without requiring the actor system to be
    /// running. This is a non-actor utility class.
    class wal_reader_t {
    public:
        /// resource backs the decoded records and every diagnostic below.
        wal_reader_t(std::pmr::memory_resource* resource, const configuration::config_wal& config, log_t& log);

        /// Read all committed records across all databases whose wal_id > after_wal_id.
        ///
        /// Scans config_.path for database subdirectories, reads all segment files
        /// in each, applies the 2-pass committed-transaction filter, and returns
        /// the merged result sorted by wal_id ascending.
        ///
        /// When committed_out is non-null, the COMMIT IDS of every durable COMMIT marker across
        /// all scanned databases are written into it, for the bitcask index txn-log recover gate
        /// to discard frames whose WAL commit marker never landed (index frames are fsync'd
        /// durable BEFORE the WAL commit marker).
        ///
        /// Commit ids, not txn ids: txn ids are recycled per-process
        /// (transaction_manager_t::next_transaction_id_ restarts at TRANSACTION_ID_START), so a
        /// set of txn ids would let an EARLIER incarnation's COMMIT marker vouch for a LATER
        /// one's transaction of the same id. A commit id is issued at most once in the database's
        /// life (restore_commit_clock raises current_timestamp_ past the durable frontier on
        /// every reopen), so membership here means exactly "this transaction committed".
        ///
        /// Three places still say "txn ids" and are stale-but-harmless (the set travels UNREAD,
        /// only the words are wrong): base_spaces.cpp's local `committed_txn_ids` and
        /// base_spaces.hpp's parameter name; wal.hpp's filter_committed_records committed_out
        /// param, now always passed nullptr; and
        /// integration/cpp/test/test_index_txn_log_routing.cpp's header comment.
        ///
        /// REFUSES when a segment cannot be OPENED — an empty list would be indistinguishable
        /// from "nothing to replay" (see base_spaces.cpp's caller for why that stops startup).
        core::result_wrapper_t<std::vector<record_t>>
        read_committed_records(id_t after_wal_id, std::set<std::uint64_t>* committed_out = nullptr);

    private:
        /// Read all records from segment files in a single database directory.
        /// committed_out, when non-null, receives this database's committed COMMIT IDS.
        core::result_wrapper_t<std::vector<record_t>> read_database_segments(const std::filesystem::path& db_dir,
                                                                             id_t after_wal_id,
                                                                             std::set<std::uint64_t>* committed_out);

        std::pmr::memory_resource* resource_;
        configuration::config_wal config_;
        log_t log_;
    };

} // namespace services::wal

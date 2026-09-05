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
        /// When committed_out is non-null, the COMMIT IDS carried by every durable COMMIT
        /// marker across all scanned databases are written into it. The bitcask index txn-log
        /// recover gate needs this set to discard frames of transactions whose WAL commit
        /// marker never landed: index txn-log frames are fsync'd durable BEFORE the WAL commit
        /// marker, so a crash inside that window would otherwise resurrect uncommitted
        /// transactions' index entries. The set is threaded out (not derived in the index
        /// layer) so it stays byte-identical with the filter applied here.
        ///
        /// COMMIT IDS AND NOT TXN IDS, AND THE DIFFERENCE IS A DURABILITY BUG. Txn ids are
        /// recycled: transaction_manager_t::next_transaction_id_ restarts at
        /// TRANSACTION_ID_START in every process, so a set of txn ids let a COMMIT marker
        /// written by an EARLIER incarnation vouch for the index frame of a LATER one's
        /// transaction of the same id -- an index entry with no heap row behind it, while
        /// filter_committed_records (ordered by wal id) correctly refused that transaction's
        /// rows. A commit id is issued at most once in the life of the database, because
        /// restore_commit_clock raises current_timestamp_ past the durable frontier at every
        /// reopen, so membership here means exactly "this transaction committed".
        ///
        /// WHAT STILL SAYS "TXN IDS" AND IS NO LONGER TRUE, listed once, here, because the
        /// files it lives in were outside the change that made it stale. Nothing on this list is
        /// a defect: the set travels from here to the gate UNREAD, so only the words are wrong.
        ///   * base_spaces.cpp -- the local this set is captured into is named
        ///     committed_txn_ids and is forwarded, unread, to bootstrap_indexes_sync and from
        ///     there to each bitcask agent (the TYPE did not change, so nothing there had to).
        ///     The name and the comments around it, plus base_spaces.hpp's parameter, describe
        ///     the old contents.
        ///   * wal.hpp -- filter_committed_records' own committed_out parameter now has NO
        ///     caller passing it anything but nullptr (this file and wal_worker_t::load are the
        ///     only two), so it is dead, and its comment still names this function as its
        ///     consumer.
        ///   * integration/cpp/test/test_index_txn_log_routing.cpp -- its header says bitcask
        ///     replays "those journalled frames whose txn_id the WAL marked committed"; it is
        ///     the commit id that is marked and matched now. The test itself checks only that
        ///     the two families leave different artefacts, so it is unaffected.
        ///
        /// REFUSES when a segment cannot be OPENED. An empty list for that case is
        /// indistinguishable from "there is nothing to replay", so a startup that could not
        /// read a segment would come up silently missing every committed transaction the
        /// segment held. See the caller in base_spaces.cpp for why that refusal stops startup.
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

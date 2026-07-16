#include "wal_reader.hpp"

#include <algorithm>
#include <set>

#include <services/wal/wal_page_reader.hpp>

namespace services::wal {

    wal_reader_t::wal_reader_t(const configuration::config_wal& config, log_t& log)
        : config_(config)
        , log_(log.clone()) {
        trace(log_, "wal_reader::create , path : {}", config_.path.string());
    }

    // -----------------------------------------------------------------------
    // read_committed_records
    //
    // 1. Scan config_.path for database subdirectories.
    // 2. For each, read all segment files via wal_page_reader_t.
    // 3. 2-pass filter: collect committed txn_ids, keep matching physical records.
    // 4. Merge all databases, sort by wal_id ascending.
    // -----------------------------------------------------------------------

    std::vector<record_t> wal_reader_t::read_committed_records(id_t after_wal_id,
                                                               std::set<std::uint64_t>* committed_out) {
        std::vector<record_t> merged;

        if (!std::filesystem::exists(config_.path)) {
            trace(log_, "wal_reader::read_committed_records , WAL path does not exist : {}", config_.path.string());
            return merged;
        }

        // Pass A: read EVERY database stream raw and union the commit markers.
        // A COMMIT marker is a global fact keyed by txn id — a txn's PHYSICAL
        // records and its COMMIT marker can land in DIFFERENT per-database
        // streams (the agent routes each record by ctx.database_oid with a
        // main_database fallback), so no stream may be filtered against only its
        // own markers: that mis-classifies committed work as uncommitted.
        std::vector<record_t> all_records;
        std::set<uint64_t> committed_txns;
        for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
            if (!entry.is_directory()) {
                continue;
            }
            trace(log_,
                  "wal_reader::read_committed_records , scanning database '{}'",
                  entry.path().filename().string());
            auto db_records = read_database_raw(entry.path(), after_wal_id);
            for (auto& r : db_records) {
                if (r.is_commit_marker() && r.is_valid()) {
                    committed_txns.insert(r.transaction_id);
                }
                all_records.push_back(std::move(r));
            }
        }
        if (committed_out != nullptr) {
            committed_out->insert(committed_txns.begin(), committed_txns.end());
        }

        // Pass B: keep committed-txn records, system records, and — repeat
        // history — the PLACEMENT records (PHYSICAL_INSERT / PHYSICAL_UPDATE) of
        // uncommitted txns on user tables, flagged txn_committed=false. An
        // uncommitted txn's appends occupied physical row-ids in the live run
        // (abort reverts marks, not placement), so every later positional record
        // was captured against a numbering that includes them; replay must place
        // them too (and retire them dead) or those records misresolve. Uncommitted
        // catalog records stay filtered: catalog rows ride the swap protocol and
        // are physically unwound at abort.
        merged.reserve(all_records.size());
        for (auto& r : all_records) {
            if (!r.is_valid()) {
                continue;
            }
            if (r.transaction_id == 0 || committed_txns.count(r.transaction_id) > 0) {
                merged.push_back(std::move(r));
            } else if ((r.record_type == wal_record_type::PHYSICAL_INSERT ||
                        r.record_type == wal_record_type::PHYSICAL_UPDATE) &&
                       r.table_oid >= components::catalog::FIRST_USER_OID) {
                r.txn_committed = false;
                merged.push_back(std::move(r));
            }
        }

        // Sort the merged result by wal_id ascending.
        std::sort(merged.begin(), merged.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_reader::read_committed_records , total committed records : {}", merged.size());
        return merged;
    }

    // -----------------------------------------------------------------------
    // read_database_segments
    //
    // Find segment files in the database directory, read all records,
    // apply the 2-pass committed-transaction filter.
    // -----------------------------------------------------------------------

    std::vector<record_t> wal_reader_t::read_database_raw(const std::filesystem::path& db_dir, id_t after_wal_id) {
        // Discover segment files. WAL segments are named wal_<db>_NNNNNN.
        std::vector<std::filesystem::path> segments;

        for (const auto& entry : std::filesystem::directory_iterator(db_dir)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            auto fname = entry.path().filename().string();
            if (fname.size() >= 4 && fname.compare(0, 4, "wal_") == 0) {
                segments.push_back(entry.path());
            }
        }

        // Sort by filename (lexicographic on zero-padded suffix).
        std::sort(segments.begin(), segments.end());

        // Read all records from all segments.
        std::vector<record_t> all_records;

        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(seg_path);

            // Verify CRC chain. On corruption, log warning. read_all_records
            // will still return valid records up to the corruption point (STOP-A).
            bool chain_ok = reader.verify_chain();
            if (!chain_ok) {
                warn(log_,
                     "wal_reader , CRC chain broken in segment '{}' , "
                     "stopping at corruption point",
                     seg_path.filename().string());
            }

            auto seg_records = reader.read_all_records(after_wal_id);
            for (auto& r : seg_records) {
                all_records.push_back(std::move(r));
            }

            // If the chain was broken, do not read subsequent segments from this
            // database -- data after the corruption point is unreliable.
            if (!chain_ok) {
                break;
            }
        }

        return all_records;
    }

} // namespace services::wal

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

    std::vector<record_t> wal_reader_t::read_committed_records(id_t after_wal_id) {
        std::vector<record_t> merged;

        if (!std::filesystem::exists(config_.path)) {
            trace(log_, "wal_reader::read_committed_records , WAL path does not exist : {}",
                  config_.path.string());
            return merged;
        }

        for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
            if (!entry.is_directory()) {
                continue;
            }

            auto db_name = entry.path().filename().string();
            trace(log_, "wal_reader::read_committed_records , scanning database '{}'", db_name);

            auto db_records = read_database_segments(entry.path(), after_wal_id);
            for (auto& r : db_records) {
                merged.push_back(std::move(r));
            }
        }

        // Sort the merged result by wal_id ascending.
        std::sort(merged.begin(), merged.end(),
                  [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_reader::read_committed_records , total committed records : {}",
              merged.size());
        return merged;
    }

    // -----------------------------------------------------------------------
    // read_database_segments
    //
    // Find segment files in the database directory, read all records,
    // apply the 2-pass committed-transaction filter.
    // -----------------------------------------------------------------------

    std::vector<record_t>
    wal_reader_t::read_database_segments(const std::filesystem::path& db_dir,
                                         id_t after_wal_id) {
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
                warn(log_, "wal_reader , CRC chain broken in segment '{}' , "
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

        // Pass 1: collect committed transaction IDs from COMMIT records.
        std::set<uint64_t> committed_txns;
        for (const auto& r : all_records) {
            if (r.is_commit_marker() && r.is_valid()) {
                committed_txns.insert(r.transaction_id);
            }
        }

        // Pass 2: keep only records belonging to committed transactions.
        std::vector<record_t> result;
        result.reserve(all_records.size());

        for (auto& r : all_records) {
            if (!r.is_valid()) {
                continue;
            }
            // Records with txn_id==0 are "system" records (always included).
            // Physical and commit records are included if their txn is committed.
            if (r.transaction_id == 0 ||
                committed_txns.count(r.transaction_id) > 0) {
                result.push_back(std::move(r));
            }
        }

        return result;
    }

} // namespace services::wal

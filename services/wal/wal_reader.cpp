#include "wal_reader.hpp"

#include <algorithm>
#include <set>

// filter_committed_records — the ONE committed-record filter, shared with wal_worker_t::load.
#include <services/wal/wal.hpp>
#include <services/wal/wal_page_reader.hpp>

namespace services::wal {

    wal_reader_t::wal_reader_t(std::pmr::memory_resource* resource, const configuration::config_wal& config, log_t& log)
        : resource_(resource)
        , config_(config)
        , log_(log.clone()) {
        trace(log_, "wal_reader::create , path : {}", config_.path.string());
    }

    // -----------------------------------------------------------------------
    // read_committed_records
    //
    // 1. Scan config_.path for database subdirectories.
    // 2. For each, read all segment files via wal_page_reader_t.
    // 3. filter_committed_records: keep a physical record only when a COMMIT marker for
    //    its txn id sits at a STRICTLY GREATER wal id (txn ids are recycled across restarts).
    // 4. Export the surviving markers' COMMIT IDS into committed_out (the index txn-log
    //    recover gate's set -- see the declaration for why it is commit ids and not txn ids).
    // 5. Merge all databases, sort by wal_id ascending.
    // -----------------------------------------------------------------------

    core::result_wrapper_t<std::vector<record_t>>
    wal_reader_t::read_committed_records(id_t after_wal_id, std::set<std::uint64_t>* committed_out) {
        std::vector<record_t> merged;

        if (!std::filesystem::exists(config_.path)) {
            trace(log_, "wal_reader::read_committed_records , WAL path does not exist : {}", config_.path.string());
            return std::move(merged);
        }

        for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
            if (!entry.is_directory()) {
                continue;
            }

            auto db_name = entry.path().filename().string();
            // Same classification as the manager's startup scan (parse_database_dir_name,
            // base.hpp) — the two walks must never disagree, or a foreign-named directory
            // could be replayed while its wal ids never bounded the id allocator.
            components::catalog::oid_t db_oid;
            if (!parse_database_dir_name(db_name, db_oid)) {
                warn(log_,
                     "wal_reader::read_committed_records , '{}' under the WAL root is not a database oid "
                     "directory , skipping it (the engine never writes this name)",
                     db_name);
                continue;
            }
            trace(log_, "wal_reader::read_committed_records , scanning database '{}'", db_name);

            // committed_out collects the union of committed COMMIT IDS across all
            // databases (read_database_segments inserts this db's ids into it).
            auto db_records = read_database_segments(entry.path(), after_wal_id, committed_out);
            if (db_records.has_error()) {
                return db_records.error();
            }
            for (auto& r : db_records.value()) {
                merged.push_back(std::move(r));
            }
        }

        // Sort the merged result by wal_id ascending.
        std::sort(merged.begin(), merged.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_reader::read_committed_records , total committed records : {}", merged.size());
        return std::move(merged);
    }

    // -----------------------------------------------------------------------
    // read_database_segments
    //
    // Find segment files in the database directory, read all records, apply the shared
    // wal-id-ordered committed-transaction filter (filter_committed_records, wal.hpp).
    // -----------------------------------------------------------------------

    core::result_wrapper_t<std::vector<record_t>>
    wal_reader_t::read_database_segments(const std::filesystem::path& db_dir,
                                         id_t after_wal_id,
                                         std::set<std::uint64_t>* committed_out) {
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
            wal_page_reader_t reader(resource_, seg_path);

            // An unopenable segment is not survivable like a CRC break: a CRC break still yields
            // every record before it (STOP-A truncates at a known point), but an unopened segment
            // yields NOTHING while later segments open fine, so continuing would replay a range
            // with a HOLE in the middle. Refuse and let the caller decide (base_spaces.cpp declines
            // to start).
            if (!reader.is_open()) {
                error(log_,
                      "wal_reader , segment '{}' could not be opened , replay refuses rather than coming up "
                      "without the transactions it holds: {}",
                      seg_path.filename().string(),
                      reader.open_error().what);
                return reader.open_error();
            }

            // Verify CRC chain. read_all_records will still return valid records up to the
            // corruption point (STOP-A).
            //
            // Two different events, logged differently: a break with nothing verifiable past it
            // is the ordinary crash-torn tail (replay loses no whole page), but pages still
            // verifying past the break mean committed transactions sit beyond where replay
            // reaches, and stay that way until the segment is repaired — logged at error level.
            const auto scan = reader.scan_pages();
            const bool chain_ok = scan.chain_intact;
            if (!chain_ok && scan.verified_pages_after_break > 0) {
                error(log_,
                      "wal_reader , CRC chain broken in segment '{}' at data page {} , {} later page(s) still "
                      "verify , REPLAY STOPS HERE and the committed transactions after the break (ids up to {}) "
                      "are NOT re-applied , restore or repair the segment to replay them",
                      seg_path.filename().string(),
                      scan.first_broken_page,
                      scan.verified_pages_after_break,
                      scan.highest_page_end_lsn);
            } else if (!chain_ok) {
                warn(log_,
                     "wal_reader , CRC chain broken in segment '{}' at data page {} , nothing verifies after it , "
                     "replay stops there and loses no whole page",
                     seg_path.filename().string(),
                     scan.first_broken_page);
            }

            auto seg_records = reader.read_all_records(after_wal_id);
            if (seg_records.has_error()) {
                return seg_records.error();
            }
            for (auto& r : seg_records.value()) {
                all_records.push_back(std::move(r));
            }

            // If the chain was broken, do not read subsequent segments from this
            // database -- data after the corruption point is unreliable.
            if (!chain_ok) {
                break;
            }
        }

        // Uses the SHARED filter (filter_committed_records, wal.hpp) that wal_worker_t::load also
        // applies: an independent copy here would test membership by txn id, which is recycled
        // across restarts and would promote uncommitted records under a stale marker.
        auto committed = filter_committed_records(std::move(all_records), nullptr);

        // Export is taken from the filtered result, not the filter's own committed_out param:
        // that param answers in TXN IDS, which are recycled across restarts and so cannot
        // identify a transaction durably. nullptr is passed above instead, and COMMIT IDS are
        // read off the markers here — a commit_id is issued at most once in the database's life.
        // Zero is not a commit id (the clock starts at 1), so a zero-stamped marker is not exported.
        if (committed_out != nullptr) {
            for (const auto& r : committed) {
                if (r.is_commit_marker() && r.commit_id != 0) {
                    committed_out->insert(r.commit_id);
                }
            }
        }
        return std::move(committed);
    }

} // namespace services::wal

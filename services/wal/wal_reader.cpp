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
            // THE SAME classification the manager's startup scan applies
            // (parse_database_dir_name, base.hpp). Replay must not walk EVERY
            // directory: a foreign-named one would be replayed in full while the
            // manager refuses to manage it, and the wal ids it carries never bound
            // the id allocator — next_wal_id() could then reissue ids UNDER records
            // this replay had already applied. Foreign content is skipped LOUDLY
            // here exactly as it is there; the two walks must never disagree.
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

            // AN UNOPENABLE SEGMENT IS NOT THE SAME FAILURE AS A BROKEN CRC CHAIN, and only
            // the second one is survivable here. A CRC break still yields every record before
            // the break, so the STOP-A below truncates the replay at a known point and what
            // came earlier is complete. A segment that never opened yields NOTHING, and the
            // segments after it open fine — so continuing would replay a range with a HOLE in
            // the middle: rows whose earlier deletes/updates were never applied. Refuse and
            // let the caller decide; base_spaces.cpp declines to start.
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
            // THE TWO CASES ARE NOT THE SAME EVENT AND MUST NOT SHARE A LOG LINE. A break
            // with nothing verifiable behind it is the ordinary crash-torn tail: replay ends
            // where the writer did and loses no whole page. A break with pages still
            // verifying past it means COMMITTED TRANSACTIONS SIT BEYOND THE POINT REPLAY WILL
            // REACH — they are not re-applied, and no amount of restarting changes that until
            // the segment is repaired or restored. That is the one thing a reader of this log
            // has to be told, and it is told at error level.
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

        // Keep only records belonging to committed transactions. The rule is the SHARED
        // filter (filter_committed_records, wal.hpp) that wal_worker_t::load also applies — a
        // second, independently written copy of it here drifts into testing membership in an
        // unordered set of committed txn ids. Txn ids are recycled across restarts, so that
        // test promotes uncommitted records of the CURRENT incarnation on the strength of a
        // COMMIT marker from a PREVIOUS one.
        auto committed = filter_committed_records(std::move(all_records), nullptr);

        // AND THE EXPORT IS TAKEN FROM THE FILTERED RESULT, NOT FROM THE FILTER'S OWN
        // committed_out. That parameter answers in TXN IDS, and a txn id cannot identify a
        // transaction across a restart — the index txn-log recover gate that consumes this set
        // was applying frames of the CURRENT incarnation's uncommitted transactions on the
        // strength of a marker an EARLIER one wrote under the recycled id. So nullptr is passed
        // above and the COMMIT IDS are read off the markers here: filter_committed_records keeps
        // every valid COMMIT marker in its result, and a marker's commit_id is issued at most
        // once in the life of the database (restore_commit_clock re-derives the clock from the
        // durable frontier at every reopen, unlike next_transaction_id_).
        //
        // ZERO IS NOT A COMMIT ID — the clock starts at 1 — so a marker carrying zero says
        // nothing about any transaction and is not exported; the gate refuses a zero-stamped
        // frame on its own side as well, so neither half depends on the other for it.
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

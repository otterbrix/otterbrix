#include "wal_reader.hpp"

#include <algorithm>
#include <set>

#include <services/wal/wal.hpp>
#include <services/wal/wal_page_reader.hpp>

namespace services::wal {

    wal_reader_t::wal_reader_t(std::pmr::memory_resource* resource, const configuration::config_wal& config, log_t& log)
        : resource_(resource)
        , config_(config)
        , log_(log.clone()) {
        trace(log_, "wal_reader::create , path : {}", config_.path.string());
    }

    core::result_wrapper_t<std::vector<record_t>>
    wal_reader_t::read_committed_records(id_t after_wal_id, std::set<std::uint64_t>* committed_out) {
        std::vector<record_t> merged;

        if (!std::filesystem::exists(config_.path)) {
            trace(log_, "wal_reader::read_committed_records , WAL path does not exist : {}", config_.path.string());
            return merged;
        }

        for (const auto& entry : std::filesystem::directory_iterator(config_.path)) {
            if (!entry.is_directory()) {
                continue;
            }

            auto db_name = entry.path().filename().string();
            // Must classify like the manager's startup scan (parse_database_dir_name, base.hpp).
            components::catalog::oid_t db_oid;
            if (!parse_database_dir_name(db_name, db_oid)) {
                warn(log_,
                     "wal_reader::read_committed_records , '{}' under the WAL root is not a database oid "
                     "directory , skipping it (the engine never writes this name)",
                     db_name);
                continue;
            }
            trace(log_, "wal_reader::read_committed_records , scanning database '{}'", db_name);

            auto db_records = read_database_segments(entry.path(), after_wal_id, committed_out);
            if (db_records.has_error()) {
                return db_records.error();
            }
            for (auto& r : db_records.value()) {
                merged.push_back(std::move(r));
            }
        }

        std::sort(merged.begin(), merged.end(), [](const record_t& a, const record_t& b) { return a.id < b.id; });

        trace(log_, "wal_reader::read_committed_records , total committed records : {}", merged.size());
        return merged;
    }

    core::result_wrapper_t<std::vector<record_t>>
    wal_reader_t::read_database_segments(const std::filesystem::path& db_dir,
                                         id_t after_wal_id,
                                         std::set<std::uint64_t>* committed_out) {
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

        std::sort(segments.begin(), segments.end());

        std::vector<record_t> all_records;

        for (const auto& seg_path : segments) {
            wal_page_reader_t reader(resource_, seg_path);

            // Unlike a CRC break (yields every record before it), an unopened segment yields NOTHING
            // while later ones open fine, opening a HOLE if replay continued; refuse instead.
            if (!reader.is_open()) {
                error(log_,
                      "wal_reader , segment '{}' could not be opened , replay refuses rather than coming up "
                      "without the transactions it holds: {}",
                      seg_path.filename().string(),
                      reader.open_error().what);
                return reader.open_error();
            }

            // read_all_records still returns every record up to the break (STOP-A); if pages verify
            // past it, committed transactions sit beyond replay's reach — logged at error, not warn.
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

            if (!chain_ok) {
                break;
            }
        }

        // Shares wal_worker_t::load's filter: an independent copy would test membership by
        // (recycled) txn id and could promote uncommitted records under a stale marker.
        auto committed = filter_committed_records(std::move(all_records));

        // COMMIT IDS, issued once and never 0.
        if (committed_out != nullptr) {
            for (const auto& r : committed) {
                if (r.is_commit_marker() && r.commit_id != 0) {
                    committed_out->insert(r.commit_id);
                }
            }
        }
        return committed;
    }

} // namespace services::wal

#include "wal_reader.hpp"

#include <algorithm>
#include <set>
#include <system_error>

#include <core/file/list_dir.hpp>
#include <services/wal/wal.hpp>
#include <services/wal/wal_page_reader.hpp>

namespace services::wal {

    core::result_wrapper_t<std::pmr::vector<std::filesystem::path>>
    find_wal_segments(std::pmr::memory_resource* resource,
                      const std::filesystem::path& database_dir,
                      std::string_view prefix) {
        std::pmr::vector<std::filesystem::path> segments(resource);
        const auto refused = [&](const std::string& why) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"wal: the journal segments of " + database_dir.string() +
                                                      " could not be listed: " + why,
                                                  resource});
        };
        std::error_code ec;
        const bool present = std::filesystem::exists(database_dir, ec);
        if (ec) {
            return refused(ec.message());
        }
        if (!present) {
            return segments;
        }
        auto listed = core::filesystem::list_dir(resource, database_dir);
        if (listed.has_error()) {
            return refused(listed.error().what.c_str());
        }
        for (auto& entry : listed.value()) {
            const auto name = entry.path.filename().string();
            if (entry.kind == std::filesystem::file_type::regular && name.size() >= prefix.size() &&
                name.compare(0, prefix.size(), prefix) == 0) {
                segments.push_back(std::move(entry.path));
            }
        }
        std::sort(segments.begin(), segments.end());
        return segments;
    }

    wal_reader_t::wal_reader_t(std::pmr::memory_resource* resource, const configuration::config_wal& config, log_t& log)
        : resource_(resource)
        , config_(config)
        , log_(log.clone()) {
        trace(log_, "wal_reader::create , path : {}", config_.path.string());
    }

    core::error_t wal_reader_t::listing_refused(const std::filesystem::path& dir, const std::error_code& ec) {
        core::error_t refusal(core::error_code_t::io_error,
                              std::pmr::string{"wal_reader: the directory " + dir.string() +
                                                   " could not be listed, replay refuses rather than coming up "
                                                   "without what it holds: " +
                                                   ec.message(),
                                               resource_});
        error(log_, "{}", refusal.what);
        return refusal;
    }

    core::result_wrapper_t<std::vector<record_t>>
    wal_reader_t::read_committed_records(std::set<std::uint64_t>* committed_out) {
        std::vector<record_t> merged;

        std::error_code ec;
        if (!std::filesystem::exists(config_.path, ec)) {
            if (ec) {
                return listing_refused(config_.path, ec);
            }
            trace(log_, "wal_reader::read_committed_records , WAL path does not exist : {}", config_.path.string());
            return merged;
        }

        auto listed = core::filesystem::list_dir(resource_, config_.path);
        if (listed.has_error()) {
            error(log_,
                  "wal_reader: replay refuses rather than coming up without what it cannot see: {}",
                  listed.error().what);
            return listed.error();
        }
        for (const auto& entry : listed.value()) {
            if (entry.kind != std::filesystem::file_type::directory) {
                continue;
            }

            auto db_name = entry.path.filename().string();
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

            auto db_records = read_database_segments(entry.path, committed_out);
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
    wal_reader_t::read_database_segments(const std::filesystem::path& db_dir, std::set<std::uint64_t>* committed_out) {
        auto found = find_wal_segments(resource_, db_dir, "wal_");
        if (found.has_error()) {
            error(log_, "wal_reader: {}", found.error().what);
            return found.error();
        }
        const auto& segments = found.value();

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

            auto seg_records = reader.read_all_records(id_t{0});
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

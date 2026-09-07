#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_binary.hpp>
#include <services/wal/wal_page_reader.hpp>
#include <services/wal/wal_page_writer.hpp>
#include <services/wal/wal_sync_mode.hpp>

namespace services::wal {

    using session_id_t = components::session::session_id_t;

    // The one committed-record filter shared by both replay readers (a second copy already drifted
    // apart once): a record is committed only when a COMMIT marker for the same txn sits at a
    // STRICTLY GREATER wal id -- txn ids are reused across restarts, but wal ids keep growing.
    [[nodiscard]] inline std::vector<record_t> filter_committed_records(std::vector<record_t>&& records,
                                                                        std::set<std::uint64_t>* committed_out) {
        std::map<std::uint64_t, std::vector<id_t>> commits_by_txn;
        for (const auto& r : records) {
            if (r.is_commit_marker() && r.is_valid()) {
                commits_by_txn[r.transaction_id].push_back(r.id);
            }
        }
        for (auto& entry : commits_by_txn) {
            std::sort(entry.second.begin(), entry.second.end());
        }
        if (committed_out != nullptr) {
            for (const auto& entry : commits_by_txn) {
                committed_out->insert(entry.first);
            }
        }

        std::vector<record_t> result;
        result.reserve(records.size());
        for (auto& r : records) {
            if (!r.is_valid()) {
                continue;
            }
            if (r.transaction_id == 0 || r.is_commit_marker()) {
                result.push_back(std::move(r));
                continue;
            }
            const auto it = commits_by_txn.find(r.transaction_id);
            if (it == commits_by_txn.end()) {
                continue;
            }
            if (std::upper_bound(it->second.begin(), it->second.end(), r.id) == it->second.end()) {
                continue;
            }
            result.push_back(std::move(r));
        }
        return result;
    }

    class wal_worker_t final : public actor_zeta::actor::basic_actor<wal_worker_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        wal_worker_t(std::pmr::memory_resource* resource,
                     log_t& log,
                     configuration::config_wal config,
                     components::catalog::oid_t database_oid);

        ~wal_worker_t();

        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        /// Parses a `wal_<db>_NNNNNN` filename; (uint32_t)-1 (never half-parsed) if the shape doesn't match.
        static uint32_t parse_segment_index(const std::filesystem::path& path, const std::string& db_dir_name);

        unique_future<core::result_wrapper_t<std::vector<record_t>>> load(session_id_t session, wal::id_t after_wal_id);

        unique_future<core::result_wrapper_t<wal::id_t>> commit_txn(session_id_t session,
                                                                    uint64_t transaction_id,
                                                                    wal_sync_mode sync_mode,
                                                                    wal::id_t wal_id,
                                                                    uint64_t commit_id);

        // Refuses rather than deleting: an unreadable segment reads as 'empty', and unlinking on that would destroy it.
        unique_future<core::error_t> truncate_before(session_id_t session, wal::id_t checkpoint_wal_id);

        unique_future<wal::id_t> current_wal_id(session_id_t session);

        unique_future<core::result_wrapper_t<wal::id_t>>
        write_physical_insert(session_id_t session,
                              components::catalog::oid_t table_oid,
                              std::pmr::vector<components::vector::data_chunk_t> chunks,
                              uint64_t row_start,
                              uint64_t row_count,
                              uint64_t txn_id,
                              wal::id_t wal_id);

        unique_future<core::result_wrapper_t<wal::id_t>> write_physical_delete(session_id_t session,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::pmr::vector<int64_t> row_ids,
                                                                               uint64_t count,
                                                                               uint64_t txn_id,
                                                                               wal::id_t wal_id);

        unique_future<core::result_wrapper_t<wal::id_t>>
        write_physical_update(session_id_t session,
                              components::catalog::oid_t table_oid,
                              std::pmr::vector<int64_t> row_ids,
                              std::pmr::vector<components::vector::data_chunk_t> new_chunks,
                              uint64_t count,
                              uint64_t txn_id,
                              wal::id_t wal_id);

        unique_future<core::result_wrapper_t<wal::id_t>>
        write_physical_add_column(session_id_t session,
                                  components::catalog::oid_t table_oid,
                                  std::unique_ptr<components::vector::data_chunk_t> schema_chunk,
                                  uint64_t column_count,
                                  uint64_t txn_id,
                                  wal::id_t wal_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&wal_worker_t::load,
                                                            &wal_worker_t::commit_txn,
                                                            &wal_worker_t::truncate_before,
                                                            &wal_worker_t::current_wal_id,
                                                            &wal_worker_t::write_physical_insert,
                                                            &wal_worker_t::write_physical_delete,
                                                            &wal_worker_t::write_physical_update,
                                                            &wal_worker_t::write_physical_add_column>;

    private:
        /// Refuses when a segment can't be opened -- skipping one breaks the CRC chain and page_lsn ordering.
        [[nodiscard]] core::error_t recover_from_disk();

        /// Build a segment file path for the given segment index.
        std::filesystem::path segment_path(uint32_t seg_index) const;

        /// Collect all segment file paths sorted by index.
        std::vector<std::filesystem::path> discover_segments() const;

        /// Rotates when full; refuses if the segment can't open or the pre-rotation flush missed disk.
        [[nodiscard]] core::error_t ensure_writer();

        /// Unlink one segment, reporting a failed unlink instead of discarding it.
        void remove_segment(const std::filesystem::path& seg_path);

        log_t log_;
        configuration::config_wal config_;
        components::catalog::oid_t database_oid_;
        std::string database_dir_name_; // numeric string of database_oid_, used as path component
        std::filesystem::path database_dir_;

        atomic_id_t id_{0};
        crc32_t last_crc_{0};
        uint32_t current_segment_index_{0};

        std::unique_ptr<wal_page_writer_t> writer_;

        /// Set when recover_from_disk() failed; while set, every write and truncate REFUSES.
        core::error_t recovery_error_;

        /// Temporary encode buffer, reused across writes to avoid re-allocation.
        buffer_t encode_buf_;
    };

    using wal_worker_ptr = std::unique_ptr<wal_worker_t, actor_zeta::pmr::deleter_t>;

} // namespace services::wal

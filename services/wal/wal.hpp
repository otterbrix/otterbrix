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

    // THE ONE COMMITTED-RECORD FILTER of the journal, shared by BOTH replay readers —
    // wal_worker_t::load (the CREATE INDEX backfill catchup) and
    // wal_reader_t::read_database_segments (the bootstrap replay). It lives here for the same
    // reason parse_database_dir_name lives in base.hpp: two copies of one rule drifted apart once
    // already, and the two walks must never disagree about what "committed" means.
    //
    // THE RULE IS ORDERED BY wal_id. A physical record at wal id r belongs to a committed
    // transaction only when a COMMIT marker for the SAME txn id sits at a STRICTLY GREATER wal id.
    // Membership in an unordered set of committed txn ids is not enough, because TXN IDS ARE REUSED
    // ACROSS RESTARTS: transaction_manager_t::next_transaction_id_ is a plain
    // {TRANSACTION_ID_START} member that is never seeded from the surviving journal, unlike the
    // commit clock (restore_commit_clock), while wal ids keep growing because recover_from_disk
    // re-derives the allocator from the segment files. So an unordered test lets a COMMIT marker
    // written by the PREVIOUS process vouch for physical records the NEXT one wrote under the
    // recycled id and never committed:
    //
    //   session 1:  wal 1 PHYSICAL_INSERT(txn T)   wal 2 COMMIT(txn T)
    //   -- restart, no checkpoint --
    //   session 2:  wal 3 PHYSICAL_INSERT(txn T)   <crash before COMMIT>
    //   replay:     committed = {T}  ->  wal 3 applied AS COMMITTED
    //
    // The commit marker of a transaction is always written after its physical records (WAL-first:
    // the append handler awaits the PHYSICAL_* future, and only the later commit pipeline sends
    // commit_txn), so "strictly greater" never rejects a genuine one.
    //
    // Records with transaction_id == 0 are system records and are always kept; COMMIT markers are
    // kept as they always were. Invalid records are dropped. committed_out, when non-null, receives
    // the union of the committed txn ids — see wal_reader_t::read_committed_records for its
    // consumer.
    [[nodiscard]] inline std::vector<record_t> filter_committed_records(std::vector<record_t>&& records,
                                                                        std::set<std::uint64_t>* committed_out) {
        // txn id -> the wal ids of its COMMIT markers, ascending.
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
            // A marker at or below this record's own wal id belongs to an EARLIER
            // incarnation of the id and says nothing about this record.
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

        // No manager pointer: all manager interaction is mailbox-only
        // (worker->address()), keeping actors free of shared mutable state.
        wal_worker_t(std::pmr::memory_resource* resource,
                     log_t& log,
                     configuration::config_wal config,
                     components::catalog::oid_t database_oid);

        ~wal_worker_t();

        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        /// Parse the segment index out of a `wal_<db>_NNNNNN` filename. Returns
        /// (uint32_t)-1 when the name is not exactly that shape — a suffix that
        /// merely BEGINS with digits ("000012.bak") is a refusal, never a
        /// half-parsed index. Public and static: pure name arithmetic with no
        /// worker state, pinned directly by the classification tests.
        static uint32_t parse_segment_index(const std::filesystem::path& path, const std::string& db_dir_name);

        // -----------------------------------------------------------------------
        // Internal methods (called by manager, NOT wal_contract)
        // -----------------------------------------------------------------------

        // The wrapper is the difference between "no record past after_wal_id" and "a segment
        // could not be read": a bare vector answers the same empty list for both, and the
        // CREATE INDEX backfill that consumes this would then build an index missing every row
        // the unreadable segment described.
        unique_future<core::result_wrapper_t<std::vector<record_t>>> load(session_id_t session, wal::id_t after_wal_id);

        // commit_id is the MVCC version timestamp allocated by
        // transaction_manager_t::commit(); written into the COMMIT record so
        // snapshot-aware replay restores published_horizon_.
        unique_future<core::result_wrapper_t<wal::id_t>> commit_txn(session_id_t session,
                                                                    uint64_t transaction_id,
                                                                    wal_sync_mode sync_mode,
                                                                    wal::id_t wal_id,
                                                                    uint64_t commit_id);

        // Refuses rather than deleting when a segment cannot be READ: "unreadable" and "empty"
        // collapse into one answer here (page_count() == 0), and unlinking on that answer
        // destroys the unreadable one.
        unique_future<core::error_t> truncate_before(session_id_t session, wal::id_t checkpoint_wal_id);

        unique_future<wal::id_t> current_wal_id(session_id_t session);

        // Every write handler below reports whether the record REACHED the segment. Returning
        // the wal_id unconditionally while dropping wal_page_writer_t::append's answer hands
        // the caller the number of a record that is not in the journal.
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
        // -----------------------------------------------------------------------
        // Startup helpers
        // -----------------------------------------------------------------------

        /// Discover existing segment files, recover max wal_id and last CRC.
        ///
        /// Refuses when a segment cannot be OPENED. That is not a cosmetic report: this scan
        /// is what sets id_ (and, through the manager, global_id_), so a segment whose records
        /// are not seen leaves the allocator BELOW ids that already exist on disk, and every
        /// later write reuses them — breaking both the CRC chain and the page_lsn ordering
        /// that truncate_before and read_all_records(after_id) compare against. Coming up
        /// short is recoverable; coming up and overwriting is not.
        [[nodiscard]] core::error_t recover_from_disk();

        /// Build a segment file path for the given segment index.
        std::filesystem::path segment_path(uint32_t seg_index) const;

        /// Collect all segment file paths sorted by index.
        std::vector<std::filesystem::path> discover_segments() const;

        /// Ensure the page writer is ready; rotate if the current segment is full.
        /// Refuses when the segment cannot be opened, or when the flush that precedes a
        /// rotation did not reach the disk.
        [[nodiscard]] core::error_t ensure_writer();

        /// Unlink one segment, reporting a failed unlink instead of discarding it.
        void remove_segment(const std::filesystem::path& seg_path);

        // -----------------------------------------------------------------------
        // State
        // -----------------------------------------------------------------------
        log_t log_;
        configuration::config_wal config_;
        components::catalog::oid_t database_oid_;
        std::string database_dir_name_; // numeric string of database_oid_, used as path component
        std::filesystem::path database_dir_;

        atomic_id_t id_{0};
        crc32_t last_crc_{0};
        uint32_t current_segment_index_{0};

        std::unique_ptr<wal_page_writer_t> writer_;

        /// Set when recover_from_disk() could not read a segment. While it is set the worker
        /// REFUSES every write and every truncate: the id space it would write into overlaps
        /// records it could not see, and the truncate would unlink files it could not read.
        core::error_t recovery_error_;

        /// Temporary encode buffer, reused across writes to avoid re-allocation.
        buffer_t encode_buf_;
    };

    using wal_worker_ptr = std::unique_ptr<wal_worker_t, actor_zeta::pmr::deleter_t>;

} // namespace services::wal

#pragma once

// drop / clear stay unique_future<void> (no recoverable failure on those
// paths). insert_many / remove_many return unique_future<core::error_t> (M3.5):
// the bitcask txn-log write path can fail on a file open / write / sync, and
// that error is now surfaced rather than aborting the process. The btree /
// non-txn (txn_id==0) branches are still assert+abort terminal and return
// no_error(). manager_index_t commit_inserts/commit_deletes co_await these and
// fold the first error into their returned core::error_t.

#include "disk_hash_table.hpp"
#include "index_disk.hpp"

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <core/executor.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <set>

namespace services::index {

    // Owns its bitcask + btree state exclusively; callers reach it only via
    // mailbox sends to its address (no shared mutable state across the actor
    // boundary).
    //
    // No DROP TABLE GC handler here: on-disk index files sit alongside table
    // files and are unlinked by manager_disk_t's on_horizon_advanced sweep.
    class index_agent_disk_t final : public actor_zeta::basic_actor<index_agent_disk_t> {
        using path_t = std::filesystem::path;
        using session_id_t = ::components::session::session_id_t;
        using value_t = components::types::logical_value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // committed_txn_ids: the WAL-replay set of committed transaction ids,
        // forwarded to the bitcask index txn-log recover gate (M1.1). Fresh,
        // post-bootstrap agents pass an EMPTY set (a fresh dir has no txn-log to
        // gate). The btree / disk_hash branches ignore it (no txn log).
        //
        // index_oid = pg_index.indexrelid; the agent's on-disk directory is
        // ${path_db}/${table_oid}/${index_oid}/ — oid-keyed, never name-keyed.
        index_agent_disk_t(std::pmr::memory_resource* resource,
                           const path_t& path_db,
                           components::catalog::oid_t table_oid,
                           components::catalog::oid_t index_oid,
                           components::logical_plan::index_type type,
                           uint64_t bitcask_flush_threshold,
                           uint64_t bitcask_segment_record_limit,
                           uint64_t btree_flush_threshold,
                           log_t& log,
                           std::pmr::set<std::uint64_t> committed_txn_ids,
                           disk_hash_table_ptr shared_hash_index);
        ~index_agent_disk_t();

        components::catalog::oid_t table_oid() const { return table_oid_; }

        unique_future<void> drop(session_id_t session);
        // Wipe the agent's stored index data while keeping the agent alive and
        // writable (bitcask: segments + hash + txn-log + applied-offset
        // sidecar; btree: tree contents/file). NOT the terminal drop. Used by
        // the runtime repopulate path: txn_id==0 re-inserts then take the
        // direct (non-txn-log) write path.
        unique_future<void> clear(session_id_t session);
        unique_future<core::error_t>
        insert_many(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<core::error_t>
        remove_many(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);

        // THE read: every row id whose key satisfies `compare` against `key`, duplicates
        // included.
        //
        // This is the ONLY way a committed row of ANY disk-backed index reaches a reader
        // — the hashed one since C1, the ordered one since C2b, which is why the message
        // carries the predicate instead of meaning equality by name. The split below is
        // the contract index_disk.hpp draws, not a type test:
        //   eq            -> find(), which every backend answers. For bitcask that means
        //                    reading the SNAPSHOT RECORD and unrolling the whole row
        //                    list; its keydir cannot answer the question, keeping one
        //                    entry per key whose payload field is `rows.back()`
        //                    (append_snapshot), so a reader that consults it silently
        //                    drops every duplicate.
        //   lt/lte/gt/gte/ne -> scan_range(), the ORDERED contract. Only an ordered
        //                    backend answers it; a hashed one has no ordering to scan and
        //                    fails LOUDLY rather than returning an empty range. The guard
        //                    that keeps a range off a hashed index is upstream, in
        //                    manager_index_t, which asks the index whether it supports an
        //                    ordered probe before dispatching anything.
        //
        // ONE STEP, no cursor. The whole matched set comes back in this single reply
        // (owner decision 4: parity with the pre-mailbox read; a cursor was weighed and
        // deferred). The cost is written down here rather than discovered later: the cap
        // from `LIMIT` is applied ABOVE, in index_scan::open_index_window, so a key
        // matching a million rows ships ~8 MB of ids (8 bytes each) across the mailbox
        // even for `LIMIT 10`. That is the price of the deferral, and the place to look
        // when it starts to hurt is a cursor message, not a cap sneaked in here — an
        // index that answers with a SUBSET is a wrong answer, not a fast one.
        //
        // Answered on THIS actor's resource(), and wrapped: an empty vector means "no
        // row satisfies the predicate", never "the read did not happen".
        unique_future<core::result_wrapper_t<std::pmr::vector<int64_t>>>
        read_rows(session_id_t session, components::expressions::compare_type compare, value_t key);

        // Mailbox flush handler — fanned out by manager_index_t::flush_all_indexes.
        // Guards on is_dropped_ internally (a dropped agent has no backing), then
        // forces the backend to persist. Ordered behind any pending insert/remove
        // ops in this agent's FIFO, so it never races an in-flight write.
        unique_future<void> force_flush(session_id_t session);

        using dispatch_traits = actor_zeta::dispatch_traits<&index_agent_disk_t::drop,
                                                            &index_agent_disk_t::clear,
                                                            &index_agent_disk_t::insert_many,
                                                            &index_agent_disk_t::remove_many,
                                                            &index_agent_disk_t::read_rows,
                                                            &index_agent_disk_t::force_flush>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        log_t log_;
        std::unique_ptr<index_disk_t> index_disk_;
        components::catalog::oid_t table_oid_;
        bool is_dropped_{false};
    };

    using index_agent_disk_ptr = std::unique_ptr<index_agent_disk_t, actor_zeta::pmr::deleter_t>;

} //namespace services::index

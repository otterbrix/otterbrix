#pragma once

// drop / clear stay unique_future<void> (no recoverable failure on those
// paths). insert_many / remove_many return unique_future<core::error_t> (M3.5):
// the bitcask txn-log write path can fail on a file open / write / sync, and
// that error is now surfaced rather than aborting the process. The btree /
// non-txn (txn_id==0) branches are still assert+abort terminal and return
// no_error(). manager_index_t commit_inserts/commit_deletes co_await these and
// fold the first error into their returned core::error_t.

#include "index_disk.hpp"
#include "disk_hash_table.hpp"

#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <core/executor.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <core/btree/btree.hpp>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <set>

namespace services::index {

    using index_name_t = std::string;

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
        index_agent_disk_t(std::pmr::memory_resource* resource,
                           const path_t& path_db,
                           components::catalog::oid_t table_oid,
                           const index_name_t& index_name,
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

        // Mailbox flush handler — fanned out by manager_index_t::flush_all_indexes.
        // Guards on is_dropped_ internally (a dropped agent has no backing), then
        // forces the backend to persist. Ordered behind any pending insert/remove
        // ops in this agent's FIFO, so it never races an in-flight write.
        unique_future<void> force_flush(session_id_t session);

        using dispatch_traits = actor_zeta::dispatch_traits<&index_agent_disk_t::drop,
                                                            &index_agent_disk_t::clear,
                                                            &index_agent_disk_t::insert_many,
                                                            &index_agent_disk_t::remove_many,
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
    using index_agent_disk_storage_t = core::pmr::btree::btree_t<index_name_t, index_agent_disk_ptr>;

} //namespace services::index

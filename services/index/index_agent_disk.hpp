#pragma once

// Error propagation: write-path methods (insert / insert_many / remove /
// remove_many / drop / force_flush) return unique_future<void> because the
// underlying bitcask_index_disk_t / btree_index_disk_t methods are
// assert+abort terminal — no recoverable failure path to surface. When the
// underlying types gain a non-terminal error mode (returning core::error_t),
// these signatures will switch to unique_future<core::error_t> — matching
// the manager_index_t commit_insert / commit_delete pattern.

#include "index_disk.hpp"

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
#include <core/btree/btree.hpp>
#include <cstdint>
#include <filesystem>

namespace services::index {

    using index_name_t = std::string;

    // index_agent_disk_t owns its own bitcask + btree state exclusively;
    // callers reach it through mailbox sends to its address (no shared
    // mutable state across actor boundary).
    //
    // Deferred DROP TABLE GC: persisted on-disk index files are unlinked by
    // manager_disk_t's on_horizon_advanced sweep (storage layout puts index
    // files alongside table files), so no separate GC handler is needed here.
    class index_agent_disk_t final : public actor_zeta::basic_actor<index_agent_disk_t> {
        using path_t = std::filesystem::path;
        using session_id_t = ::components::session::session_id_t;
        using value_t = components::types::logical_value_t;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        index_agent_disk_t(std::pmr::memory_resource* resource,
                           const path_t& path_db,
                           components::catalog::oid_t table_oid,
                           const index_name_t& index_name,
                           components::logical_plan::index_type type,
                           uint64_t bitcask_flush_threshold,
                           uint64_t bitcask_segment_record_limit,
                           uint64_t btree_flush_threshold,
                           log_t& log);
        ~index_agent_disk_t();

        components::catalog::oid_t table_oid() const { return table_oid_; }
        bool is_dropped() const;

        unique_future<void> drop(session_id_t session);
        unique_future<void> insert(session_id_t session, value_t key, size_t row_id);
        unique_future<void>
        insert_many(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<void> remove(session_id_t session, value_t key, size_t row_id);
        unique_future<void>
        remove_many(session_id_t session, uint64_t txn_id, std::vector<std::pair<value_t, size_t>> values);
        unique_future<index_disk_t::result>
        find(session_id_t session, value_t value, components::expressions::compare_type compare);
        unique_future<void> force_flush(session_id_t session);

        // Synchronous flush — bypasses actor mailbox, safe to call from owning manager
        void force_flush_sync();

        using dispatch_traits = actor_zeta::dispatch_traits<&index_agent_disk_t::drop,
                                                            &index_agent_disk_t::insert,
                                                            &index_agent_disk_t::insert_many,
                                                            &index_agent_disk_t::remove,
                                                            &index_agent_disk_t::remove_many,
                                                            &index_agent_disk_t::find,
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

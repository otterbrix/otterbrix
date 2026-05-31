#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include "index_agent_disk.hpp"

#include <core/executor.hpp>
#include <core/result_wrapper.hpp>

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/expressions/forward.hpp>
#include <components/index/forward.hpp>
#include <components/index/index_engine.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/session/session.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <filesystem>
#include <memory>
#include <memory_resource>
#include <string>
#include <vector>

namespace services::index {

    using execution_context_t = components::execution_context_t;

    // Per-table index agent. Block D of the actor-zeta refactor introduces this
    // class as the eventual owner of a per-table index_engine_t.
    //
    // STATUS (Block D Final cleanup — engine ownership migrated):
    // * All 14 DML/MVCC/query/metadata/maintenance handlers are declared here
    //   and wired in dispatch_traits. Bodies use the engine_ptr_ dispatch path:
    //   when bound, the handler operates on the agent-owned engine; when
    //   unbound it returns an empty / no-op result and the manager-side
    //   engines_ fallback handles the request (legacy path for un-migrated
    //   oids).
    // * Engine ownership has moved: base_spaces Phase 3.5 calls
    //   manager_index_t::take_engine_ownership_sync per oid and hands the
    //   resulting core::pmr::unique_ptr<index_engine_t> to set_engine_owned_sync
    //   below. The agent holds the unique_ptr in engine_owned_ and engine_ptr_
    //   aliases its heap address.
    // * After Phase 3.5, manager_index_t::engines_ has no entry for migrated
    //   oids — the per_table_agents_ router forwards every routed message to
    //   the owning agent, so the engines_-based fallback paths in the manager
    //   only fire for oids that have not yet been migrated (e.g. runtime
    //   CREATE TABLE before its agent gets spawned).
    //
    // + dec 46: search / search_with_preferred_type /
    // has_index hot-path handlers are now declared here and routed through
    // manager_index_t::enqueue_impl delegation forward (Option D mailbox). Like
    // the DML handlers, their bodies are no-op stubs (empty vector / false)
    // until the engine ownership migration binds engine_ptr_ — manager_index's
    // existing engines_ body is the truthful answer in the meantime, reached
    // via the empty-result fallback in the router (see manager_index.cpp
    // ::search and ::has_index).
    //
    // get_indexed_keys / get_indexed_descriptions metadata-read
    // handlers are also declared here and routed via per_table_agents_ with the
    // same empty-vector fallback pattern as search/has_index.
    //
    // DDL routing: create_index_local / drop_index_local /
    // apply_wal_record_for_index_local now live here as well. The manager-side
    // create_index / drop_index / apply_wal_record_for_index handlers forward
    // to the agent when per_table_agents_ has an entry; they fall back to
    // engines_ otherwise. Closes the regression where bootstrap-bound oids
    // saw silent DDL failures after the engine ownership migration emptied
    // engines_.
    class index_table_agent_t final : public actor_zeta::basic_actor<index_table_agent_t> {
        using session_id_t = ::components::session::session_id_t;
        using index_name_t = std::string;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        index_table_agent_t(std::pmr::memory_resource* resource,
                            log_t& log,
                            components::catalog::oid_t table_oid);

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // Bootstrap hook — direct (non-mailbox) call, valid only during
        // base_spaces Phase 3.5 before scheduler start. Sets engine_ptr_ as a
        // non-owning view; the engine is owned elsewhere (manager_index_t::
        // engines_ for un-migrated oids). Prefer set_engine_owned_sync below
        // for the engine ownership migration path.
        void set_engine_sync(components::index::index_engine_t* engine) noexcept { engine_ptr_ = engine; }

        // Final cleanup — engine ownership migration entry point.
        // Direct (non-mailbox) call, valid only during base_spaces Phase 3.5
        // before scheduler start. Transfers ownership of the per-table
        // index_engine_t to this agent: engine_owned_ takes the unique_ptr and
        // engine_ptr_ is rebound to its heap-stable address.
        //
        // Once an agent has been bound via this method, all routed DML / query
        // / metadata handlers operate on the agent-owned engine; manager_index_t::
        // engines_[oid] is empty for that oid and the router's engines_-based
        // fallback never fires (per_table_agents_ lookup hits first).
        //
        // Caller contract: the engine instance must not be referenced from any
        // other actor or path after this call returns — actor-zeta constraint
        // #11 (no shared mutable state between actors). Today the only callers
        // are base_spaces Phase 3.5 (single-threaded bootstrap).
        void set_engine_owned_sync(components::index::index_engine_ptr engine) {
            engine_owned_ = std::move(engine);
            engine_ptr_ = engine_owned_.get();
        }

        // DDL routing — direct (non-mailbox) bootstrap setter for the
        // disk-spawn configuration required by create_index_local / drop_index_local.
        // base_spaces Phase 3.5 invokes this right after set_engine_owned_sync so
        // every agent leaves bootstrap ready to handle DDL traffic for its oid.
        //
        // We keep these as agent-owned state (path layout + bitcask/btree
        // thresholds + scheduler_raw for disk-agent enqueue) rather than
        // copying them from manager on every DDL call — they are immutable for
        // the agent's lifetime, mirroring the constants manager_index_t holds
        // in its own member fields.
        //
        // The scheduler is needed because the agent now spawns / drives
        // index_agent_disk_t actors directly: actor_zeta::send returns a
        // needs_sched flag and the framework requires us to scheduler->enqueue
        // the receiver on its first message just like manager_index_t's
        // schedule_agent helper did.
        //
        // NOT a mailbox handler — single-threaded callers only (bootstrap path).
        void set_disk_config_sync(std::filesystem::path path_db,
                                  uint64_t bitcask_flush_threshold,
                                  uint64_t bitcask_segment_record_limit,
                                  uint64_t btree_flush_threshold,
                                  actor_zeta::scheduler_raw scheduler) {
            path_db_ = std::move(path_db);
            bitcask_flush_threshold_ = bitcask_flush_threshold;
            bitcask_segment_record_limit_ = bitcask_segment_record_limit;
            btree_flush_threshold_ = btree_flush_threshold;
            scheduler_ = scheduler;
        }

        // DML — txn-aware bulk index operations.
        // Bodies are no-op stubs for this iteration; the actual work still runs
        // in manager_index_t. See class comment above.
        unique_future<void> insert_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        uint64_t start_row_id,
                                        uint64_t count);
        unique_future<void> delete_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> data,
                                        std::pmr::vector<int64_t> row_ids);
        unique_future<void> update_rows(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::unique_ptr<components::vector::data_chunk_t> old_data,
                                        std::unique_ptr<components::vector::data_chunk_t> new_data,
                                        std::pmr::vector<int64_t> row_ids,
                                        int64_t new_start_row_id);

        // MVCC commit/revert. core::error_t is returned directly
        // (no_error() = success, contains_error() = failure).
        unique_future<core::error_t>
        commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<core::error_t>
        commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id);
        unique_future<void> revert_insert(execution_context_t ctx, components::catalog::oid_t table_oid);

        // Query — txn-aware. Block D Pass 9 dec 2 + dec 46: search hot-path is
        // routed through the per-table agent via manager_index::enqueue_impl
        // delegation forward (Option D mailbox). Bodies are no-op stubs today
        // (engine_ptr_ still nullptr in steady state) — when engine_ptr_ is
        // bound by the eventual ownership-migration step the bound branch will
        // forward the query to engine_ptr_->search. Until then manager_index's
        // existing engines_ body handles the work via the feature-flag
        // fallback. Returning an empty vector here is safe only because the
        // caller (manager_index_t::search router) co_returns the agent future
        // ONLY when present — see manager_index.cpp::search comment.
        unique_future<std::pmr::vector<int64_t>>
        search(session_id_t session,
               components::catalog::oid_t table_oid,
               components::index::keys_base_storage_t keys,
               components::types::logical_value_t value,
               components::expressions::compare_type compare,
               uint64_t start_time,
               uint64_t txn_id,
               core::date::timezone_offset_t session_tz);

        unique_future<std::pmr::vector<int64_t>>
        search_with_preferred_type(session_id_t session,
                                   components::catalog::oid_t table_oid,
                                   components::index::keys_base_storage_t keys,
                                   components::types::logical_value_t value,
                                   components::expressions::compare_type compare,
                                   components::logical_plan::index_type preferred_type,
                                   uint64_t start_time,
                                   uint64_t txn_id,
                                   core::date::timezone_offset_t session_tz);

        unique_future<bool>
        has_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name);

        // index metadata-read handlers. Same mailbox-routed
        // shape as search/has_index: manager_index forwards via per_table_agents_
        // when an agent is registered, takes the agent answer when non-empty,
        // and otherwise falls through to its engines_ body. Bodies are stubs
        // today (engine_ptr_ stays nullptr in steady state) so the empty return
        // always triggers the fallback — the truthful metadata still lives on
        // manager_index_t::engines_ until engine ownership migrates.
        unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
        get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid);

        unique_future<std::pmr::vector<components::index::index_description_t>>
        get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid);

        // (Pass 9 Variant 2) — per-table maintenance handlers.
        // The broadcast wrappers on manager_index_t (cleanup_all_versions /
        // flush_all_indexes) fan one of these messages out per registered
        // per-table agent and collect their futures; rebuild_indexes is the
        // per-oid route (no fanout — manager forwards by per_table_agents_
        // lookup, same shape as insert_rows). Bodies follow the engine_ptr_
        // guard pattern of the DML stubs above: nullptr -> no-op success
        // (manager_index_t still owns engines_ and the fallback runs there),
        // bound -> routes to engine_ptr_->{cleanup_versions, matching(...)->
        // clean_memory_to_new_elements(0)}. flush_indexes stays a logging
        // no-op on the bound branch until disk-agent ownership migrates onto
        // the per-table agent (force_flush_sync still fans out via manager).
        unique_future<void> cleanup_versions(execution_context_t ctx,
                                             components::catalog::oid_t table_oid,
                                             uint64_t lowest_active_start_time);
        unique_future<void> rebuild_indexes(session_id_t session,
                                            components::catalog::oid_t table_oid);
        unique_future<void> flush_indexes(session_id_t session,
                                          components::catalog::oid_t table_oid);

        // DDL routing — per-table DDL handlers. The regression closed
        // here: after engine ownership migration (Phase 3.5), manager_index_t::
        // engines_ has no entry for bootstrap-bound oids, so the legacy
        // engines_-based DDL bodies on manager fall through silently for every
        // system / user table known at startup. These handlers operate on the
        // agent-owned engine (engine_owned_ / engine_ptr_) so CREATE INDEX /
        // DROP INDEX / CREATE INDEX WAL catchup keep working post-migration.
        //
        // The manager-side handlers (create_index / drop_index /
        // apply_wal_record_for_index) route here via per_table_agents_ lookup
        // when an agent is registered, and fall back to engines_ otherwise (the
        // un-migrated path — runtime CREATE TABLE before its agent gets
        // spawned, deferred until the runtime spawn step lands).
        //
        // The disk-agent fanout (index_agent_disk_t spawn / drop) lives on the
        // agent now: disk_agents_ moved out of manager_index_t so the agent is
        // the sole owner of the per-table physical-index lifecycle. Constraint
        // #11 holds — the manager forwards by mailbox send and never touches
        // engine_ptr_ or disk_agents_ directly.
        unique_future<uint32_t>
        create_index_local(session_id_t session,
                           components::catalog::oid_t table_oid,
                           index_name_t index_name,
                           components::index::keys_base_storage_t keys,
                           components::logical_plan::index_type type,
                           core::date::timezone_offset_t session_tz);
        unique_future<void>
        drop_index_local(session_id_t session,
                         components::catalog::oid_t table_oid,
                         index_name_t index_name);
        unique_future<void>
        apply_wal_record_for_index_local(session_id_t session,
                                         components::catalog::oid_t table_oid,
                                         components::catalog::oid_t index_oid,
                                         uint64_t wal_record_id,
                                         uint8_t record_type,
                                         std::pmr::vector<int64_t> row_ids,
                                         std::unique_ptr<components::vector::data_chunk_t> physical_data,
                                         uint64_t physical_row_start,
                                         uint64_t txn_id,
                                         core::date::timezone_offset_t session_tz);

        using dispatch_traits = actor_zeta::dispatch_traits<&index_table_agent_t::insert_rows,
                                                            &index_table_agent_t::delete_rows,
                                                            &index_table_agent_t::update_rows,
                                                            &index_table_agent_t::commit_insert,
                                                            &index_table_agent_t::commit_delete,
                                                            &index_table_agent_t::revert_insert,
                                                            &index_table_agent_t::search,
                                                            &index_table_agent_t::search_with_preferred_type,
                                                            &index_table_agent_t::has_index,
                                                            &index_table_agent_t::get_indexed_keys,
                                                            &index_table_agent_t::get_indexed_descriptions,
                                                            &index_table_agent_t::cleanup_versions,
                                                            &index_table_agent_t::rebuild_indexes,
                                                            &index_table_agent_t::flush_indexes,
                                                            &index_table_agent_t::create_index_local,
                                                            &index_table_agent_t::drop_index_local,
                                                            &index_table_agent_t::apply_wal_record_for_index_local>;

    private:
        log_t log_;
        components::catalog::oid_t table_oid_;

        // Final cleanup — engine ownership migration.
        //
        // engine_owned_ is the agent's owned index_engine_t (populated by
        // set_engine_owned_sync during base_spaces Phase 3.5). When held, it is
        // the exclusive owner of the engine instance: manager_index_t::engines_
        // has no entry for this oid, and no other actor references it
        // (constraint #11). engine_ptr_ is a non-owning view that aliases the
        // heap address of *engine_owned_ when owned, or points at an
        // externally-owned engine when only set_engine_sync was called.
        //
        // Lifetime ordering: engine_ptr_ is declared after engine_owned_ so its
        // destruction precedes the unique_ptr's. set_engine_owned_sync
        // reassigns engine_ptr_ AFTER the move, so it's always valid relative
        // to the current engine_owned_.
        //
        // For the un-migrated bootstrap path (set_engine_sync(raw)), engine_owned_
        // stays empty and engine_ptr_ points to the engine still living under
        // manager_index_t::engines_[oid]. The hot-path handlers don't care
        // which case they're in — they only dereference engine_ptr_.
        // engine_owned_ is constructed in the ctor body (deleter requires a
        // memory_resource — not default-constructible). Empty unique_ptr is
        // the steady state for un-migrated oids.
        components::index::index_engine_ptr engine_owned_;
        components::index::index_engine_t* engine_ptr_{nullptr};

        // DDL routing — per-table disk-agent config + storage.
        // path_db_/threshold fields mirror the manager_index_t::* values; they
        // are populated once by set_disk_config_sync during base_spaces
        // Phase 3.5 bootstrap and stay immutable for the agent's lifetime.
        // disk_agents_ holds the index_agent_disk_t actors spawned by
        // create_index_local — each disk agent is owned exclusively by this
        // agent (constraint #11) and lives until drop_index_local removes it.
        std::filesystem::path path_db_;
        uint64_t bitcask_flush_threshold_{1000};
        uint64_t bitcask_segment_record_limit_{100};
        uint64_t btree_flush_threshold_{1000};
        actor_zeta::scheduler_raw scheduler_{nullptr};
        std::vector<index_agent_disk_ptr> disk_agents_;

        // DDL routing — small helper mirroring manager_index_t::
        // schedule_agent. Walks disk_agents_ and calls scheduler_->enqueue on
        // the matching disk actor when needs_sched is true. No-op when the
        // address is no longer in disk_agents_ (already dropped) or when the
        // scheduler hasn't been wired yet.
        void schedule_disk_agent(const actor_zeta::address_t& addr, bool needs_sched) noexcept;
    };

    using index_table_agent_ptr = std::unique_ptr<index_table_agent_t, actor_zeta::pmr::deleter_t>;

} // namespace services::index

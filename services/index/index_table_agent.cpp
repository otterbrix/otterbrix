#include "index_table_agent.hpp"

#include "bitcask_index_disk.hpp"
#include "btree_index_disk.hpp"
#include "disk_hash_table.hpp"

#include <actor-zeta/spawn.hpp>
#include <components/index/disk_hash_single_field_index.hpp>
#include <components/index/hash_single_field_index.hpp>
#include <components/index/single_field_index.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <core/file/local_file_system.hpp>
#include <msgpack.hpp>
#include <services/wal/record.hpp>

#include <algorithm>
#include <exception>
#include <filesystem>

namespace {
    using namespace core::b_plus_tree;

    // Block D DDL routing — local copies of the btree key/id getters used by
    // create_index_local's persistent-storage replay. Mirrors the anon-namespace
    // helpers in manager_index.cpp so the agent body can stay self-contained;
    // both copies are byte-equivalent and operate on the same on-disk format.
    auto idx_item_key_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/0");
    };

    auto idx_id_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/1");
    };

    using value_t = components::types::logical_value_t;
    using namespace components::types;

    value_t idx_reverse_convert(std::pmr::memory_resource* r, const physical_value& pv) {
        switch (pv.type()) {
            case physical_type::BOOL:
                return value_t(r, pv.value<physical_type::BOOL>());
            case physical_type::UINT8:
                return value_t(r, pv.value<physical_type::UINT8>());
            case physical_type::INT8:
                return value_t(r, pv.value<physical_type::INT8>());
            case physical_type::UINT16:
                return value_t(r, pv.value<physical_type::UINT16>());
            case physical_type::INT16:
                return value_t(r, pv.value<physical_type::INT16>());
            case physical_type::UINT32:
                return value_t(r, pv.value<physical_type::UINT32>());
            case physical_type::INT32:
                return value_t(r, pv.value<physical_type::INT32>());
            case physical_type::UINT64:
                return value_t(r, pv.value<physical_type::UINT64>());
            case physical_type::INT64:
                return value_t(r, pv.value<physical_type::INT64>());
            case physical_type::FLOAT:
                return value_t(r, pv.value<physical_type::FLOAT>());
            case physical_type::DOUBLE:
                return value_t(r, pv.value<physical_type::DOUBLE>());
            case physical_type::STRING: {
                auto sv = pv.value<physical_type::STRING>();
                return value_t(r, std::string(sv));
            }
            default:
                return value_t(r, complex_logical_type{logical_type::NA});
        }
    }
} // anonymous namespace

namespace services::index {

    index_table_agent_t::index_table_agent_t(std::pmr::memory_resource* resource,
                                             log_t& log,
                                             components::catalog::oid_t table_oid)
        : actor_zeta::basic_actor<index_table_agent_t>(resource)
        , log_(log.clone())
        , table_oid_(table_oid)
        // core::pmr::unique_ptr uses a deleter_t that captures the memory_resource
        // used for the eventual deallocation. Initialise empty (no engine yet);
        // set_engine_owned_sync rebinds via move-assignment from the manager's
        // unique_ptr, which carries its own deleter — the empty starting state
        // never invokes this deleter.
        , engine_owned_(nullptr, core::pmr::deleter_t(resource)) {
        trace(log_, "index_table_agent::create (table_oid={})", static_cast<unsigned>(table_oid_));
    }

    auto index_table_agent_t::make_type() const noexcept -> const char* { return "index_table_agent"; }

    actor_zeta::behavior_t index_table_agent_t::behavior(actor_zeta::mailbox::message* msg) {
        switch (msg->command()) {
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::insert_rows>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::insert_rows, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::delete_rows>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::delete_rows, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::update_rows>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::update_rows, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::commit_insert>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::commit_insert, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::commit_delete>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::commit_delete, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::revert_insert>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::revert_insert, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::search>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::search, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::search_with_preferred_type>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::search_with_preferred_type, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::has_index>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::has_index, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::get_indexed_keys>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::get_indexed_keys, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::get_indexed_descriptions>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::get_indexed_descriptions, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::cleanup_versions>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::cleanup_versions, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::rebuild_indexes>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::rebuild_indexes, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::flush_indexes>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::flush_indexes, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::create_index_local>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::create_index_local, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::drop_index_local>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::drop_index_local, msg);
                break;
            case actor_zeta::msg_id<index_table_agent_t, &index_table_agent_t::apply_wal_record_for_index_local>:
                co_await actor_zeta::dispatch(this, &index_table_agent_t::apply_wal_record_for_index_local, msg);
                break;
            default:
                break;
        }
    }

    // ---------------------------------------------------------------------------
    // Block D Final cleanup — engine_ptr_ guard dispatch.
    //
    // Each handler is structured around a guard on engine_ptr_:
    //   * engine_ptr_ == nullptr  -> no-op success (un-migrated oid: agent was
    //                                spawned but ownership was not transferred,
    //                                so the manager_index_t::engines_ fallback
    //                                in the router handles the request).
    //   * engine_ptr_ != nullptr  -> agent owns the engine via engine_owned_;
    //                                run the operation directly against
    //                                engine_ptr_, mirroring the body that
    //                                manager_index_t::* used to run.
    //
    // After base_spaces Phase 3.5 takes engine ownership via
    // set_engine_owned_sync, engine_ptr_ is non-null for every oid known at
    // bootstrap, and the bound branch is the hot path.
    // ---------------------------------------------------------------------------

    index_table_agent_t::unique_future<void>
    index_table_agent_t::insert_rows(execution_context_t ctx,
                                     components::catalog::oid_t /*table_oid*/,
                                     std::unique_ptr<components::vector::data_chunk_t> data,
                                     uint64_t start_row_id,
                                     uint64_t count) {
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning success preserves the existing feature-flag fallback in
            // manager_index — manager handles DML through its own engines_ entry.
            trace(log_,
                  "index_table_agent::insert_rows[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D engine_ptr_ branch — mirror of manager_index_t::insert_rows
        // body (lines 628-636). The manager router already early-returns after
        // forwarding to the agent (manager_index.cpp::insert_rows), so this is
        // the sole writer for the engine when an agent is registered — no
        // double-work risk.
        if (!data || count == 0) {
            co_return;
        }
        const auto txn_id = ctx.txn.transaction_id;
        for (uint64_t i = 0; i < count; ++i) {
            engine_ptr_->insert_row(*data,
                                    i,
                                    static_cast<int64_t>(start_row_id + i),
                                    txn_id,
                                    ctx.session_tz);
        }
        // No disk mirroring — uncommitted entries don't go to disk (mirrors
        // manager_index_t::insert_rows comment).
        co_return;
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::delete_rows(execution_context_t ctx,
                                     components::catalog::oid_t /*table_oid*/,
                                     std::unique_ptr<components::vector::data_chunk_t> data,
                                     std::pmr::vector<int64_t> row_ids) {
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning success preserves the existing feature-flag fallback in
            // manager_index — manager handles DML through its own engines_ entry.
            trace(log_,
                  "index_table_agent::delete_rows[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D engine_ptr_ branch — mirror of manager_index_t::delete_rows
        // body (lines 662-670). Manager router already early-returns after
        // forwarding to the agent, so this is the sole writer when an agent is
        // registered — no double-work risk.
        if (!data || row_ids.empty()) {
            co_return;
        }
        const auto txn_id = ctx.txn.transaction_id;
        for (size_t i = 0; i < row_ids.size(); ++i) {
            engine_ptr_->mark_delete_row(*data, i, row_ids[i], txn_id, ctx.session_tz);
        }
        // No disk mirroring — uncommitted deletes don't go to disk (mirrors
        // manager_index_t::delete_rows comment).
        co_return;
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::update_rows(execution_context_t ctx,
                                     components::catalog::oid_t /*table_oid*/,
                                     std::unique_ptr<components::vector::data_chunk_t> old_data,
                                     std::unique_ptr<components::vector::data_chunk_t> new_data,
                                     std::pmr::vector<int64_t> row_ids,
                                     int64_t new_start_row_id) {
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning success preserves the existing feature-flag fallback in
            // manager_index — manager handles DML through its own engines_ entry.
            trace(log_,
                  "index_table_agent::update_rows[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D engine_ptr_ branch — mirror of manager_index_t::update_rows
        // body (lines 700-715). Manager router already early-returns after
        // forwarding to the agent, so this is the sole writer when an agent is
        // registered — no double-work risk. No disk mirroring: uncommitted
        // entries don't go to disk (mirrors manager_index update_rows body).
        if (!old_data || !new_data || row_ids.empty()) {
            co_return;
        }
        const auto txn_id = ctx.txn.transaction_id;
        // Phase 1 — mark old entries as deleted.
        for (size_t i = 0; i < row_ids.size(); ++i) {
            engine_ptr_->mark_delete_row(*old_data, i, row_ids[i], txn_id, ctx.session_tz);
        }
        // Phase 2 — insert new entries.
        for (size_t i = 0; i < row_ids.size(); ++i) {
            engine_ptr_->insert_row(*new_data,
                                    i,
                                    new_start_row_id + static_cast<int64_t>(i),
                                    txn_id,
                                    ctx.session_tz);
        }
        co_return;
    }

    index_table_agent_t::unique_future<result_t>
    index_table_agent_t::commit_insert(execution_context_t ctx,
                                       components::catalog::oid_t /*table_oid*/,
                                       uint64_t commit_id) {
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning success preserves the existing feature-flag fallback in
            // manager_index — manager handles DML through its own engines_ entry.
            trace(log_,
                  "index_table_agent::commit_insert[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return result_t::success();
        }
        // Block D follow-up wiring — in-memory MVCC flip via engine_ptr_.
        // Disk persistence fanout (for_each_pending_disk_insert ->
        // index_agent_disk_t batched insert_many) is still owned by
        // manager_index_t::commit_insert because the disk_agents lifetime
        // stays under the manager for this iteration; that fanout runs BEFORE
        // the router forwards here, or — on the fallback path — runs inline
        // in manager. We perform only the engine-level pending->committed
        // flip for both the txn-local pending map and the global
        // (txn_id == 0) map, mirroring the manager body.
        const auto txn_id = ctx.txn.transaction_id;
        engine_ptr_->commit_insert(txn_id, commit_id);
        if (txn_id != 0) {
            engine_ptr_->commit_insert(0, commit_id);
        }
        trace(log_,
              "index_table_agent::commit_insert[{}] engine bound; txn={} commit={}",
              static_cast<unsigned>(table_oid_),
              txn_id,
              commit_id);
        co_return result_t::success();
    }

    index_table_agent_t::unique_future<result_t>
    index_table_agent_t::commit_delete(execution_context_t ctx,
                                       components::catalog::oid_t /*table_oid*/,
                                       uint64_t commit_id) {
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning success preserves the existing feature-flag fallback in
            // manager_index — manager handles DML through its own engines_ entry.
            trace(log_,
                  "index_table_agent::commit_delete[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return result_t::success();
        }
        // Block D follow-up wiring — in-memory MVCC flip via engine_ptr_.
        // Same disk-fanout deferral as commit_insert: the
        // for_each_pending_disk_delete -> index_agent_disk_t batched
        // remove_many step stays with manager_index until disk_agents
        // ownership migrates. Here we just flip the engine's
        // pending->committed maps for both txn-local and global
        // (txn_id == 0).
        const auto txn_id = ctx.txn.transaction_id;
        engine_ptr_->commit_delete(txn_id, commit_id);
        if (txn_id != 0) {
            engine_ptr_->commit_delete(0, commit_id);
        }
        trace(log_,
              "index_table_agent::commit_delete[{}] engine bound; txn={} commit={}",
              static_cast<unsigned>(table_oid_),
              txn_id,
              commit_id);
        co_return result_t::success();
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::revert_insert(execution_context_t ctx,
                                       components::catalog::oid_t /*table_oid*/) {
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning success preserves the existing feature-flag fallback in
            // manager_index — manager handles DML through its own engines_ entry.
            trace(log_,
                  "index_table_agent::revert_insert[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D follow-up wiring — revert is a pure in-memory drop of the
        // txn's pending entries. Uncommitted entries never reached disk, so
        // no disk-agent fanout is required (matches manager_index body).
        const auto txn_id = ctx.txn.transaction_id;
        engine_ptr_->revert_insert(txn_id);
        trace(log_,
              "index_table_agent::revert_insert[{}] engine bound; txn={}",
              static_cast<unsigned>(table_oid_),
              txn_id);
        co_return;
    }

    // ---------------------------------------------------------------------------
    // Block D Pass 9 dec 2 + dec 46 — search hot-path routes through
    // manager_index_t::enqueue_impl delegation forward (Option D mailbox).
    //
    // Each handler:
    //   * engine_ptr_ == nullptr  -> empty result / false (manager_index's
    //                                feature-flag fallback runs its existing
    //                                engines_ body via the router).
    //   * engine_ptr_ != nullptr  -> dispatches directly to engine_ptr_->
    //                                matching/search/has_index, mirroring the
    //                                manager_index_t bodies.
    //
    // The empty/false return on the unbound branch is observable ONLY when the
    // caller is the Block D router in manager_index.cpp::search — that router
    // co_returns the agent future when an agent IS registered, so today's V4
    // bootstrap (which does not bind engines) plus this stub is equivalent to
    // the agent existing but reporting "I have no index for that oid". The
    // surrounding manager_index code must therefore retain its existing engines_
    // body and choose between routing vs falling through; see the comment in
    // manager_index.cpp::search.
    // ---------------------------------------------------------------------------

    index_table_agent_t::unique_future<std::pmr::vector<int64_t>>
    index_table_agent_t::search(session_id_t /*session*/,
                                components::catalog::oid_t /*table_oid*/,
                                components::index::keys_base_storage_t keys,
                                components::types::logical_value_t value,
                                components::expressions::compare_type compare,
                                uint64_t start_time,
                                uint64_t txn_id,
                                core::date::timezone_offset_t session_tz) {
        std::pmr::vector<int64_t> result(resource());
        if (engine_ptr_ == nullptr) {
            // Block D engine ownership migration not yet activated for this oid.
            // Returning empty preserves the existing manager_index engines_ body
            // via the router fallback (see manager_index.cpp::search).
            trace(log_,
                  "index_table_agent::search[{}] engine not bound; empty",
                  static_cast<unsigned>(table_oid_));
            co_return result;
        }
        // Block D engine_ptr_ dispatch — mirror of manager_index_t::search body
        // (matching(keys) -> index->search(compare, value, start_time, txn_id,
        // session_tz)). search_index() helper takes an index_engine_ptr&; here
        // we hold a raw engine_ptr_ so we call engine_ptr_->matching() directly.
        auto* index = engine_ptr_->matching(keys);
        if (!index) {
            co_return result;
        }
        co_return index->search(compare, value, start_time, txn_id, session_tz);
    }

    index_table_agent_t::unique_future<std::pmr::vector<int64_t>>
    index_table_agent_t::search_with_preferred_type(session_id_t /*session*/,
                                                    components::catalog::oid_t /*table_oid*/,
                                                    components::index::keys_base_storage_t keys,
                                                    components::types::logical_value_t value,
                                                    components::expressions::compare_type compare,
                                                    components::logical_plan::index_type preferred_type,
                                                    uint64_t start_time,
                                                    uint64_t txn_id,
                                                    core::date::timezone_offset_t session_tz) {
        std::pmr::vector<int64_t> result(resource());
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::search_with_preferred_type[{}] engine not bound; empty",
                  static_cast<unsigned>(table_oid_));
            co_return result;
        }
        // Block D engine_ptr_ dispatch — mirror of
        // manager_index_t::search_with_preferred_type body: prefer the index
        // matching the requested type, fall back to any index over the same keys,
        // then index->search(compare, value, start_time, txn_id, session_tz).
        auto* index = engine_ptr_->matching(keys, preferred_type);
        if (!index) {
            index = engine_ptr_->matching(keys);
        }
        if (!index) {
            co_return result;
        }
        co_return index->search(compare, value, start_time, txn_id, session_tz);
    }

    index_table_agent_t::unique_future<bool>
    index_table_agent_t::has_index(session_id_t /*session*/,
                                   components::catalog::oid_t /*table_oid*/,
                                   index_name_t index_name) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::has_index[{}] engine not bound; false",
                  static_cast<unsigned>(table_oid_));
            co_return false;
        }
        // Block D engine_ptr_ dispatch — mirror of manager_index_t::has_index
        // body (engine->has_index(name)).
        co_return engine_ptr_->has_index(index_name);
    }

    // ---------------------------------------------------------------------------
    // Block D Step 4 — index metadata-read handlers (get_indexed_keys /
    // get_indexed_descriptions). Same engine_ptr_ guard pattern as the search
    // handlers above:
    //   * engine_ptr_ == nullptr  -> empty pmr vector. manager_index's router
    //                                treats the empty answer as "agent isn't
    //                                authoritative yet" and falls through to
    //                                its existing engines_ body (truthful
    //                                source while engine ownership migration
    //                                hasn't run).
    //   * engine_ptr_ != nullptr  -> forwards to engine_ptr_->all_indexed_keys()
    //                                / engine_ptr_->all_indexed_descriptions().
    // ---------------------------------------------------------------------------

    index_table_agent_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    index_table_agent_t::get_indexed_keys(session_id_t /*session*/,
                                          components::catalog::oid_t /*table_oid*/) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::get_indexed_keys[{}] engine not bound; empty",
                  static_cast<unsigned>(table_oid_));
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource());
        }
        // Block D engine_ptr_ dispatch — mirror of
        // manager_index_t::get_indexed_keys body (engine->all_indexed_keys()).
        co_return engine_ptr_->all_indexed_keys();
    }

    index_table_agent_t::unique_future<std::pmr::vector<components::index::index_description_t>>
    index_table_agent_t::get_indexed_descriptions(session_id_t /*session*/,
                                                  components::catalog::oid_t /*table_oid*/) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::get_indexed_descriptions[{}] engine not bound; empty",
                  static_cast<unsigned>(table_oid_));
            co_return std::pmr::vector<components::index::index_description_t>(resource());
        }
        // Block D engine_ptr_ dispatch — mirror of
        // manager_index_t::get_indexed_descriptions body
        // (engine->all_indexed_descriptions()).
        co_return engine_ptr_->all_indexed_descriptions();
    }

    // ---------------------------------------------------------------------------
    // Block D Step 6 (Pass 9 Variant 2) — per-table maintenance handlers.
    // cleanup_versions and flush_indexes are the per-oid building blocks that
    // manager_index_t::cleanup_all_versions / flush_all_indexes fan out to via
    // per_table_agents_. rebuild_indexes is the per-oid route mirror of
    // insert_rows (no fanout — manager picks the one agent by oid lookup).
    //
    // Same engine_ptr_ guard pattern as the DML/search stubs above:
    //   * engine_ptr_ == nullptr  -> no-op success. manager_index still owns
    //                                the engine via engines_ and the broadcast
    //                                wrappers also walk engines_ as the
    //                                truthful path until the ownership
    //                                migration ships.
    //   * engine_ptr_ != nullptr  -> routes to engine_ptr_->cleanup_versions
    //                                (cleanup_versions),
    //                                engine_ptr_->matching(name)->
    //                                clean_memory_to_new_elements(0)
    //                                (rebuild_indexes), or logging no-op for
    //                                flush_indexes (disk fanout still owned by
    //                                manager — pending disk-agent ownership
    //                                migration).
    // ---------------------------------------------------------------------------

    index_table_agent_t::unique_future<void>
    index_table_agent_t::cleanup_versions(execution_context_t /*ctx*/,
                                          components::catalog::oid_t /*table_oid*/,
                                          uint64_t lowest_active_start_time) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::cleanup_versions[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D engine_ptr_ branch — mirror of manager_index_t::cleanup_all_versions
        // per-oid body (engine->cleanup_versions(lowest_active)). Manager-side
        // broadcast wrapper awaits the per-table agent future before walking its
        // engines_ map, so a single in-memory cleanup pass runs per oid.
        engine_ptr_->cleanup_versions(lowest_active_start_time);
        trace(log_,
              "index_table_agent::cleanup_versions[{}] engine bound; lowest_active={}",
              static_cast<unsigned>(table_oid_),
              lowest_active_start_time);
        co_return;
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::rebuild_indexes(session_id_t /*session*/,
                                         components::catalog::oid_t /*table_oid*/) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::rebuild_indexes[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D engine_ptr_ branch — mirror of manager_index_t::rebuild_indexes
        // body (lines 947-953). Walk each named index in the engine and clear
        // it via clean_memory_to_new_elements(0). The executor re-populates by
        // streaming scan data back to manager_index for rebuild — that step is
        // unchanged. We hold a raw engine_ptr_ rather than the unique_ptr that
        // components::index::search_index expects, so we call
        // engine_ptr_->matching(name) directly.
        for (auto& idx_name : engine_ptr_->indexes()) {
            auto* idx = engine_ptr_->matching(idx_name);
            if (idx) {
                idx->clean_memory_to_new_elements(0);
            }
        }
        trace(log_,
              "index_table_agent::rebuild_indexes[{}] engine bound; cleared indexes",
              static_cast<unsigned>(table_oid_));
        co_return;
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::flush_indexes(session_id_t /*session*/,
                                       components::catalog::oid_t /*table_oid*/) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::flush_indexes[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }
        // Block D engine_ptr_ branch — flush is a disk-agent concern, not an
        // index_engine_t concern. manager_index_t::flush_all_indexes iterates
        // disk_agents_ and calls force_flush_sync per agent; the in-memory
        // index_engine_t has no flush semantics of its own. Until disk_agents_
        // ownership migrates onto the per-table agent (next migration step),
        // the engine_ptr_-bound branch is intentionally a logging no-op:
        //   * The manager-side flush_all_indexes broadcast wrapper awaits each
        //     per-table agent future, then runs its disk_agents_ force_flush_sync
        //     loop, which is the authoritative flush path.
        //   * Returning early here keeps the agent contract honest (the future
        //     resolves once flush "has been handled" for this oid — which is
        //     true because the broadcast wrapper's own loop will do it).
        // Once disk-agent ownership moves under index_table_agent_t this stays
        // the entry point and the body grows a force_flush_sync over the
        // agent-owned slice.
        trace(log_,
              "index_table_agent::flush_indexes[{}] engine bound; no-op (disk fanout owned by manager)",
              static_cast<unsigned>(table_oid_));
        co_return;
    }

    // ---------------------------------------------------------------------------
    // Block D DDL routing — per-table DDL handlers. Closes the regression
    // introduced by the Phase 3.5 engine ownership migration: after migration,
    // manager_index_t::engines_ has no entry for bootstrap-bound oids, so the
    // legacy engines_-based DDL bodies on manager_index_t::create_index /
    // drop_index / apply_wal_record_for_index fall through silently for every
    // system / user table known at startup. The bodies below mirror those
    // manager bodies but operate on the agent-owned engine via engine_ptr_,
    // and own the per-table disk_agents_ vector that was previously held on
    // the manager.
    // ---------------------------------------------------------------------------

    void index_table_agent_t::schedule_disk_agent(const actor_zeta::address_t& addr, bool needs_sched) noexcept {
        if (!needs_sched || scheduler_ == nullptr) {
            return;
        }
        for (auto& agent : disk_agents_) {
            if (agent->address() == addr) {
                scheduler_->enqueue(agent.get());
                return;
            }
        }
    }

    index_table_agent_t::unique_future<uint32_t>
    index_table_agent_t::create_index_local(session_id_t /*session*/,
                                            components::catalog::oid_t /*table_oid*/,
                                            index_name_t index_name,
                                            components::index::keys_base_storage_t keys,
                                            components::logical_plan::index_type type,
                                            core::date::timezone_offset_t session_tz) {
        trace(log_,
              "index_table_agent::create_index_local[{}] name={}",
              static_cast<unsigned>(table_oid_),
              index_name);

        if (engine_ptr_ == nullptr) {
            // Engine ownership not migrated for this oid — the router on
            // manager_index_t falls back to its engines_ body and runs the
            // legacy DDL path there. Returning INDEX_ID_UNDEFINED is the agreed
            // sentinel for "agent isn't authoritative".
            trace(log_,
                  "index_table_agent::create_index_local[{}] engine not bound; INDEX_ID_UNDEFINED",
                  static_cast<unsigned>(table_oid_));
            co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (engine_ptr_->has_index(index_name)) {
            co_return components::index::INDEX_ID_UNDEFINED;
        }

        // engine_owned_ is the owning unique_ptr — components::index::make_index
        // and components::index::search_index both want an index_engine_ptr&. We
        // pass engine_owned_ directly so the helper templates resolve.
        uint32_t id_index = components::index::INDEX_ID_UNDEFINED;
        switch (type) {
            case components::logical_plan::index_type::single: {
                id_index =
                    components::index::make_index<components::index::single_field_index_t>(engine_owned_, index_name, keys);
                break;
            }
            case components::logical_plan::index_type::hashed: {
                if (path_db_.empty()) {
                    id_index =
                        components::index::make_index<components::index::hash_single_field_index_t>(engine_owned_,
                                                                                                    index_name,
                                                                                                    keys);
                } else {
                    const auto base = path_db_ / std::to_string(static_cast<unsigned>(table_oid_)) / index_name;
                    std::filesystem::create_directories(base);
                    try {
                        id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                            engine_owned_,
                            index_name,
                            keys,
                            std::make_unique<services::index::disk_hash_table_t>(base / "hash_index.bin",
                                                                                  services::index::disk_hash_table_t::default_bucket_count,
                                                                                  true,
                                                                                  resource()));
                    } catch (const std::exception& e) {
                        trace(log_,
                              "index_table_agent::create_index_local: disk hash storage init failed, fallback to memory: {}",
                              e.what());
                        id_index = components::index::make_index<components::index::hash_single_field_index_t>(
                            engine_owned_,
                            index_name,
                            keys);
                    }
                }
                break;
            }
            default:
                trace(log_, "index_table_agent::create_index_local: unsupported index type");
                co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Load index data from btree (persistent storage). Mirrors
            // manager_index_t::create_index lines 457-493.
            if (!path_db_.empty() && type == components::logical_plan::index_type::single) {
                auto btree_path = path_db_ / std::to_string(static_cast<unsigned>(table_oid_)) / index_name;
                if (std::filesystem::exists(btree_path / "metadata")) {
                    try {
                        core::filesystem::local_file_system_t fs;
                        auto db =
                            std::make_unique<core::b_plus_tree::btree_t>(resource(), fs, btree_path, idx_item_key_getter);
                        db->load();

                        if (db->size() > 0) {
                            struct pv_entry {
                                components::types::physical_value key;
                                int64_t row_id;
                            };
                            std::pmr::vector<pv_entry> raw(resource());
                            db->full_scan<pv_entry>(&raw, [](void* data, size_t sz) -> pv_entry {
                                auto item = core::b_plus_tree::btree_t::item_data{
                                    static_cast<core::b_plus_tree::data_ptr_t>(data),
                                    static_cast<uint32_t>(sz)};
                                return {idx_item_key_getter(item),
                                        static_cast<int64_t>(
                                            idx_id_getter(item).value<components::types::physical_type::UINT64>())};
                            });

                            auto* idx = components::index::search_index(engine_owned_, keys);
                            if (idx) {
                                for (auto& e : raw) {
                                    idx->insert(idx_reverse_convert(resource(), e.key), e.row_id, session_tz);
                                }
                                trace(log_,
                                      "index_table_agent::create_index_local: loaded {} entries from btree",
                                      raw.size());
                            }
                        }
                    } catch (const std::exception& e) {
                        trace(log_, "index_table_agent::create_index_local: btree load failed: {}", e.what());
                    }
                }
            }

            // Create disk agent for persistent storage. The agent owns
            // disk_agents_ post-migration — the spawned actor lives here until
            // drop_index_local removes it. We pass address() of self as the
            // "manager" param to set_disk_agent because (a) the disk_manager_
            // field stored by index_t is never actually read by any consumer,
            // and (b) this keeps the per-table agent the sole owner of the
            // disk-agent lifecycle (constraint #11).
            if (!path_db_.empty()) {
                try {
                    auto agent =
                        actor_zeta::spawn<index_agent_disk_t>(resource(),
                                                              path_db_,
                                                              table_oid_,
                                                              std::string(index_name),
                                                              type,
                                                              bitcask_index_disk_t::default_flush_threshold_,
                                                              bitcask_index_disk_t::default_segment_record_limit_,
                                                              btree_index_disk_t::default_flush_threshold_,
                                                              log_);

                    auto* idx = components::index::search_index(engine_owned_, keys);
                    if (idx) {
                        idx->set_disk_agent(agent->address(), address());
                        engine_owned_->add_disk_agent(id_index, agent->address());
                    }

                    disk_agents_.emplace_back(std::move(agent));
                } catch (const std::exception& e) {
                    trace(log_, "index_table_agent::create_index_local: disk agent creation failed: {}", e.what());
                }
            }
        }

        trace(log_,
              "index_table_agent::create_index_local[{}] id_index={}",
              static_cast<unsigned>(table_oid_),
              id_index);
        co_return id_index;
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::drop_index_local(session_id_t session,
                                          components::catalog::oid_t /*table_oid*/,
                                          index_name_t index_name) {
        trace(log_,
              "index_table_agent::drop_index_local[{}] name={}",
              static_cast<unsigned>(table_oid_),
              index_name);

        if (engine_ptr_ == nullptr) {
            // See create_index_local: un-migrated oid falls back to manager_index_t.
            trace(log_,
                  "index_table_agent::drop_index_local[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }

        auto* index = components::index::search_index(engine_owned_, index_name);
        if (!index) {
            co_return;
        }

        // Drop disk agent if exists. We send the drop and await — the disk
        // actor needs to land its delete-segment / btree-purge before we erase
        // it from disk_agents_. actor_zeta::send handles in-actor scheduling.
        if (index->is_disk()) {
            auto agent_addr = index->disk_agent();
            auto [needs_sched, future] = actor_zeta::send(agent_addr, &index_agent_disk_t::drop, session);
            schedule_disk_agent(agent_addr, needs_sched);
            co_await std::move(future);

            disk_agents_.erase(std::remove_if(disk_agents_.begin(),
                                              disk_agents_.end(),
                                              [&agent_addr](const auto& a) { return a->address() == agent_addr; }),
                               disk_agents_.end());
        }

        components::index::drop_index(engine_owned_, index);

        trace(log_,
              "index_table_agent::drop_index_local[{}] name={} done",
              static_cast<unsigned>(table_oid_),
              index_name);
        co_return;
    }

    index_table_agent_t::unique_future<void>
    index_table_agent_t::apply_wal_record_for_index_local(session_id_t /*session*/,
                                                          components::catalog::oid_t /*table_oid*/,
                                                          components::catalog::oid_t index_oid,
                                                          uint64_t wal_record_id,
                                                          uint8_t record_type,
                                                          std::pmr::vector<int64_t> row_ids,
                                                          std::unique_ptr<components::vector::data_chunk_t> physical_data,
                                                          uint64_t physical_row_start,
                                                          uint64_t txn_id,
                                                          core::date::timezone_offset_t session_tz) {
        if (engine_ptr_ == nullptr) {
            trace(log_,
                  "index_table_agent::apply_wal_record_for_index_local[{}] engine not bound; no-op",
                  static_cast<unsigned>(table_oid_));
            co_return;
        }

        if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT)) {
            // V1.d real-impl: replay the INSERT chunk into the build's engine.
            // Mirrors manager_index_t::apply_wal_record_for_index INSERT branch.
            if (!physical_data || physical_data->size() == 0) {
                trace(log_,
                      "index_table_agent::apply_wal_record_for_index_local INSERT: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid_),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
                co_return;
            }
            const auto rows = physical_data->size();
            for (uint64_t i = 0; i < rows; ++i) {
                engine_ptr_->insert_row(*physical_data,
                                        static_cast<size_t>(i),
                                        static_cast<int64_t>(physical_row_start + i),
                                        txn_id,
                                        session_tz);
            }
            trace(log_,
                  "index_table_agent::apply_wal_record_for_index_local INSERT: "
                  "table_oid={} index_oid={} wal_id={} rows={}",
                  static_cast<unsigned>(table_oid_),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  rows);
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE)) {
            // Mirrors manager_index_t::apply_wal_record_for_index DELETE branch.
            // The operator pre-fetches the OLD key chunk via storage_fetch and
            // hands it in via physical_data; we loop mark_delete_row across the
            // recovered prefix.
            if (!physical_data || physical_data->size() == 0) {
                trace(log_,
                      "index_table_agent::apply_wal_record_for_index_local DELETE: no recovered chunk "
                      "(table_oid={} index_oid={} wal_id={} row_ids={}), skipping",
                      static_cast<unsigned>(table_oid_),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      row_ids.size());
            } else {
                const auto rows = std::min<uint64_t>(physical_data->size(), row_ids.size());
                for (uint64_t i = 0; i < rows; ++i) {
                    engine_ptr_->mark_delete_row(*physical_data,
                                                 static_cast<size_t>(i),
                                                 row_ids[static_cast<size_t>(i)],
                                                 txn_id,
                                                 session_tz);
                }
                trace(log_,
                      "index_table_agent::apply_wal_record_for_index_local DELETE: "
                      "table_oid={} index_oid={} wal_id={} rows={} (row_ids={} chunk={})",
                      static_cast<unsigned>(table_oid_),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      rows,
                      row_ids.size(),
                      physical_data->size());
            }
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_UPDATE)) {
            // Mirrors manager_index_t::apply_wal_record_for_index UPDATE branch
            // — NEW-insert half only; the OLD-delete half arrives as a separate
            // PHYSICAL_DELETE message issued by the operator.
            if (physical_data && physical_data->size() > 0) {
                const auto rows = physical_data->size();
                for (uint64_t i = 0; i < rows; ++i) {
                    engine_ptr_->insert_row(*physical_data,
                                            static_cast<size_t>(i),
                                            static_cast<int64_t>(physical_row_start + i),
                                            txn_id,
                                            session_tz);
                }
                trace(log_,
                      "index_table_agent::apply_wal_record_for_index_local UPDATE (insert new half): "
                      "table_oid={} index_oid={} wal_id={} new_rows={} old_row_ids={}",
                      static_cast<unsigned>(table_oid_),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      rows,
                      row_ids.size());
            } else {
                trace(log_,
                      "index_table_agent::apply_wal_record_for_index_local UPDATE: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid_),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
            }
        } else {
            trace(log_,
                  "index_table_agent::apply_wal_record_for_index_local: ignoring "
                  "record_type={} (table_oid={} wal_id={})",
                  static_cast<unsigned>(record_type),
                  static_cast<unsigned>(table_oid_),
                  wal_record_id);
        }
        co_return;
    }

} // namespace services::index

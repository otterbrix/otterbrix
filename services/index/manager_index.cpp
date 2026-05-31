#include "manager_index.hpp"

#include "bitcask_index_disk.hpp"
#include "btree_index_disk.hpp"
#include "disk_hash_table.hpp"

#include <actor-zeta/spawn.hpp>
#include <components/index/hash_single_field_index.hpp>
#include <components/index/index_engine.hpp>
#include <components/index/disk_hash_single_field_index.hpp>
#include <components/index/single_field_index.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <core/executor.hpp>
#include <msgpack.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/record.hpp>
#include <unordered_map>

namespace {
    using namespace core::b_plus_tree;

    auto item_key_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/0");
    };

    auto id_getter = [](const btree_t::item_data& item) -> btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return get_field(msg.get(), "/1");
    };

    using value_t = components::types::logical_value_t;
    using namespace components::types;

    value_t reverse_convert(std::pmr::memory_resource* r, const physical_value& pv) {
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
} // anonymous namespace`

namespace {
    // Batched disk operation types — collect per-agent ops, send once
    using disk_batch_t = std::vector<std::pair<value_t, size_t>>;
    using agent_batch_map_t = std::unordered_map<uintptr_t, disk_batch_t>;
    using agent_addr_map_t = std::unordered_map<uintptr_t, actor_zeta::address_t>;

    [[maybe_unused]] void collect_disk_op(const components::index::index_engine_ptr& engine,
                                          const components::vector::data_chunk_t& chunk,
                                          size_t row,
                                          std::pmr::memory_resource* target_resource,
                                          agent_batch_map_t& batches,
                                          agent_addr_map_t& addrs) {
        engine->for_each_disk_op(chunk,
                                 row,
                                 [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key) {
                                     auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                                     addrs.try_emplace(id, agent_addr);
                                     batches[id].emplace_back(value_t(target_resource, key), row);
                                 });
    }
} // anonymous namespace

namespace services::index {
    manager_index_t::manager_index_t(std::pmr::memory_resource* resource,
                                     actor_zeta::scheduler_raw scheduler,
                                     log_t& log,
                                     std::filesystem::path path_db,
                                     uint64_t bitcask_flush_threshold,
                                     uint64_t bitcask_segment_record_limit,
                                     uint64_t btree_flush_threshold,
                                     run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_index_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , run_fn_(std::move(run_fn))
        , log_(log)
        , path_db_(std::move(path_db))
        , bitcask_flush_threshold_(bitcask_flush_threshold)
        , bitcask_segment_record_limit_(bitcask_segment_record_limit)
        , btree_flush_threshold_(btree_flush_threshold)
        , engines_(resource)
        , per_table_agents_(resource)
        , dropped_table_agents_(resource)
        , pending_void_(resource) {
        if (!path_db_.empty()) {
            std::filesystem::create_directories(path_db_);
        }
    }

    auto manager_index_t::make_type() const noexcept -> const char* { return "manager_index"; }

    actor_zeta::behavior_t manager_index_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::register_collection>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::register_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::unregister_collection>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::unregister_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::create_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::drop_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::has_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::has_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::insert_rows>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::insert_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::delete_rows>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::delete_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::update_rows>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::update_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::commit_insert>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::commit_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::commit_delete>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::commit_delete, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::revert_insert>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::revert_insert, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::cleanup_all_versions>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::cleanup_all_versions, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::rebuild_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::rebuild_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::search>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::search, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::search_with_preferred_type>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::search_with_preferred_type, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::flush_all_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::flush_all_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_keys>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_keys, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_descriptions>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_descriptions, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::on_horizon_advanced>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::on_horizon_advanced, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::mark_table_dropped>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::mark_table_dropped, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::apply_wal_record_for_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::apply_wal_record_for_index, msg);
                break;
            }
            default:
                break;
        }
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_index_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        std::lock_guard<std::mutex> guard(mutex_);
        current_behavior_ = behavior(msg.get());

        while (current_behavior_.is_busy()) {
            if (current_behavior_.is_awaited_ready()) {
                auto cont = current_behavior_.take_awaited_continuation();
                if (cont) {
                    cont.resume();
                }
            } else {
                run_fn_();
            }
        }

        return {false, actor_zeta::detail::enqueue_result::success};
    }

    void manager_index_t::poll_pending() {
        for (auto it = pending_void_.begin(); it != pending_void_.end();) {
            if (it->is_ready()) {
                it = pending_void_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void manager_index_t::sync(address_pack pack) {
        disk_address_ = std::get<0>(pack);
        trace(log_, "manager_index_t::sync: disk_address set");
    }

    void manager_index_t::register_table_agent_sync(components::catalog::oid_t oid, actor_zeta::address_t addr) {
        // Bootstrap-only path: invoked from base_spaces Phase 3.5 before the
        // scheduler is started. No locking — single-threaded by construction.
        per_table_agents_.emplace(oid, std::move(addr));
    }

    components::index::index_engine_t*
    manager_index_t::engine_for_oid_sync(components::catalog::oid_t oid) const noexcept {
        // Bootstrap-only path: invoked from base_spaces Phase 3.5 alongside
        // register_table_agent_sync. engines_ stores
        // core::pmr::unique_ptr<index_engine_t>, so the raw pointer we return
        // here is the heap address of the engine object — stable across any
        // unordered_map rehash. Returns nullptr when no engine exists for the
        // oid yet (engines_ is populated lazily by register_collection).
        auto it = engines_.find(oid);
        if (it == engines_.end()) {
            return nullptr;
        }
        return it->second.get();
    }

    components::index::index_engine_ptr
    manager_index_t::take_engine_ownership_sync(components::catalog::oid_t oid) {
        // Bootstrap-only path: invoked from base_spaces Phase 3.5 after
        // register_table_agent_sync. If engines_ already has an entry for the
        // oid (system-table bootstrap or earlier register_collection), pop it
        // out so the owning index_table_agent_t becomes the exclusive holder.
        // Otherwise mint a fresh engine here so the agent always receives a
        // non-null unique_ptr — mirrors register_collection's lazy-init shape
        // so the migrated and unmigrated paths converge.
        auto it = engines_.find(oid);
        if (it != engines_.end()) {
            auto engine = std::move(it->second);
            engines_.erase(it);
            return engine; // NRVO; explicit std::move not needed for local of same type
        }
        return components::index::make_index_engine(resource_);
    }

    void manager_index_t::mark_table_dropped_sync(components::catalog::oid_t oid, uint64_t dropped_at_commit_id) {
        // Bootstrap helper — dec 37 catalog scan rebuild populates this. Also
        // called internally by the mark_table_dropped mailbox handler. NOT a
        // mailbox handler — single-threaded callers only (bootstrap path).
        dropped_table_agents_[oid] = dropped_at_commit_id;
    }

    manager_index_t::unique_future<void>
    manager_index_t::mark_table_dropped(session_id_t /*session*/,
                                        components::catalog::oid_t table_oid,
                                        uint64_t dropped_at_commit_id) {
        // Runtime DROP TABLE path — operator_dynamic_cascade_delete sends this
        // from inside the executor actor. Thin coroutine wrapper around
        // mark_table_dropped_sync so the operator can co_await a real future
        // and the dropped_table_agents_ mutation stays on the manager_index_t
        // actor's mailbox (rule 11 — no sync inter-actor mutation).
        trace(log_,
              "manager_index_t::mark_table_dropped , oid : {} , commit_id : {}",
              static_cast<unsigned>(table_oid),
              dropped_at_commit_id);
        mark_table_dropped_sync(table_oid, dropped_at_commit_id);
        co_return;
    }

    void manager_index_t::set_manager_dispatcher_sync(actor_zeta::address_t address) {
        // Bootstrap-only path: base_spaces wires this before scheduler.start.
        // Single-threaded by construction — no locking required.
        manager_dispatcher_ = std::move(address);
    }

    void manager_index_t::schedule_agent(const actor_zeta::address_t& addr, bool needs_sched) {
        if (!needs_sched)
            return;
        for (auto& agent : disk_agents_) {
            if (agent->address() == addr) {
                scheduler_->enqueue(agent.get());
                return;
            }
        }
    }

    // --- Collection lifecycle ---

    manager_index_t::unique_future<void> manager_index_t::register_collection(session_id_t /*session*/,
                                                                              components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::register_collection: oid={}", static_cast<unsigned>(table_oid));

        // Final cleanup — engine ownership migration. If a per-table
        // agent is already registered for this oid (base_spaces Phase 3.5 ran
        // and handed engine ownership to the agent via take_engine_ownership_sync
        // -> set_engine_owned_sync), engines_ has no entry by design. Skip the
        // lazy-init here: re-creating an empty engine on engines_ would diverge
        // from the agent's owned engine and any DDL/legacy path still pointed
        // at engines_ would silently see an empty index set.
        if (per_table_agents_.find(table_oid) != per_table_agents_.end()) {
            co_return;
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            engines_.emplace(table_oid, components::index::make_index_engine(resource_));
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(session_id_t /*session*/,
                                                                                components::catalog::oid_t table_oid) {
        trace(log_, "manager_index_t::unregister_collection: oid={}", static_cast<unsigned>(table_oid));

        engines_.erase(table_oid);
        co_return;
    }

    // --- DDL: index management ---

    manager_index_t::unique_future<uint32_t> manager_index_t::create_index(session_id_t session,
                                                                           components::catalog::oid_t table_oid,
                                                                           index_name_t index_name,
                                                                           components::index::keys_base_storage_t keys,
                                                                           components::logical_plan::index_type type,
                                                                           core::date::timezone_offset_t session_tz) {
        trace(log_, "manager_index_t::create_index: {} on oid={}", index_name, static_cast<unsigned>(table_oid));

        // DDL routing — closes the regression introduced by the engine
        // ownership migration. After base_spaces Phase 3.5 hands ownership of
        // the per-table index_engine_t to the agent via set_engine_owned_sync,
        // engines_[oid] is empty for every bootstrap-bound oid. The legacy
        // engines_-based body below would therefore early-return with
        // INDEX_ID_UNDEFINED and CREATE INDEX would fail silently for all
        // system + user tables known at startup.
        //
        // Routing path: if a per-table agent is registered, forward via
        // create_index_local — the agent runs the engine_ptr_-bound body and
        // owns disk_agents_ for the new physical index. Otherwise (un-migrated
        // runtime CREATE TABLE path) fall through to the legacy engines_ body
        // below, which is still authoritative for that case until the runtime
        // spawn step (Step N+1) lands.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::create_index_local,
                                             session,
                                             table_oid,
                                             index_name,
                                             std::move(keys),
                                             type,
                                             session_tz);
            co_return co_await std::move(fut);
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return components::index::INDEX_ID_UNDEFINED;
        }

        auto& engine = it->second;

        if (engine->has_index(index_name)) {
            co_return components::index::INDEX_ID_UNDEFINED;
        }

        uint32_t id_index = components::index::INDEX_ID_UNDEFINED;
        switch (type) {
            case components::logical_plan::index_type::single: {
                id_index =
                    components::index::make_index<components::index::single_field_index_t>(engine, index_name, keys);
                break;
            }
            case components::logical_plan::index_type::hashed: {
                if (path_db_.empty()) {
                    id_index =
                        components::index::make_index<components::index::hash_single_field_index_t>(engine,
                                                                                                      index_name,
                                                                                                      keys);
                } else {
                    const auto base = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) / index_name;
                    std::filesystem::create_directories(base);
                    try {
                        id_index = components::index::make_index<components::index::disk_hash_single_field_index_t>(
                            engine,
                            index_name,
                            keys,
                            std::make_unique<services::index::disk_hash_table_t>(base / "hash_index.bin",
                                                                                 services::index::disk_hash_table_t::default_bucket_count,
                                                                                 true,
                                                                                 resource_));
                    } catch (const std::exception& e) {
                        trace(log_,
                              "manager_index_t::create_index: disk hash storage init failed, fallback to memory: {}",
                              e.what());
                        id_index = components::index::make_index<components::index::hash_single_field_index_t>(
                            engine,
                            index_name,
                            keys);
                    }
                }
                break;
            }
            default:
                trace(log_, "manager_index_t::create_index: unsupported index type");
                co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Load index data from btree (persistent storage). Path layout
            // mirrors disk-side ${path_db}/${table_oid}/${index_name}/.
            if (!path_db_.empty() && type == components::logical_plan::index_type::single) {
                auto btree_path = path_db_ / std::to_string(static_cast<unsigned>(table_oid)) / index_name;
                if (std::filesystem::exists(btree_path / "metadata")) {
                    try {
                        core::filesystem::local_file_system_t fs;
                        auto db =
                            std::make_unique<core::b_plus_tree::btree_t>(resource_, fs, btree_path, item_key_getter);
                        db->load();

                        if (db->size() > 0) {
                            struct pv_entry {
                                components::types::physical_value key;
                                int64_t row_id;
                            };
                            std::pmr::vector<pv_entry> raw(resource_);
                            db->full_scan<pv_entry>(&raw, [](void* data, size_t sz) -> pv_entry {
                                auto item = core::b_plus_tree::btree_t::item_data{
                                    static_cast<core::b_plus_tree::data_ptr_t>(data),
                                    static_cast<uint32_t>(sz)};
                                return {item_key_getter(item),
                                        static_cast<int64_t>(
                                            id_getter(item).value<components::types::physical_type::UINT64>())};
                            });

                            auto* idx = components::index::search_index(engine, keys);
                            if (idx) {
                                for (auto& e : raw) {
                                    idx->insert(reverse_convert(resource_, e.key), e.row_id, session_tz);
                                }
                                trace(log_, "create_index: loaded {} entries from btree", raw.size());
                            }
                        }
                    } catch (const std::exception& e) {
                        trace(log_, "create_index: btree load failed: {}", e.what());
                    }
                }
            }

            // Create disk agent for persistent storage
            if (!path_db_.empty()) {
                try {
                    auto agent =
                        actor_zeta::spawn<index_agent_disk_t>(resource_,
                                                              path_db_,
                                                              table_oid,
                                                              std::string(index_name),
                                                              type,
                                                              bitcask_index_disk_t::default_flush_threshold_,
                                                              bitcask_index_disk_t::default_segment_record_limit_,
                                                              btree_index_disk_t::default_flush_threshold_,
                                                              log_);

                    // Link disk agent with in-memory index
                    auto* idx = components::index::search_index(engine, keys);
                    if (idx) {
                        idx->set_disk_agent(agent->address(), address());
                        engine->add_disk_agent(id_index, agent->address());
                    }

                    disk_agents_.emplace_back(std::move(agent));
                } catch (const std::exception& e) {
                    trace(log_, "manager_index_t::create_index: disk agent creation failed: {}", e.what());
                }
            }
        }

        // Routing observability post-DDL. Trace whether the
        // per-table agent already exists (path (a) above) or whether we just
        // created an index for an oid not yet known to per_table_agents_
        // (path (b) — router fallback covers DML correctness for now).
        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            const bool has_agent = per_table_agents_.find(table_oid) != per_table_agents_.end();
            trace(log_,
                  "manager_index_t::create_index: oid={} per_table_agent={} (id_index={})",
                  static_cast<unsigned>(table_oid),
                  has_agent ? "bound" : "fallback-via-engines_",
                  id_index);
        }

        co_return id_index;
    }

    manager_index_t::unique_future<void>
    manager_index_t::drop_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name) {
        trace(log_, "manager_index_t::drop_index: {} on oid={}", index_name, static_cast<unsigned>(table_oid));

        // DDL routing — same regression fix as create_index. After
        // engine ownership migration, engines_[oid] is empty for bootstrap-
        // bound oids, so the legacy body below would no-op silently. Forward
        // to the agent's drop_index_local handler when registered; otherwise
        // fall through to the legacy engines_ body for runtime CREATE TABLE
        // oids that haven't migrated yet.
        //
        // DROP INDEX is NOT a GC trigger: per_table_agents_[oid] is left
        // untouched (the agent still owns sibling indexes + the engine itself
        // until DROP TABLE / unregister_collection drives the dec 33 sweep).
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::drop_index_local,
                                             session,
                                             table_oid,
                                             index_name);
            co_await std::move(fut);
            trace(log_,
                  "manager_index_t::drop_index: oid={} routed to per_table_agent",
                  static_cast<unsigned>(table_oid));
            co_return;
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        auto* index = components::index::search_index(engine, index_name);

        if (index) {
            // Drop disk agent if exists
            if (index->is_disk()) {
                auto agent_addr = index->disk_agent();
                auto [needs_sched, future] =
                    actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::drop, session);
                schedule_agent(agent_addr, needs_sched);

                // Wait for drop to complete before destroying the agent
                co_await std::move(future);

                // Remove agent from our list
                disk_agents_.erase(std::remove_if(disk_agents_.begin(),
                                                  disk_agents_.end(),
                                                  [&agent_addr](const auto& a) { return a->address() == agent_addr; }),
                                   disk_agents_.end());
            }

            components::index::drop_index(engine, index);
        }

        // DDL routing observability. Trace the per-table
        // agent's continued binding state: DROP INDEX must NOT clear it.
        const bool has_agent = per_table_agents_.find(table_oid) != per_table_agents_.end();
        trace(log_,
              "manager_index_t::drop_index: oid={} per_table_agent={} (preserved across DROP INDEX)",
              static_cast<unsigned>(table_oid),
              has_agent ? "bound" : "fallback-via-engines_");

        co_return;
    }

    // --- Query ---

    manager_index_t::unique_future<bool> manager_index_t::has_index(session_id_t session,
                                                                    components::catalog::oid_t table_oid,
                                                                    index_name_t index_name) {
        // + dec 46 — per-table router. Forward via mailbox
        // to the owning index_table_agent_t when one is registered. The agent's
        // body is a stub today (engine_ptr_ stays null in steady state) so the
        // truthful answer still comes from the engines_ fallback below; once
        // engine ownership migrates, the agent will own the answer and the
        // fallback will be removed.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] =
                actor_zeta::send(agent_it->second, &index_table_agent_t::has_index, session, table_oid, index_name);
            auto agent_result = co_await std::move(fut);
            if (agent_result) {
                co_return true;
            }
            // engine not bound on agent yet — fall through to engines_ body.
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return false;

        co_return it->second->has_index(index_name);
    }

    // --- Txn-aware DML ---

    manager_index_t::unique_future<void>
    manager_index_t::insert_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                 uint64_t start_row_id,
                                 uint64_t count) {
        if (!data || count == 0)
            co_return;

        // DML migration — feature-flag router. If a per-table agent is
        // registered for this oid (via register_table_agent_sync during V4
        // bootstrap), forward the message and await its future. Today the agent
        // body is a no-op stub; the actual engine_ work still happens below
        // when the map has no entry, so existing call sites keep working until
        // the engine ownership move ships.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::insert_rows,
                                             ctx,
                                             table_oid,
                                             std::move(data),
                                             start_row_id,
                                             count);
            co_await std::move(fut);
            co_return;
        }

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (uint64_t i = 0; i < count; i++) {
            engine->insert_row(*data, i, static_cast<int64_t>(start_row_id + i), txn_id, ctx.session_tz);
        }
        // No disk mirroring — uncommitted entries don't go to disk

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::delete_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                 std::pmr::vector<int64_t> row_ids) {
        if (!data || row_ids.empty())
            co_return;

        // DML migration — feature-flag router (see insert_rows).
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::delete_rows,
                                             ctx,
                                             table_oid,
                                             std::move(data),
                                             std::move(row_ids));
            co_await std::move(fut);
            co_return;
        }

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->mark_delete_row(*data, i, row_ids[i], txn_id, ctx.session_tz);
        }
        // No disk mirroring — uncommitted deletes don't go to disk

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::update_rows(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::unique_ptr<components::vector::data_chunk_t> old_data,
                                 std::unique_ptr<components::vector::data_chunk_t> new_data,
                                 std::pmr::vector<int64_t> row_ids,
                                 int64_t new_start_row_id) {
        if (!old_data || !new_data || row_ids.empty())
            co_return;

        // DML migration — feature-flag router (see insert_rows).
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::update_rows,
                                             ctx,
                                             table_oid,
                                             std::move(old_data),
                                             std::move(new_data),
                                             std::move(row_ids),
                                             new_start_row_id);
            co_await std::move(fut);
            co_return;
        }

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Mark old entries as deleted
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->mark_delete_row(*old_data, i, row_ids[i], txn_id, ctx.session_tz);
        }

        // Insert new entries
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->insert_row(*new_data, i, new_start_row_id + static_cast<int64_t>(i), txn_id, ctx.session_tz);
        }

        co_return;
    }

    // --- MVCC commit/revert/cleanup ---

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id) {
        // DML migration — feature-flag router (see insert_rows). The
        // agent's core::error_t is forwarded back so the executor commit-path
        // keeps its typed-error contract.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::commit_insert,
                                             ctx,
                                             table_oid,
                                             commit_id);
            auto result = co_await std::move(fut);
            co_return result;
        }

        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;

        // Parallel A.B3: two-phase fan-out. Phase 1 — gather pending
        // disk inserts from BOTH the txn-local pending map and the global
        // pending map (txn_id == 0 path), batch per disk-agent, then send all
        // insert_many messages without intervening co_await. Phase 2 — collect
        // every future in one pass. Decoupling send from await means N disk
        // agents progress in parallel instead of strictly serially.
        agent_batch_map_t insert_batches;
        agent_addr_map_t insert_addrs;
        engine->for_each_pending_disk_insert(
            txn_id,
            [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                insert_addrs.try_emplace(id, agent_addr);
                insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
            });
        if (txn_id != 0) {
            engine->for_each_pending_disk_insert(
                0,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    insert_addrs.try_emplace(id, agent_addr);
                    insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });
        }

        // Phase 1: send all insert_many messages, collect futures.
        std::pmr::vector<unique_future<void>> futures(resource_);
        futures.reserve(insert_batches.size());
        for (auto& [id, batch] : insert_batches) {
            auto& addr = insert_addrs.at(id);
            auto [ns, f] =
                actor_zeta::otterbrix::send(addr, &index_agent_disk_t::insert_many, session, txn_id, std::move(batch));
            schedule_agent(addr, ns);
            futures.emplace_back(std::move(f));
        }

        // Phase 2: collect every future. Underlying disk methods are
        // assert+abort terminal today (Block A.B1), so reaching the end of
        // this loop implies success for all agents.
        for (auto& f : futures) {
            co_await std::move(f);
        }

        // In-memory flip is unconditional: if any disk agent had failed, the
        // process would already have been aborted by the assert in bitcask.
        engine->commit_insert(txn_id, commit_id);
        if (txn_id != 0) {
            engine->commit_insert(0, commit_id);
        }

        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<core::error_t>
    manager_index_t::commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id) {
        // DML migration — feature-flag router (see insert_rows).
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::commit_delete,
                                             ctx,
                                             table_oid,
                                             commit_id);
            auto result = co_await std::move(fut);
            co_return result;
        }

        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return core::error_t::no_error();

        auto& engine = it->second;

        // Parallel A.B3: two-phase fan-out (mirror of commit_insert).
        // Phase 1 batches pending disk deletes from both the txn-local pending
        // map and the global (txn_id == 0) map, then sends every remove_many
        // message before any co_await. Phase 2 awaits the collected futures.
        agent_batch_map_t remove_batches;
        agent_addr_map_t remove_addrs;
        engine->for_each_pending_disk_delete(
            txn_id,
            [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                remove_addrs.try_emplace(id, agent_addr);
                remove_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
            });
        if (txn_id != 0) {
            engine->for_each_pending_disk_delete(
                0,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                    auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                    remove_addrs.try_emplace(id, agent_addr);
                    remove_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
                });
        }

        // Phase 1: send all remove_many messages, collect futures.
        std::pmr::vector<unique_future<void>> futures(resource_);
        futures.reserve(remove_batches.size());
        for (auto& [id, batch] : remove_batches) {
            auto& addr = remove_addrs.at(id);
            auto [ns, f] =
                actor_zeta::otterbrix::send(addr, &index_agent_disk_t::remove_many, session, txn_id, std::move(batch));
            schedule_agent(addr, ns);
            futures.emplace_back(std::move(f));
        }

        // Phase 2: collect every future. Same assert+abort terminal contract
        // as commit_insert applies — reaching this point means all succeeded.
        for (auto& f : futures) {
            co_await std::move(f);
        }

        engine->commit_delete(txn_id, commit_id);
        if (txn_id != 0) {
            engine->commit_delete(0, commit_id);
        }

        co_return core::error_t::no_error();
    }

    manager_index_t::unique_future<void> manager_index_t::revert_insert(execution_context_t ctx,
                                                                        components::catalog::oid_t table_oid) {
        // DML migration — feature-flag router (see insert_rows).
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::revert_insert,
                                             ctx,
                                             table_oid);
            co_await std::move(fut);
            co_return;
        }

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        it->second->revert_insert(txn_id);
        // No disk action — uncommitted entries never went to disk

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::cleanup_all_versions(session_id_t session,
                                                                               uint64_t lowest_active) {
        // (Pass 9 Variant 2) — broadcast wrapper. For every
        // registered per-table agent send cleanup_versions(ctx, oid,
        // lowest_active) and collect the futures, then co_await them all in
        // a second pass so the fan-out runs in parallel (mailbox-only per
        // rule 11, no shared state per rule 10). The agent bodies are no-op
        // stubs today (engine_ptr_ stays null in steady state), so the
        // truthful cleanup still happens via the engines_ loop below —
        // mirrors the "router + engines_ fallback" pattern used by
        // search/has_index. Once engine ownership migrates onto the
        // per-table agents the manager-side fallback can be removed.
        if (!per_table_agents_.empty()) {
            std::pmr::vector<unique_future<void>> futures(resource_);
            futures.reserve(per_table_agents_.size());
            for (auto& [oid, addr] : per_table_agents_) {
                execution_context_t ctx{};
                ctx.session = session;
                ctx.table_oid = oid;
                auto [_, fut] =
                    actor_zeta::send(addr, &index_table_agent_t::cleanup_versions, ctx, oid, lowest_active);
                futures.emplace_back(std::move(fut));
            }
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        for (auto& [oid, engine] : engines_) {
            engine->cleanup_versions(lowest_active);
        }

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::rebuild_indexes(session_id_t session,
                                                                          components::catalog::oid_t table_oid) {
        // (Pass 9 Variant 2) — per-oid router. Same shape as
        // insert_rows: when a per-table agent is registered for this oid,
        // forward via mailbox and await its future. Agent body is a no-op
        // stub today (engine_ptr_ null in steady state); the truthful
        // rebuild still happens in the engines_ body below via the existing
        // fallback path until engine ownership migrates.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] =
                actor_zeta::send(agent_it->second, &index_table_agent_t::rebuild_indexes, session, table_oid);
            co_await std::move(fut);
            // Fall through to engines_ body — agent stub is no-op while
            // engine_ptr_ is null.
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Clear all indexes in this engine
        for (auto& idx_name : engine->indexes()) {
            auto* idx = components::index::search_index(engine, idx_name);
            if (idx) {
                idx->clean_memory_to_new_elements(0);
            }
        }

        // Rebuild will be triggered by executor sending scan data to
        // manager_index for index rebuild.
        trace(log_, "manager_index_t::rebuild_indexes: cleared indexes for oid={}", static_cast<unsigned>(table_oid));

        co_return;
    }

    // --- Txn-aware Query ---

    manager_index_t::unique_future<std::pmr::vector<int64_t>>
    manager_index_t::search_with_preferred_type(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::index::keys_base_storage_t keys,
                                                components::types::logical_value_t value,
                                                components::expressions::compare_type compare,
                                                components::logical_plan::index_type preferred_type,
                                                uint64_t start_time,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t session_tz) {
        // + dec 46 — search hot-path router. Forward via
        // mailbox to the owning index_table_agent_t when one is registered.
        // The agent stub returns an empty vector while engine_ptr_ stays null
        // (the engine ownership migration hasn't run yet), so when the agent
        // future is empty AND the agent isn't authoritative yet, fall back to
        // the existing engines_ body. Once engine ownership migrates, only the
        // agent will run and the fallback can be deleted.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::search_with_preferred_type,
                                             session,
                                             table_oid,
                                             keys,
                                             value,
                                             compare,
                                             preferred_type,
                                             start_time,
                                             txn_id,
                                             session_tz);
            auto agent_result = co_await std::move(fut);
            if (!agent_result.empty()) {
                co_return agent_result;
            }
            // empty agent answer — engine not bound yet; fall through.
        }

        std::pmr::vector<int64_t> result(resource_);
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return result;
        }

        auto* index = it->second->matching(keys, preferred_type);
        if (!index) {
            index = components::index::search_index(it->second, keys);
        }
        if (!index) {
            co_return result;
        }
        co_return index->search(compare, value, start_time, txn_id, session_tz);
    }

    manager_index_t::unique_future<std::pmr::vector<int64_t>>
    manager_index_t::search(session_id_t session,
                            components::catalog::oid_t table_oid,
                            components::index::keys_base_storage_t keys,
                            components::types::logical_value_t value,
                            components::expressions::compare_type compare,
                            uint64_t start_time,
                            uint64_t txn_id,
                            core::date::timezone_offset_t session_tz) {
        // + dec 46 — search hot-path router. See the
        // longer comment in search_with_preferred_type above. Mailbox-only
        // delegation per constraint #11; manager engines_ body remains the
        // truthful source until engine ownership migrates onto the per-table
        // agent.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::search,
                                             session,
                                             table_oid,
                                             keys,
                                             value,
                                             compare,
                                             start_time,
                                             txn_id,
                                             session_tz);
            auto agent_result = co_await std::move(fut);
            if (!agent_result.empty()) {
                co_return agent_result;
            }
            // empty agent answer — engine not bound yet; fall through.
        }

        std::pmr::vector<int64_t> result(resource_);

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return result;

        auto* index = components::index::search_index(it->second, keys);
        if (!index)
            co_return result;

        co_return index->search(compare, value, start_time, txn_id, session_tz);
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    manager_index_t::get_indexed_keys(session_id_t session, components::catalog::oid_t table_oid) {
        // metadata-read router. Forward via mailbox to the
        // owning index_table_agent_t when one is registered. The agent stub
        // returns an empty vector while engine_ptr_ stays null (engine
        // ownership migration hasn't run), so an empty agent answer is treated
        // as "agent not authoritative yet" — fall through to the existing
        // engines_ body. Once engine ownership migrates, the agent will own the
        // answer and the fallback can be deleted.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::get_indexed_keys,
                                             session,
                                             table_oid);
            auto agent_result = co_await std::move(fut);
            if (!agent_result.empty()) {
                co_return agent_result;
            }
            // empty agent answer — engine not bound yet; fall through.
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return it->second->all_indexed_keys();
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::index_description_t>>
    manager_index_t::get_indexed_descriptions(session_id_t session, components::catalog::oid_t table_oid) {
        // metadata-read router. See get_indexed_keys above
        // for the empty-result fallback contract.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::get_indexed_descriptions,
                                             session,
                                             table_oid);
            auto agent_result = co_await std::move(fut);
            if (!agent_result.empty()) {
                co_return agent_result;
            }
            // empty agent answer — engine not bound yet; fall through.
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::index_description_t>(resource_);
        }
        co_return it->second->all_indexed_descriptions();
    }

    manager_index_t::unique_future<void> manager_index_t::flush_all_indexes(session_id_t session) {
        trace(log_, "manager_index_t::flush_all_indexes, session: {}", session.data());

        // (Pass 9 Variant 2) — broadcast wrapper. Fan a
        // flush_indexes message out to every registered per-table agent,
        // collect futures, then co_await them in a second pass so the
        // fan-out runs in parallel (mailbox-only per rule 11, no shared
        // state per rule 10). Agent bodies are no-op stubs today
        // (engine_ptr_ null in steady state); the truthful flush still runs
        // via the disk_agents_ loop below — the manager-side path remains
        // authoritative until engine + disk-agent ownership migrate.
        if (!per_table_agents_.empty()) {
            std::pmr::vector<unique_future<void>> futures(resource_);
            futures.reserve(per_table_agents_.size());
            for (auto& [oid, addr] : per_table_agents_) {
                auto [_, fut] = actor_zeta::send(addr, &index_table_agent_t::flush_indexes, session, oid);
                futures.emplace_back(std::move(fut));
            }
            for (auto& f : futures) {
                co_await std::move(f);
            }
        }

        // Await all pending agent operations to ensure no in-flight writes
        for (auto& f : pending_void_) {
            co_await std::move(f);
        }
        pending_void_.clear();
        // Now safe to call synchronously — no actor messaging, avoids TSan race
        for (auto& agent : disk_agents_) {
            if (agent && !agent->is_dropped()) {
                agent->force_flush_sync();
            }
        }
        co_return;
    }

    // event-driven GC subscriber. Walks dropped_table_agents_ and
    // erases the routing entry for any table whose dropped_at_commit_id is now
    // strictly below the snapshot floor (new_horizon). Address-only entry
    // erase: the owning unique_ptr<index_table_agent_t> lives in base_spaces
    // per the Block D V4 bootstrap design, so removing the address from
    // per_table_agents_ here only drops the routing entry — the actor itself
    // is reaped at base_spaces shutdown (or via a future base_spaces.cpp
    // cleanup once the address is no longer referenced). No double-free is
    // possible from this path.
    //
    // event-driven GC subscriber. On drain (queue empty) sends
    // on_subscriber_empty(INDEX_KIND) ack to dispatcher so the
    // selective-broadcast flag clears and no further on_horizon_advanced
    // broadcasts arrive until a new DROP TABLE re-marks the subscriber.
    manager_index_t::unique_future<void> manager_index_t::on_horizon_advanced(uint64_t new_horizon) {
        trace(log_, "manager_index_t::on_horizon_advanced , horizon : {}", new_horizon);
        // symmetric GC pass: erase dropped per-table agents whose
        // dropped_at_commit_id is now below the snapshot floor.
        for (auto it = dropped_table_agents_.begin(); it != dropped_table_agents_.end();) {
            if (it->second < new_horizon) {
                // Drop the address-only routing entry from per_table_agents_.
                // The owning unique_ptr<index_table_agent_t> lives in
                // base_spaces (per Block D V4 bootstrap design), so erasing
                // here only removes routing — actor destruction happens at
                // base_spaces shutdown OR via base_spaces.cpp cleanup when
                // the address is no longer referenced (future work).
                per_table_agents_.erase(it->first);
                it = dropped_table_agents_.erase(it);
            } else {
                ++it;
            }
        }
        if (dropped_table_agents_.empty() && manager_dispatcher_ != actor_zeta::address_t::empty_address()) {
            // ack flag flip — dispatcher clears index_has_dropped_ on
            // receipt so no further on_horizon_advanced broadcasts arrive
            // until a new DROP TABLE re-marks the subscriber.
            constexpr uint8_t INDEX_KIND = 2;
            [[maybe_unused]] auto _ = actor_zeta::send(manager_dispatcher_,
                                                      &services::dispatcher::manager_dispatcher_t::on_subscriber_empty,
                                                      INDEX_KIND);
        }
        co_return;
    }

    // d — apply a single WAL record's effect to the
    // build's in-memory index_engine_t during CREATE INDEX Phase 2.5 catchup.
    //
    // The handler mirrors the steady-state DML path (insert_rows / delete_rows
    // / update_rows above) but drives a single record instead of a chunk batch.
    // INSERT (and the NEW-row half of UPDATE) are wired against engine->insert_row
    // using the WAL chunk + physical_row_start; the storage_row assigned to
    // each chunk row is reconstructed as physical_row_start + i so the index
    // keys land on the same row_ids the table-side writer used.
    //
    // DELETE (and the OLD-row half of UPDATE) are wired via a sibling
    // recovery path: the WAL PHYSICAL_DELETE / UPDATE records ship only
    // row_ids, so the operator side performs a storage_fetch(row_ids)
    // BEFORE sending this message and forwards the recovered chunk via
    // physical_data. The handler then runs the same engine->mark_delete_row
    // loop the steady-state DML path uses (no engine API change, no row_id
    // → key reverse map). PHYSICAL_UPDATE replay is split into two messages
    // by the operator: a PHYSICAL_UPDATE one carrying the NEW chunk for the
    // insert half, then a PHYSICAL_DELETE one carrying the recovered OLD
    // chunk for the delete half. See
    // operator_create_index_backfill.cpp for the Phase 2.5 catchup loop
    // that drives this sequence.
    manager_index_t::unique_future<void>
    manager_index_t::apply_wal_record_for_index(session_id_t session,
                                                components::catalog::oid_t table_oid,
                                                components::catalog::oid_t index_oid,
                                                uint64_t wal_record_id,
                                                uint8_t record_type,
                                                std::pmr::vector<int64_t> row_ids,
                                                std::unique_ptr<components::vector::data_chunk_t> physical_data,
                                                uint64_t physical_row_start,
                                                uint64_t txn_id,
                                                core::date::timezone_offset_t session_tz) {
        // DDL routing — closes the regression for CREATE INDEX WAL
        // catchup (operator_create_index_backfill Phase 2.5 loop). After
        // engine ownership migration the manager-side engines_ map is empty
        // for bootstrap-bound oids; the legacy body below would log "no
        // engine for table_oid=..." and skip every replay record. Forward to
        // the agent's local handler when registered; otherwise fall through
        // for un-migrated runtime oids.
        if (auto agent_it = per_table_agents_.find(table_oid); agent_it != per_table_agents_.end()) {
            auto [_, fut] = actor_zeta::send(agent_it->second,
                                             &index_table_agent_t::apply_wal_record_for_index_local,
                                             session,
                                             table_oid,
                                             index_oid,
                                             wal_record_id,
                                             record_type,
                                             std::move(row_ids),
                                             std::move(physical_data),
                                             physical_row_start,
                                             txn_id,
                                             session_tz);
            co_await std::move(fut);
            co_return;
        }

        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            // The build's engine should already have been created by the
            // register_collection / create_index calls earlier in the operator.
            // Missing engine here means a bookkeeping mismatch; log and skip
            // (no exceptions per rule constraint #2).
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: no engine for "
                  "table_oid={} (index_oid={} wal_id={} type={}), skipping",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  static_cast<unsigned>(record_type));
            co_return;
        }

        auto& engine = it->second;

        if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_INSERT)) {
            // V1.d real-impl: replay the INSERT chunk into the build's engine.
            // Entries are tagged with the CREATE INDEX txn_id so they stay
            // PENDING until the post-pipeline commit_insert publishes them
            // alongside the rest of the build (operator_create_index_backfill
            // wires dml_append_row_count for the executor to fire that commit).
            if (!physical_data || physical_data->size() == 0) {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index INSERT: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
                co_return;
            }
            const auto rows = physical_data->size();
            for (uint64_t i = 0; i < rows; ++i) {
                engine->insert_row(*physical_data,
                                   static_cast<size_t>(i),
                                   static_cast<int64_t>(physical_row_start + i),
                                   txn_id,
                                   session_tz);
            }
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index INSERT: "
                  "table_oid={} index_oid={} wal_id={} rows={}",
                  static_cast<unsigned>(table_oid),
                  static_cast<unsigned>(index_oid),
                  wal_record_id,
                  rows);
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_DELETE)) {
            // V1.d follow-up real-impl: the PHYSICAL_DELETE WAL record itself
            // ships only storage row_ids — no key columns. The operator side
            // (operator_create_index_backfill Phase 2.5 loop) closes that gap
            // by performing a storage_fetch(row_ids) before sending us the
            // message, and forwards the recovered chunk via physical_data.
            // This keeps the engine API unchanged (no row_id → keys reverse
            // map needed) at the cost of one extra read-only fetch per
            // PHYSICAL_DELETE record during catchup — acceptable for the
            // bounded-retry CREATE INDEX loop.
            //
            // Defensive fall-throughs:
            //   - physical_data == nullptr or empty: the operator could not
            //     recover the chunk (rows physically gone, fetch failed).
            //     Best-effort skip + log; the V1.a convergence guard still
            //     fires if the index keeps diverging.
            //   - row_ids.size() != physical_data->size(): partial recovery
            //     (some rows missing from storage). Apply only the prefix
            //     where both sides agree and log the mismatch — same
            //     best-effort posture as the all-missing case.
            if (!physical_data || physical_data->size() == 0) {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index DELETE: no recovered chunk "
                      "(table_oid={} index_oid={} wal_id={} row_ids={}), skipping",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      row_ids.size());
            } else {
                const auto rows = std::min<uint64_t>(physical_data->size(), row_ids.size());
                for (uint64_t i = 0; i < rows; ++i) {
                    engine->mark_delete_row(*physical_data,
                                            static_cast<size_t>(i),
                                            row_ids[static_cast<size_t>(i)],
                                            txn_id,
                                            session_tz);
                }
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index DELETE: "
                      "table_oid={} index_oid={} wal_id={} rows={} (row_ids={} chunk={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      rows,
                      row_ids.size(),
                      physical_data->size());
            }
        } else if (record_type == static_cast<uint8_t>(services::wal::wal_record_type::PHYSICAL_UPDATE)) {
            // V1.d real-impl: PHYSICAL_UPDATE records carry the NEW chunk
            // (in physical_data) and the OLD storage row_ids (in row_ids).
            // The new-row insert half is replayed here. The OLD-row delete
            // half is replayed by a separate apply_wal_record_for_index call
            // with record_type=PHYSICAL_DELETE that the operator issues right
            // after this one — the operator owns the storage_fetch on the
            // OLD row_ids that recovers the key chunk for the delete branch
            // above (same recovery pattern as standalone PHYSICAL_DELETE).
            //
            // Splitting OLD-delete and NEW-insert into two messages keeps the
            // signature stable (one chunk per call) and lets the operator
            // schedule the fetch with its existing disk_address handle (the
            // index manager intentionally does not own a disk_address handle
            // for the catchup path — rule 11: mailbox-only inter-actor).
            if (physical_data && physical_data->size() > 0) {
                // For UPDATE the new storage row-ids are appended starting at
                // physical_row_start (the table writer's append cursor at the
                // time the UPDATE landed); chunk row i maps to that base + i,
                // matching the row_id contract the rest of the engine assumes.
                const auto rows = physical_data->size();
                for (uint64_t i = 0; i < rows; ++i) {
                    engine->insert_row(*physical_data,
                                       static_cast<size_t>(i),
                                       static_cast<int64_t>(physical_row_start + i),
                                       txn_id,
                                       session_tz);
                }
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index UPDATE (insert new half): "
                      "table_oid={} index_oid={} wal_id={} new_rows={} old_row_ids={} "
                      "(OLD-delete half follows as a separate DELETE message)",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id,
                      rows,
                      row_ids.size());
            } else {
                trace(log_,
                      "manager_index_t::apply_wal_record_for_index UPDATE: empty chunk "
                      "(table_oid={} index_oid={} wal_id={})",
                      static_cast<unsigned>(table_oid),
                      static_cast<unsigned>(index_oid),
                      wal_record_id);
            }
        } else {
            trace(log_,
                  "manager_index_t::apply_wal_record_for_index: ignoring "
                  "record_type={} (table_oid={} wal_id={})",
                  static_cast<unsigned>(record_type),
                  static_cast<unsigned>(table_oid),
                  wal_record_id);
        }
        co_return;
    }

} // namespace services::index

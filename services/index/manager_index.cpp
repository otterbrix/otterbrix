#include "manager_index.hpp"

#include "bitcask_index_disk.hpp"
#include "btree_index_disk.hpp"

#include <actor-zeta/spawn.hpp>
#include <components/index/index_engine.hpp>
#include <components/index/hash_single_field_index.hpp>
#include <components/index/single_field_index.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <core/executor.hpp>
#include <msgpack.hpp>
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
} // anonymous namespace

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
                                     run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_index_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , run_fn_(std::move(run_fn))
        , log_(log)
        , path_db_(std::move(path_db))
        , engines_(resource)
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
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_keys_by_type>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_keys_by_type, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_keys_with_types>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_keys_with_types, msg);
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
            if (it->available()) {
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

    manager_index_t::unique_future<uint32_t> manager_index_t::create_index(session_id_t /*session*/,
                                                                           components::catalog::oid_t table_oid,
                                                                           index_name_t index_name,
                                                                           components::index::keys_base_storage_t keys,
                                                                           components::logical_plan::index_type type) {
        trace(log_, "manager_index_t::create_index: {} on oid={}", index_name, static_cast<unsigned>(table_oid));

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
                id_index =
                    components::index::make_index<components::index::hash_single_field_index_t>(engine, index_name, keys);
                break;
            }
            default:
                trace(log_, "manager_index_t::create_index: unsupported index type");
                co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Load index data from btree (persistent storage). Path layout
            // mirrors disk-side ${path_db}/${table_oid}/${index_name}/.
            if (!path_db_.empty()) {
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
                                    idx->insert(reverse_convert(resource_, e.key), e.row_id);
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

        co_return id_index;
    }

    manager_index_t::unique_future<void>
    manager_index_t::drop_index(session_id_t session, components::catalog::oid_t table_oid, index_name_t index_name) {
        trace(log_, "manager_index_t::drop_index: {} on oid={}", index_name, static_cast<unsigned>(table_oid));

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

        co_return;
    }

    // --- Query ---

    manager_index_t::unique_future<bool> manager_index_t::has_index(session_id_t /*session*/,
                                                                    components::catalog::oid_t table_oid,
                                                                    index_name_t index_name) {
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

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (uint64_t i = 0; i < count; i++) {
            engine->insert_row(*data, i, static_cast<int64_t>(start_row_id + i), txn_id);
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

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->mark_delete_row(*data, i, row_ids[i], txn_id);
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

        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Mark old entries as deleted
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->mark_delete_row(*old_data, i, row_ids[i], txn_id);
        }

        // Insert new entries
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->insert_row(*new_data, i, new_start_row_id + static_cast<int64_t>(i), txn_id);
        }

        co_return;
    }

    // --- MVCC commit/revert/cleanup ---

    manager_index_t::unique_future<void>
    manager_index_t::commit_insert(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Mirror committed inserts to disk agents BEFORE commit clears pending maps
        agent_batch_map_t insert_batches;
        agent_addr_map_t insert_addrs;
        engine->for_each_pending_disk_insert(
            txn_id,
            [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                insert_addrs.try_emplace(id, agent_addr);
                insert_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
            });
        for (auto& [id, batch] : insert_batches) {
            auto& addr = insert_addrs.at(id);
            auto [ns, f] =
                actor_zeta::otterbrix::send(addr, &index_agent_disk_t::insert_many, session, std::move(batch));
            schedule_agent(addr, ns);
            pending_void_.emplace_back(std::move(f));
        }

        engine->commit_insert(txn_id, commit_id);

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::commit_delete(execution_context_t ctx, components::catalog::oid_t table_oid, uint64_t commit_id) {
        auto session = ctx.session;
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Mirror committed deletes to disk agents BEFORE commit clears pending maps
        agent_batch_map_t remove_batches;
        agent_addr_map_t remove_addrs;
        engine->for_each_pending_disk_delete(
            txn_id,
            [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key, int64_t row_index) {
                auto id = reinterpret_cast<uintptr_t>(agent_addr.get());
                remove_addrs.try_emplace(id, agent_addr);
                remove_batches[id].emplace_back(value_t(resource_, key), static_cast<size_t>(row_index));
            });
        for (auto& [id, batch] : remove_batches) {
            auto& addr = remove_addrs.at(id);
            auto [ns, f] =
                actor_zeta::otterbrix::send(addr, &index_agent_disk_t::remove_many, session, std::move(batch));
            schedule_agent(addr, ns);
            pending_void_.emplace_back(std::move(f));
        }

        engine->commit_delete(txn_id, commit_id);

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::revert_insert(execution_context_t ctx,
                                                                        components::catalog::oid_t table_oid) {
        auto txn_id = ctx.txn.transaction_id;
        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return;

        it->second->revert_insert(txn_id);
        // No disk action — uncommitted entries never went to disk

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::cleanup_all_versions(session_id_t /*session*/,
                                                                               uint64_t lowest_active) {
        for (auto& [oid, engine] : engines_) {
            engine->cleanup_versions(lowest_active);
        }

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::rebuild_indexes(session_id_t /*session*/,
                                                                          components::catalog::oid_t table_oid) {
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
    manager_index_t::search(session_id_t /*session*/,
                            components::catalog::oid_t table_oid,
                            components::index::keys_base_storage_t keys,
                            components::types::logical_value_t value,
                            components::expressions::compare_type compare,
                            uint64_t start_time,
                            uint64_t txn_id) {
        std::pmr::vector<int64_t> result(resource_);

        auto it = engines_.find(table_oid);
        if (it == engines_.end())
            co_return result;

        auto* index = components::index::search_index(it->second, keys);
        if (!index)
            co_return result;

        co_return index->search(compare, value, start_time, txn_id);
    }

    manager_index_t::unique_future<std::pmr::vector<int64_t>>
    manager_index_t::search_with_preferred_type(session_id_t /*session*/,
                                                components::catalog::oid_t table_oid,
                                                components::index::keys_base_storage_t keys,
                                                components::types::logical_value_t value,
                                                components::expressions::compare_type compare,
                                                uint64_t start_time,
                                                uint64_t txn_id,
                                                components::logical_plan::index_type preferred_type) {
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
        co_return index->search(compare, value, start_time, txn_id);
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    manager_index_t::get_indexed_keys(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return it->second->all_indexed_keys();
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    manager_index_t::get_indexed_keys_by_type(session_id_t /*session*/,
                                              components::catalog::oid_t table_oid,
                                              components::logical_plan::index_type type) {
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return it->second->all_indexed_keys(type);
    }

    manager_index_t::unique_future<std::pmr::vector<indexed_keys_typed_t>>
    manager_index_t::get_indexed_keys_with_types(session_id_t /*session*/, components::catalog::oid_t table_oid) {
        std::pmr::vector<indexed_keys_typed_t> result(resource_);
        auto it = engines_.find(table_oid);
        if (it == engines_.end()) {
            co_return result;
        }

        auto& engine = it->second;
        for (const auto& idx_name : engine->indexes()) {
            auto* idx = components::index::search_index(engine, idx_name);
            if (!idx) {
                continue;
            }
            auto [begin, end] = idx->keys();
            components::index::keys_base_storage_t keys(resource_);
            for (auto key_it = begin; key_it != end; ++key_it) {
                keys.emplace_back(*key_it);
            }
            result.emplace_back(indexed_keys_typed_t{idx->type(), std::move(keys)});
        }
        co_return result;
    }

    manager_index_t::unique_future<void> manager_index_t::flush_all_indexes(session_id_t session) {
        trace(log_, "manager_index_t::flush_all_indexes, session: {}", session.data());
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

} // namespace services::index

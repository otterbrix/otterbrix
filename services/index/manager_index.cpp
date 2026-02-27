#include "manager_index.hpp"

#include <actor-zeta/spawn.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <components/index/index_engine.hpp>
#include <components/index/single_field_index.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>
#include <core/executor.hpp>
#include <msgpack.hpp>

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
        , metafile_indexes_(nullptr)
        , pending_void_(resource) {
        if (!path_db_.empty()) {
            std::filesystem::create_directories(path_db_);
            metafile_indexes_ = open_file(fs_,
                                          path_db_ / "indexes_METADATA",
                                          core::filesystem::file_flags::READ | core::filesystem::file_flags::WRITE |
                                              core::filesystem::file_flags::FILE_CREATE,
                                          core::filesystem::file_lock_type::NO_LOCK);
        }
    }

    void manager_index_t::register_collection_sync(session_id_t /*session*/, const collection_full_name_t& name) {
        trace(log_, "manager_index_t::register_collection_sync: {}", name.to_string());
        auto it = engines_.find(name);
        if (it == engines_.end()) {
            engines_.emplace(name, components::index::make_index_engine(resource_));
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
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::create_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::create_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::drop_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::drop_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::search>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::search, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::has_index>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::has_index, msg);
                break;
            }
            case actor_zeta::msg_id<manager_index_t, &manager_index_t::get_indexed_keys>: {
                co_await actor_zeta::dispatch(this, &manager_index_t::get_indexed_keys, msg);
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
                                                                              collection_full_name_t name) {
        trace(log_, "manager_index_t::register_collection: {}", name.to_string());

        auto it = engines_.find(name);
        if (it == engines_.end()) {
            engines_.emplace(name, components::index::make_index_engine(resource_));
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(session_id_t /*session*/,
                                                                                collection_full_name_t name) {
        trace(log_, "manager_index_t::unregister_collection: {}", name.to_string());

        engines_.erase(name);
        remove_all_indexes_for_collection(name.collection);
        co_return;
    }

    // --- DML: bulk index operations ---

    manager_index_t::unique_future<void>
    manager_index_t::insert_rows(session_id_t session,
                                 collection_full_name_t name,
                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                 uint64_t start_row_id,
                                 uint64_t count) {
        if (!data || count == 0)
            co_return;

        auto it = engines_.find(name);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (uint64_t i = 0; i < count; i++) {
            size_t row = static_cast<size_t>(start_row_id + i);
            engine->insert_row(*data, row);

            // Mirror to disk agents
            engine->for_each_disk_op(
                *data,
                row,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key) {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::insert, session, key, row);
                    schedule_agent(agent_addr, needs_sched);
                    pending_void_.emplace_back(std::move(future));
                });
        }

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::delete_rows(session_id_t session,
                                 collection_full_name_t name,
                                 std::unique_ptr<components::vector::data_chunk_t> data,
                                 std::pmr::vector<size_t> row_ids) {
        if (!data || row_ids.empty())
            co_return;

        auto it = engines_.find(name);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;
        for (auto row_id : row_ids) {
            engine->delete_row(*data, row_id);

            // Mirror to disk agents
            engine->for_each_disk_op(
                *data,
                row_id,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key) {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::remove, session, key, row_id);
                    schedule_agent(agent_addr, needs_sched);
                    pending_void_.emplace_back(std::move(future));
                });
        }

        co_return;
    }

    manager_index_t::unique_future<void>
    manager_index_t::update_rows(session_id_t session,
                                 collection_full_name_t name,
                                 std::unique_ptr<components::vector::data_chunk_t> old_data,
                                 std::unique_ptr<components::vector::data_chunk_t> new_data,
                                 std::pmr::vector<size_t> row_ids) {
        if (!old_data || !new_data || row_ids.empty())
            co_return;

        auto it = engines_.find(name);
        if (it == engines_.end())
            co_return;

        auto& engine = it->second;

        // Delete old entries
        for (auto row_id : row_ids) {
            engine->delete_row(*old_data, row_id);

            // Mirror remove to disk agents
            engine->for_each_disk_op(
                *old_data,
                row_id,
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key) {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::remove, session, key, row_id);
                    schedule_agent(agent_addr, needs_sched);
                    pending_void_.emplace_back(std::move(future));
                });
        }

        // Insert new entries
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->insert_row(*new_data, row_ids[i]);

            // Mirror insert to disk agents
            engine->for_each_disk_op(
                *new_data,
                row_ids[i],
                [&](const actor_zeta::address_t& agent_addr, const components::index::value_t& key) {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent_addr, &index_agent_disk_t::insert, session, key, row_ids[i]);
                    schedule_agent(agent_addr, needs_sched);
                    pending_void_.emplace_back(std::move(future));
                });
        }

        co_return;
    }

    // --- DDL: index management ---

    manager_index_t::unique_future<uint32_t> manager_index_t::create_index(session_id_t /*session*/,
                                                                           collection_full_name_t name,
                                                                           index_name_t index_name,
                                                                           components::index::keys_base_storage_t keys,
                                                                           components::logical_plan::index_type type) {
        trace(log_, "manager_index_t::create_index: {} on {}", index_name, name.to_string());

        auto it = engines_.find(name);
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
            default:
                trace(log_, "manager_index_t::create_index: unsupported index type");
                co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Load index data from btree (persistent storage)
            if (!path_db_.empty()) {
                auto btree_path = path_db_ / name.database / name.collection / index_name;
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
                        actor_zeta::spawn<index_agent_disk_t>(resource_, path_db_, name, std::string(index_name), log_);

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

            // Persist index metadata
            auto node =
                components::logical_plan::make_node_create_index(resource_, name, std::string(index_name), type);
            node->keys() = keys;
            write_index_to_metafile(node);
        }

        co_return id_index;
    }

    manager_index_t::unique_future<void>
    manager_index_t::drop_index(session_id_t session, collection_full_name_t name, index_name_t index_name) {
        trace(log_, "manager_index_t::drop_index: {} on {}", index_name, name.to_string());

        auto it = engines_.find(name);
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

            // Remove from metafile
            remove_index_from_metafile(index_name);
        }

        co_return;
    }

    // --- Query ---

    manager_index_t::unique_future<std::pmr::vector<int64_t>>
    manager_index_t::search(session_id_t /*session*/,
                            collection_full_name_t name,
                            components::index::keys_base_storage_t keys,
                            components::types::logical_value_t value,
                            components::expressions::compare_type compare) {
        std::pmr::vector<int64_t> result(resource_);

        auto it = engines_.find(name);
        if (it == engines_.end())
            co_return result;

        auto* index = components::index::search_index(it->second, keys);
        if (!index)
            co_return result;

        co_return index->search(compare, value);
    }

    manager_index_t::unique_future<bool>
    manager_index_t::has_index(session_id_t /*session*/, collection_full_name_t name, index_name_t index_name) {
        auto it = engines_.find(name);
        if (it == engines_.end())
            co_return false;

        co_return it->second->has_index(index_name);
    }

    manager_index_t::unique_future<std::pmr::vector<components::index::keys_base_storage_t>>
    manager_index_t::get_indexed_keys(session_id_t /*session*/, collection_full_name_t name) {
        auto it = engines_.find(name);
        if (it == engines_.end()) {
            co_return std::pmr::vector<components::index::keys_base_storage_t>(resource_);
        }
        co_return it->second->all_indexed_keys();
    }

    // --- Index metafile persistence ---

    void manager_index_t::write_index_to_metafile(const components::logical_plan::node_create_index_ptr& index) {
        if (!metafile_indexes_)
            return;
        components::serializer::msgpack_serializer_t serializer(resource_);
        serializer.start_array(1);
        index->serialize(&serializer);
        serializer.end_array();
        auto buf = serializer.result();
        auto size = buf.size();
        metafile_indexes_->write(&size, sizeof(size), metafile_indexes_->file_size());
        metafile_indexes_->write(buf.data(), buf.size(), metafile_indexes_->file_size());
    }

    std::vector<components::logical_plan::node_create_index_ptr> manager_index_t::read_indexes_from_metafile() const {
        std::vector<components::logical_plan::node_create_index_ptr> res;
        if (!metafile_indexes_)
            return res;

        constexpr auto count_byte_by_size = sizeof(size_t);
        size_t size;
        size_t offset = 0;
        std::unique_ptr<char[]> size_str(new char[count_byte_by_size]);

        while (true) {
            metafile_indexes_->seek(offset);
            auto bytes_read = metafile_indexes_->read(size_str.get(), count_byte_by_size);
            if (bytes_read == count_byte_by_size) {
                offset += count_byte_by_size;
                std::memcpy(&size, size_str.get(), count_byte_by_size);
                std::pmr::string buf(resource_);
                buf.resize(size);
                metafile_indexes_->read(buf.data(), size, offset);
                offset += size;
                components::serializer::msgpack_deserializer_t deserializer(buf);
                deserializer.advance_array(0);
                auto index = components::logical_plan::node_t::deserialize(&deserializer);
                deserializer.pop_array();
                res.push_back(
                    boost::polymorphic_pointer_downcast<components::logical_plan::node_create_index_t>(index));
            } else {
                break;
            }
        }
        return res;
    }

    void manager_index_t::remove_index_from_metafile(const index_name_t& name) {
        if (!metafile_indexes_)
            return;
        auto indexes = read_indexes_from_metafile();
        indexes.erase(std::remove_if(indexes.begin(),
                                     indexes.end(),
                                     [&name](const components::logical_plan::node_create_index_ptr& index) {
                                         return index->name() == name;
                                     }),
                      indexes.end());
        metafile_indexes_->truncate(0);
        for (const auto& index : indexes) {
            write_index_to_metafile(index);
        }
    }

    void manager_index_t::remove_all_indexes_for_collection(const collection_name_t& collection) {
        if (!metafile_indexes_)
            return;
        auto indexes = read_indexes_from_metafile();
        indexes.erase(std::remove_if(indexes.begin(),
                                     indexes.end(),
                                     [&collection](const components::logical_plan::node_create_index_ptr& index) {
                                         return index->collection_name() == collection;
                                     }),
                      indexes.end());
        metafile_indexes_->truncate(0);
        for (const auto& index : indexes) {
            write_index_to_metafile(index);
        }
    }

} // namespace services::index

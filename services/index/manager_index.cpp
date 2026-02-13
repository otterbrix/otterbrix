#include "manager_index.hpp"

#include <components/index/single_field_index.hpp>
#include <components/index/index_engine.hpp>
#include <boost/polymorphic_pointer_cast.hpp>
#include <components/serialization/serializer.hpp>
#include <components/serialization/deserializer.hpp>
#include <services/disk/manager_disk.hpp>

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
                                          core::filesystem::file_flags::READ |
                                          core::filesystem::file_flags::WRITE |
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

    // --- Collection lifecycle ---

    manager_index_t::unique_future<void> manager_index_t::register_collection(
        session_id_t /*session*/, collection_full_name_t name) {
        trace(log_, "manager_index_t::register_collection: {}", name.to_string());

        auto it = engines_.find(name);
        if (it == engines_.end()) {
            engines_.emplace(name, components::index::make_index_engine(resource_));
        }
        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::unregister_collection(
        session_id_t /*session*/, collection_full_name_t name) {
        trace(log_, "manager_index_t::unregister_collection: {}", name.to_string());

        engines_.erase(name);
        remove_all_indexes_for_collection(name.collection);
        co_return;
    }

    // --- DML: bulk index operations ---

    manager_index_t::unique_future<void> manager_index_t::insert_rows(
        session_id_t /*session*/,
        collection_full_name_t name,
        std::unique_ptr<components::vector::data_chunk_t> data,
        uint64_t start_row_id,
        uint64_t count) {

        if (!data || count == 0) co_return;

        auto it = engines_.find(name);
        if (it == engines_.end()) co_return;

        auto& engine = it->second;
        for (uint64_t i = 0; i < count; i++) {
            size_t row = static_cast<size_t>(start_row_id + i);
            engine->insert_row(*data, row);
        }

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::delete_rows(
        session_id_t /*session*/,
        collection_full_name_t name,
        std::unique_ptr<components::vector::data_chunk_t> data,
        std::pmr::vector<size_t> row_ids) {

        if (!data || row_ids.empty()) co_return;

        auto it = engines_.find(name);
        if (it == engines_.end()) co_return;

        auto& engine = it->second;
        for (auto row_id : row_ids) {
            engine->delete_row(*data, row_id);
        }

        co_return;
    }

    manager_index_t::unique_future<void> manager_index_t::update_rows(
        session_id_t /*session*/,
        collection_full_name_t name,
        std::unique_ptr<components::vector::data_chunk_t> old_data,
        std::unique_ptr<components::vector::data_chunk_t> new_data,
        std::pmr::vector<size_t> row_ids) {

        if (!old_data || !new_data || row_ids.empty()) co_return;

        auto it = engines_.find(name);
        if (it == engines_.end()) co_return;

        auto& engine = it->second;

        // Delete old entries
        for (auto row_id : row_ids) {
            engine->delete_row(*old_data, row_id);
        }

        // Insert new entries
        for (size_t i = 0; i < row_ids.size(); i++) {
            engine->insert_row(*new_data, row_ids[i]);
        }

        co_return;
    }

    // --- DDL: index management ---

    manager_index_t::unique_future<uint32_t> manager_index_t::create_index(
        session_id_t session,
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
                id_index = components::index::make_index<components::index::single_field_index_t>(
                    engine, index_name, keys);
                break;
            }
            default:
                trace(log_, "manager_index_t::create_index: unsupported index type");
                co_return components::index::INDEX_ID_UNDEFINED;
        }

        if (id_index != components::index::INDEX_ID_UNDEFINED) {
            // Populate index from storage via manager_disk_t
            auto [_tr, trf] = actor_zeta::send(disk_address_,
                &disk::manager_disk_t::storage_total_rows, session, name);
            auto total_rows = co_await std::move(trf);

            if (total_rows > 0) {
                auto [_ss, ssf] = actor_zeta::send(disk_address_,
                    &disk::manager_disk_t::storage_scan_segment,
                    session, name, int64_t{0}, total_rows);
                auto scan_data = co_await std::move(ssf);

                if (scan_data) {
                    for (uint64_t i = 0; i < scan_data->size(); i++) {
                        engine->insert_row(*scan_data, i);
                    }
                }
            }

            // Create disk index for persistent storage
            if (!path_db_.empty()) {
                auto disk_path = path_db_ / name.database / name.collection / index_name;
                auto index_disk = std::make_unique<index_disk_t>(disk_path, resource_);
                index_disks_.try_emplace(std::string(index_name), std::move(index_disk));
            }

            // Persist index metadata
            auto node = components::logical_plan::make_node_create_index(
                resource_, name, std::string(index_name), type);
            node->keys() = keys;
            write_index_to_metafile(node);
        }

        co_return id_index;
    }

    manager_index_t::unique_future<void> manager_index_t::drop_index(
        session_id_t /*session*/,
        collection_full_name_t name,
        index_name_t index_name) {

        trace(log_, "manager_index_t::drop_index: {} on {}", index_name, name.to_string());

        auto it = engines_.find(name);
        if (it == engines_.end()) co_return;

        auto& engine = it->second;
        auto* index = components::index::search_index(engine, index_name);

        if (index) {
            // Drop disk index if exists
            auto disk_it = index_disks_.find(std::string(index_name));
            if (disk_it != index_disks_.end()) {
                disk_it->second->drop();
                index_disks_.erase(disk_it);
            }

            components::index::drop_index(engine, index);

            // Remove from metafile
            remove_index_from_metafile(index_name);
        }

        co_return;
    }

    // --- Query ---

    manager_index_t::unique_future<std::pmr::vector<int64_t>> manager_index_t::search(
        session_id_t /*session*/,
        collection_full_name_t name,
        components::index::keys_base_storage_t keys,
        components::types::logical_value_t value,
        components::expressions::compare_type compare) {

        std::pmr::vector<int64_t> result(resource_);

        auto it = engines_.find(name);
        if (it == engines_.end()) co_return result;

        auto& engine = it->second;
        auto* index = components::index::search_index(engine, keys);
        if (!index) co_return result;

        // Perform in-memory search based on compare type
        switch (compare) {
            case components::expressions::compare_type::eq: {
                auto range = index->find(value);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case components::expressions::compare_type::lt: {
                auto range = index->lower_bound(value);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case components::expressions::compare_type::lte: {
                auto ub = index->upper_bound(value);
                for (auto iter = index->cbegin(); iter != ub.first; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case components::expressions::compare_type::gt: {
                auto range = index->upper_bound(value);
                for (auto iter = range.first; iter != range.second; ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case components::expressions::compare_type::gte: {
                auto lb = index->lower_bound(value);
                for (auto iter = lb.second; iter != index->cend(); ++iter) {
                    result.push_back(iter->row_index);
                }
                break;
            }
            case components::expressions::compare_type::ne: {
                auto eq_range = index->find(value);
                for (auto iter = index->cbegin(); iter != index->cend(); ++iter) {
                    bool in_eq = false;
                    for (auto eq_it = eq_range.first; eq_it != eq_range.second; ++eq_it) {
                        if (eq_it->row_index == iter->row_index) {
                            in_eq = true;
                            break;
                        }
                    }
                    if (!in_eq) {
                        result.push_back(iter->row_index);
                    }
                }
                break;
            }
            default:
                break;
        }

        co_return result;
    }

    manager_index_t::unique_future<bool> manager_index_t::has_index(
        session_id_t /*session*/,
        collection_full_name_t name,
        index_name_t index_name) {

        auto it = engines_.find(name);
        if (it == engines_.end()) co_return false;

        co_return it->second->has_index(index_name);
    }

    // --- Index metafile persistence ---

    void manager_index_t::write_index_to_metafile(const components::logical_plan::node_create_index_ptr& index) {
        if (!metafile_indexes_) return;
        components::serializer::msgpack_serializer_t serializer(resource_);
        serializer.start_array(1);
        index->serialize(&serializer);
        serializer.end_array();
        auto buf = serializer.result();
        auto size = buf.size();
        metafile_indexes_->write(&size, sizeof(size), metafile_indexes_->file_size());
        metafile_indexes_->write(buf.data(), buf.size(), metafile_indexes_->file_size());
    }

    std::vector<components::logical_plan::node_create_index_ptr>
    manager_index_t::read_indexes_from_metafile() const {
        std::vector<components::logical_plan::node_create_index_ptr> res;
        if (!metafile_indexes_) return res;

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
        if (!metafile_indexes_) return;
        auto indexes = read_indexes_from_metafile();
        indexes.erase(std::remove_if(indexes.begin(), indexes.end(),
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
        if (!metafile_indexes_) return;
        auto indexes = read_indexes_from_metafile();
        indexes.erase(std::remove_if(indexes.begin(), indexes.end(),
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

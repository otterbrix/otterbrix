#include "manager_disk.hpp"
#include "result.hpp"
#include <actor-zeta/spawn.hpp>
#include <chrono>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <thread>
#include <unordered_set>

#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

namespace services::disk {

    using namespace core::filesystem;

    namespace {
    } // namespace

    manager_disk_t::manager_disk_t(std::pmr::memory_resource* resource,
                                   actor_zeta::scheduler_raw scheduler,
                                   actor_zeta::scheduler_raw scheduler_disk,
                                   configuration::config_disk config,
                                   log_t& log,
                                   run_fn_t run_fn)
        : actor_zeta::actor::actor_mixin<manager_disk_t>()
        , resource_(resource)
        , scheduler_(scheduler)
        , scheduler_disk_(scheduler_disk)
        , run_fn_(std::move(run_fn))
        , log_(log.clone())
        , fs_(core::filesystem::local_file_system_t())
        , config_(std::move(config))
        , pending_void_(resource)
        , pending_load_(resource)
        , pending_find_(resource) {
        trace(log_, "manager_disk start");
        if (!config_.path.empty()) {
            create_directories(config_.path);
        }
        create_agent(config.agent);
        trace(log_, "manager_disk finish");
    }

    manager_disk_t::~manager_disk_t() { trace(log_, "delete manager_disk_t"); }

    void manager_disk_t::poll_pending() {
        if (is_polling_) {
            return;
        }
        is_polling_ = true;

        if (pending_void_.empty() && pending_load_.empty() && pending_find_.empty()) {
            is_polling_ = false;
            return;
        }

        size_t i = 0;
        while (i < pending_void_.size()) {
            if (!pending_void_[i].valid() || pending_void_[i].available()) {
                if (i + 1 < pending_void_.size()) {
                    std::swap(pending_void_[i], pending_void_.back());
                }
                pending_void_.pop_back();
            } else {
                ++i;
            }
        }

        i = 0;
        while (i < pending_load_.size()) {
            if (!pending_load_[i].valid() || pending_load_[i].available()) {
                if (i + 1 < pending_load_.size()) {
                    std::swap(pending_load_[i], pending_load_.back());
                }
                pending_load_.pop_back();
            } else {
                ++i;
            }
        }

        i = 0;
        while (i < pending_find_.size()) {
            if (!pending_find_[i].valid() || pending_find_[i].available()) {
                if (i + 1 < pending_find_.size()) {
                    std::swap(pending_find_[i], pending_find_.back());
                }
                pending_find_.pop_back();
            } else {
                ++i;
            }
        }

        is_polling_ = false;
    }

    std::pair<bool, actor_zeta::detail::enqueue_result>
    manager_disk_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
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

    actor_zeta::behavior_t manager_disk_t::behavior(actor_zeta::mailbox::message* msg) {
        poll_pending();

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::load>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::load_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::load_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::append_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::remove_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::remove_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::append_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::append_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::remove_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::remove_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::write_data_chunk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::write_data_chunk, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::remove_documents>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::remove_documents, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::flush, msg);
                break;
            }
            // Storage management
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_with_columns>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_with_columns, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::drop_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::drop_storage, msg);
                break;
            }
            // Storage queries
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_types>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_types, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_total_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_total_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_calculate_size>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_calculate_size, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_columns>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_columns, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_has_schema>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_has_schema, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_adopt_schema>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_adopt_schema, msg);
                break;
            }
            // Storage data operations
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_fetch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_fetch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_scan_segment>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_scan_segment, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_update>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_update, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_delete_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_delete_rows, msg);
                break;
            }
            default:
                break;
        }
    }

    void manager_disk_t::sync(address_pack pack) {
        constexpr static int manager_wal = 0;
        manager_wal_ = std::get<manager_wal>(pack);
    }

    void manager_disk_t::create_agent(int count_agents) {
        for (int i = 0; i < count_agents; i++) {
            auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
            trace(log_, "manager_disk create_agent : {}", name_agent);
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), this, config_.path, log_);
            agents_.emplace_back(std::move(agent));
        }
    }

    manager_disk_t::unique_future<result_load_t> manager_disk_t::load(session_id_t session) {
        trace(log_, "manager_disk_t::load , session : {}", session.data());
        auto [needs_sched, future] = actor_zeta::otterbrix::send(agent(), &agent_disk_t::load, session);
        if (needs_sched) {
            scheduler_->enqueue(agents_[0].get());
        }
        co_return co_await std::move(future);
    }

    manager_disk_t::unique_future<void> manager_disk_t::load_indexes(session_id_t session,
                                                                     actor_zeta::address_t /*dispatcher_address*/) {
        trace(log_,
              "manager_disk_t::load_indexes , session : {} (no-op, indexes handled by manager_index_t)",
              session.data());
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::append_database(session_id_t session,
                                                                        database_name_t database) {
        trace(log_, "manager_disk_t::append_database , session : {} , database : {}", session.data(), database);
        command_append_database_t command{database};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::remove_database(session_id_t session,
                                                                        database_name_t database) {
        trace(log_, "manager_disk_t::remove_database , session : {} , database : {}", session.data(), database);
        command_remove_database_t command{database};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::append_collection(session_id_t session, database_name_t database, collection_name_t collection) {
        trace(log_,
              "manager_disk_t::append_collection , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_append_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::remove_collection(session_id_t session, database_name_t database, collection_name_t collection) {
        trace(log_,
              "manager_disk_t::remove_collection , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_remove_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::remove_documents(session_id_t session,
                                                                         database_name_t database,
                                                                         collection_name_t collection,
                                                                         document_ids_t documents) {
        trace(log_,
              "manager_disk_t::remove_documents , session : {} , database : {} , collection : {}",
              session.data(),
              database,
              collection);
        command_remove_documents_t command{std::move(database), std::move(collection), std::move(documents)};
        append_command(commands_, session, command_t(command));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::write_data_chunk(session_id_t session,
                                     database_name_t database,
                                     collection_name_t collection,
                                     std::unique_ptr<components::vector::data_chunk_t> data) {
        trace(log_,
              "manager_disk_t::write_data_chunk , session : {} , database : {} , collection : {} , rows : {}",
              session.data(),
              database,
              collection,
              data ? data->size() : 0);
        // TODO: Implement actual disk persistence for data_chunk (columnar storage)
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::flush(session_id_t session, wal::id_t wal_id) {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);

        // Batch: collect commands from the target session AND any other accumulated sessions
        std::vector<command_t> batch;
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            batch.insert(batch.end(),
                         std::make_move_iterator(it->second.begin()),
                         std::make_move_iterator(it->second.end()));
            commands_.erase(it);
        }
        // Opportunistic batching: flush other sessions' commands that have accumulated
        for (auto sit = commands_.begin(); sit != commands_.end();) {
            batch.insert(batch.end(),
                         std::make_move_iterator(sit->second.begin()),
                         std::make_move_iterator(sit->second.end()));
            sit = commands_.erase(sit);
        }

        if (batch.empty()) {
            co_return;
        }

        trace(log_, "manager_disk_t::flush batch size: {}", batch.size());

        // Dispatch all batched commands to the agent without waiting between sends
        std::pmr::vector<unique_future<void>> pending(resource());
        for (const auto& command : batch) {
            switch (command.name()) {
                case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_database>: {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent(), &agent_disk_t::append_database, command);
                    if (needs_sched) {
                        scheduler_->enqueue(agents_[0].get());
                    }
                    pending.push_back(std::move(future));
                    break;
                }
                case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_database>: {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_database, command);
                    if (needs_sched) {
                        scheduler_->enqueue(agents_[0].get());
                    }
                    pending.push_back(std::move(future));
                    break;
                }
                case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::append_collection>: {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent(), &agent_disk_t::append_collection, command);
                    if (needs_sched) {
                        scheduler_->enqueue(agents_[0].get());
                    }
                    pending.push_back(std::move(future));
                    break;
                }
                case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_collection>: {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_collection, command);
                    if (needs_sched) {
                        scheduler_->enqueue(agents_[0].get());
                    }
                    pending.push_back(std::move(future));
                    break;
                }
                case actor_zeta::msg_id<agent_disk_t, &agent_disk_t::remove_documents>: {
                    auto [needs_sched, future] =
                        actor_zeta::otterbrix::send(agent(), &agent_disk_t::remove_documents, command);
                    if (needs_sched) {
                        scheduler_->enqueue(agents_[0].get());
                    }
                    pending.push_back(std::move(future));
                    break;
                }
                default:
                    break;
            }
        }

        // Await all dispatched commands
        for (auto& fut : pending) {
            co_await std::move(fut);
        }
        co_return;
    }

    // --- Synchronous storage creation (for init before schedulers start) ---

    void manager_disk_t::create_storage_sync(const collection_full_name_t& name) {
        trace(log_, "manager_disk_t::create_storage_sync , name : {}", name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource()));
    }

    void manager_disk_t::create_storage_with_columns_sync(const collection_full_name_t& name,
                                                          std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_with_columns_sync , name : {}", name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
    }

    // --- Storage management ---

    components::storage::storage_t* manager_disk_t::get_storage(const collection_full_name_t& name) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            error(log_, "manager_disk: storage not found for {}", name.to_string());
            return nullptr;
        }
        return it->second->storage.get();
    }

    manager_disk_t::unique_future<void> manager_disk_t::create_storage(session_id_t session,
                                                                       collection_full_name_t name) {
        trace(log_, "manager_disk_t::create_storage , session : {} , name : {}", session.data(), name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource()));
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::create_storage_with_columns(session_id_t session,
                                                collection_full_name_t name,
                                                std::vector<components::table::column_definition_t> columns) {
        trace(log_,
              "manager_disk_t::create_storage_with_columns , session : {} , name : {}",
              session.data(),
              name.to_string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::drop_storage(session_id_t session,
                                                                     collection_full_name_t name) {
        trace(log_, "manager_disk_t::drop_storage , session : {} , name : {}", session.data(), name.to_string());
        storages_.erase(name);
        co_return;
    }

    // --- Storage queries ---

    manager_disk_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_t::storage_types(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return std::pmr::vector<components::types::complex_logical_type>(resource());
        co_return s->types();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_total_rows(session_id_t /*session*/,
                                                                               collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return 0;
        co_return s->total_rows();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_calculate_size(session_id_t /*session*/,
                                                                                   collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return 0;
        co_return s->calculate_size();
    }

    manager_disk_t::unique_future<std::vector<components::table::column_definition_t>>
    manager_disk_t::storage_columns(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return std::vector<components::table::column_definition_t>{};
        const auto& cols = s->columns();
        std::vector<components::table::column_definition_t> result;
        result.reserve(cols.size());
        for (const auto& col : cols) {
            result.emplace_back(col.copy());
        }
        co_return std::move(result);
    }

    manager_disk_t::unique_future<bool> manager_disk_t::storage_has_schema(session_id_t /*session*/,
                                                                           collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return false;
        co_return s->has_schema();
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_adopt_schema(session_id_t /*session*/,
                                         collection_full_name_t name,
                                         std::pmr::vector<components::types::complex_logical_type> types) {
        auto* s = get_storage(name);
        if (s)
            s->adopt_schema(types);
        co_return;
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan(session_id_t /*session*/,
                                 collection_full_name_t name,
                                 std::unique_ptr<components::table::table_filter_t> filter,
                                 int limit) {
        auto* s = get_storage(name);
        if (!s)
            co_return nullptr;
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit);
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  collection_full_name_t name,
                                  components::vector::vector_t row_ids,
                                  uint64_t count) {
        auto* s = get_storage(name);
        if (!s)
            co_return nullptr;
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types, count);
        s->fetch(*result, row_ids, count);
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan_segment(session_id_t /*session*/,
                                         collection_full_name_t name,
                                         int64_t start,
                                         uint64_t count) {
        auto* s = get_storage(name);
        if (!s)
            co_return nullptr;
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { result->append(chunk); });
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::pair<uint64_t, uint64_t>>
    manager_disk_t::storage_append(session_id_t /*session*/,
                                   collection_full_name_t name,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(name);
        if (!s || !data || data->size() == 0)
            co_return std::make_pair(uint64_t{0}, uint64_t{0});

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 2. Column expansion — expand incoming data if storage has more columns
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() < table_columns.size()) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(data->data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], data->size());
                    expanded_data.back().validity().set_all_invalid(data->size());
                }
            }
            data->data = std::move(expanded_data);
        }

        // 3. Dedup — filter out rows with _id values that already exist in the table
        if (s->total_rows() > 0) {
            int64_t id_col = -1;
            for (uint64_t col = 0; col < data->column_count(); col++) {
                if (data->data[col].type().has_alias() && data->data[col].type().alias() == "_id") {
                    id_col = static_cast<int64_t>(col);
                    break;
                }
            }
            if (id_col >= 0) {
                auto existing = std::make_unique<components::vector::data_chunk_t>(resource(), s->types(), 0);
                s->scan(*existing, nullptr, -1);

                int64_t existing_id_col = -1;
                for (uint64_t col = 0; col < existing->column_count(); col++) {
                    if (existing->data[col].type().has_alias() && existing->data[col].type().alias() == "_id") {
                        existing_id_col = static_cast<int64_t>(col);
                        break;
                    }
                }

                if (existing_id_col >= 0 && existing->size() > 0) {
                    std::unordered_set<std::string> existing_ids;
                    for (uint64_t i = 0; i < existing->size(); i++) {
                        auto val = existing->data[static_cast<size_t>(existing_id_col)].value(i);
                        if (!val.is_null()) {
                            existing_ids.emplace(val.value<std::string_view>());
                        }
                    }

                    std::vector<uint64_t> keep_rows;
                    keep_rows.reserve(data->size());
                    for (uint64_t i = 0; i < data->size(); i++) {
                        auto val = data->data[static_cast<size_t>(id_col)].value(i);
                        if (val.is_null() ||
                            existing_ids.find(std::string(val.value<std::string_view>())) == existing_ids.end()) {
                            keep_rows.push_back(i);
                        }
                    }

                    if (keep_rows.empty()) {
                        co_return std::make_pair(uint64_t{0}, uint64_t{0});
                    }

                    if (keep_rows.size() < data->size()) {
                        auto filtered = std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                                           data->types(),
                                                                                           keep_rows.size());
                        for (uint64_t col = 0; col < data->column_count(); col++) {
                            for (uint64_t i = 0; i < keep_rows.size(); i++) {
                                auto val = data->data[col].value(keep_rows[i]);
                                filtered->data[col].set_value(i, val);
                            }
                        }
                        data = std::move(filtered);
                    }
                }
            }
        }

        // 4. Type compatibility check — computing tables may evolve types per column,
        //    but columnar storage is fixed-type. Skip append if types don't match.
        if (s->has_schema() && !table_columns.empty()) {
            bool types_match = true;
            for (size_t i = 0; i < table_columns.size() && i < data->column_count(); i++) {
                if (data->data[i].type().type() != table_columns[i].type().type()) {
                    types_match = false;
                    break;
                }
            }
            if (!types_match) {
                trace(log_, "storage_append: column type mismatch, skipping append (type evolution)");
                co_return std::make_pair(s->total_rows(), uint64_t{0});
            }
        }

        // 5. Append
        auto actual_count = data->size();
        auto start_row = s->append(*data);
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::storage_update(session_id_t /*session*/,
                                   collection_full_name_t name,
                                   components::vector::vector_t row_ids,
                                   std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(name);
        if (s)
            s->update(row_ids, *data);
        co_return;
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_delete_rows(session_id_t /*session*/,
                                                                                collection_full_name_t name,
                                                                                components::vector::vector_t row_ids,
                                                                                uint64_t count) {
        auto* s = get_storage(name);
        if (!s)
            co_return 0;
        co_return s->delete_rows(row_ids, count);
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t { return agents_[0]->address(); }

    manager_disk_empty_t::manager_disk_empty_t(std::pmr::memory_resource* resource,
                                               actor_zeta::scheduler::sharing_scheduler* /*scheduler*/)
        : actor_zeta::actor::actor_mixin<manager_disk_empty_t>()
        , resource_(resource)
        , pending_void_(resource)
        , pending_load_(resource)
        , pending_find_(resource) {}

    auto manager_disk_empty_t::make_type() const noexcept -> const char* { return "manager_disk"; }

    void manager_disk_empty_t::sync(address_pack /*pack*/) {}

    void manager_disk_empty_t::create_agent(int) {}

    actor_zeta::behavior_t manager_disk_empty_t::behavior(actor_zeta::mailbox::message* msg) {
        pending_void_.erase(std::remove_if(pending_void_.begin(),
                                           pending_void_.end(),
                                           [](const auto& f) { return !f.valid() || f.available(); }),
                            pending_void_.end());
        pending_load_.erase(std::remove_if(pending_load_.begin(),
                                           pending_load_.end(),
                                           [](const auto& f) { return !f.valid() || f.available(); }),
                            pending_load_.end());

        switch (msg->command()) {
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::load>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::load, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::load_indexes>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::load_indexes, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::append_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::append_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::remove_database>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::remove_database, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::append_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::append_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::remove_collection>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::remove_collection, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::write_data_chunk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::write_data_chunk, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::remove_documents>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::remove_documents, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::flush, msg);
                break;
            }
            // Storage management
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::create_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::create_storage, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::create_storage_with_columns>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::create_storage_with_columns, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::drop_storage>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::drop_storage, msg);
                break;
            }
            // Storage queries
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_types>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_types, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_total_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_total_rows, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_calculate_size>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_calculate_size, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_columns>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_columns, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_has_schema>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_has_schema, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_adopt_schema>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_adopt_schema, msg);
                break;
            }
            // Storage data operations
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_scan, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_fetch>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_fetch, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_scan_segment>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_scan_segment, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_update>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_update, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_delete_rows>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_delete_rows, msg);
                break;
            }
            default:
                break;
        }
    }

    manager_disk_empty_t::unique_future<result_load_t> manager_disk_empty_t::load(session_id_t /*session*/) {
        co_return result_load_t::empty();
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::load_indexes(session_id_t /*session*/, actor_zeta::address_t /*dispatcher_address*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::append_database(session_id_t /*session*/,
                                                                                    database_name_t /*database*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::remove_database(session_id_t /*session*/,
                                                                                    database_name_t /*database*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::append_collection(session_id_t /*session*/,
                                            database_name_t /*database*/,
                                            collection_name_t /*collection*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::remove_collection(session_id_t /*session*/,
                                            database_name_t /*database*/,
                                            collection_name_t /*collection*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::write_data_chunk(session_id_t /*session*/,
                                           database_name_t /*database*/,
                                           collection_name_t /*collection*/,
                                           std::unique_ptr<components::vector::data_chunk_t> /*data*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::remove_documents(session_id_t /*session*/,
                                                                                     database_name_t /*database*/,
                                                                                     collection_name_t /*collection*/,
                                                                                     document_ids_t /*documents*/) {
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::flush(session_id_t /*session*/,
                                                                          wal::id_t /*wal_id*/) {
        co_return;
    }

    // --- Storage management (in-memory, no disk I/O) ---

    components::storage::storage_t* manager_disk_empty_t::get_storage(const collection_full_name_t& name) {
        auto it = storages_.find(name);
        if (it == storages_.end())
            return nullptr;
        return it->second->storage.get();
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::create_storage(session_id_t /*session*/,
                                                                                   collection_full_name_t name) {
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource()));
        co_return;
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::create_storage_with_columns(session_id_t /*session*/,
                                                      collection_full_name_t name,
                                                      std::vector<components::table::column_definition_t> columns) {
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns)));
        co_return;
    }

    manager_disk_empty_t::unique_future<void> manager_disk_empty_t::drop_storage(session_id_t /*session*/,
                                                                                 collection_full_name_t name) {
        storages_.erase(name);
        co_return;
    }

    manager_disk_empty_t::unique_future<std::pmr::vector<components::types::complex_logical_type>>
    manager_disk_empty_t::storage_types(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return std::pmr::vector<components::types::complex_logical_type>(resource());
        co_return s->types();
    }

    manager_disk_empty_t::unique_future<uint64_t>
    manager_disk_empty_t::storage_total_rows(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return 0;
        co_return s->total_rows();
    }

    manager_disk_empty_t::unique_future<uint64_t>
    manager_disk_empty_t::storage_calculate_size(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return 0;
        co_return s->calculate_size();
    }

    manager_disk_empty_t::unique_future<std::vector<components::table::column_definition_t>>
    manager_disk_empty_t::storage_columns(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return std::vector<components::table::column_definition_t>{};
        const auto& cols = s->columns();
        std::vector<components::table::column_definition_t> result;
        result.reserve(cols.size());
        for (const auto& col : cols) {
            result.emplace_back(col.name(), col.type());
        }
        co_return result;
    }

    manager_disk_empty_t::unique_future<bool> manager_disk_empty_t::storage_has_schema(session_id_t /*session*/,
                                                                                       collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s)
            co_return false;
        co_return s->has_schema();
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::storage_adopt_schema(session_id_t /*session*/,
                                               collection_full_name_t name,
                                               std::pmr::vector<components::types::complex_logical_type> types) {
        auto* s = get_storage(name);
        if (s)
            s->adopt_schema(types);
        co_return;
    }

    manager_disk_empty_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_empty_t::storage_scan(session_id_t /*session*/,
                                       collection_full_name_t name,
                                       std::unique_ptr<components::table::table_filter_t> filter,
                                       int limit) {
        auto* s = get_storage(name);
        if (!s)
            co_return nullptr;
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit);
        co_return std::move(result);
    }

    manager_disk_empty_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_empty_t::storage_fetch(session_id_t /*session*/,
                                        collection_full_name_t name,
                                        components::vector::vector_t row_ids,
                                        uint64_t count) {
        auto* s = get_storage(name);
        if (!s)
            co_return nullptr;
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->fetch(*result, row_ids, count);
        co_return std::move(result);
    }

    manager_disk_empty_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_empty_t::storage_scan_segment(session_id_t /*session*/,
                                               collection_full_name_t name,
                                               int64_t start,
                                               uint64_t count) {
        auto* s = get_storage(name);
        if (!s)
            co_return nullptr;
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { result->append(chunk); });
        co_return std::move(result);
    }

    manager_disk_empty_t::unique_future<std::pair<uint64_t, uint64_t>>
    manager_disk_empty_t::storage_append(session_id_t /*session*/,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(name);
        if (!s || !data || data->size() == 0)
            co_return std::make_pair(uint64_t{0}, uint64_t{0});

        // Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // Column expansion
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() < table_columns.size()) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }
            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < data->column_count(); col++) {
                    if (data->data[col].type().has_alias() &&
                        data->data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(data->data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], data->size());
                    expanded_data.back().validity().set_all_invalid(data->size());
                }
            }
            data->data = std::move(expanded_data);
        }

        // Dedup
        if (s->total_rows() > 0) {
            int64_t id_col = -1;
            for (uint64_t col = 0; col < data->column_count(); col++) {
                if (data->data[col].type().has_alias() && data->data[col].type().alias() == "_id") {
                    id_col = static_cast<int64_t>(col);
                    break;
                }
            }
            if (id_col >= 0) {
                auto existing = std::make_unique<components::vector::data_chunk_t>(resource(), s->types(), 0);
                s->scan(*existing, nullptr, -1);
                int64_t existing_id_col = -1;
                for (uint64_t col = 0; col < existing->column_count(); col++) {
                    if (existing->data[col].type().has_alias() && existing->data[col].type().alias() == "_id") {
                        existing_id_col = static_cast<int64_t>(col);
                        break;
                    }
                }
                if (existing_id_col >= 0 && existing->size() > 0) {
                    std::unordered_set<std::string> existing_ids;
                    for (uint64_t i = 0; i < existing->size(); i++) {
                        auto val = existing->data[static_cast<size_t>(existing_id_col)].value(i);
                        if (!val.is_null()) {
                            existing_ids.emplace(val.value<std::string_view>());
                        }
                    }
                    std::vector<uint64_t> keep_rows;
                    keep_rows.reserve(data->size());
                    for (uint64_t i = 0; i < data->size(); i++) {
                        auto val = data->data[static_cast<size_t>(id_col)].value(i);
                        if (val.is_null() ||
                            existing_ids.find(std::string(val.value<std::string_view>())) == existing_ids.end()) {
                            keep_rows.push_back(i);
                        }
                    }
                    if (keep_rows.empty())
                        co_return std::make_pair(uint64_t{0}, uint64_t{0});
                    if (keep_rows.size() < data->size()) {
                        auto filtered = std::make_unique<components::vector::data_chunk_t>(resource(),
                                                                                           data->types(),
                                                                                           keep_rows.size());
                        for (uint64_t col = 0; col < data->column_count(); col++) {
                            for (uint64_t i = 0; i < keep_rows.size(); i++) {
                                auto val = data->data[col].value(keep_rows[i]);
                                filtered->data[col].set_value(i, val);
                            }
                        }
                        data = std::move(filtered);
                    }
                }
            }
        }

        auto actual_count = data->size();
        auto start_row = s->append(*data);
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::storage_update(session_id_t /*session*/,
                                         collection_full_name_t name,
                                         components::vector::vector_t row_ids,
                                         std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(name);
        if (s)
            s->update(row_ids, *data);
        co_return;
    }

    manager_disk_empty_t::unique_future<uint64_t>
    manager_disk_empty_t::storage_delete_rows(session_id_t /*session*/,
                                              collection_full_name_t name,
                                              components::vector::vector_t row_ids,
                                              uint64_t count) {
        auto* s = get_storage(name);
        if (!s)
            co_return 0;
        co_return s->delete_rows(row_ids, count);
    }

} //namespace services::disk
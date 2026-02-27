#include "manager_disk.hpp"
#include "result.hpp"
#include <actor-zeta/spawn.hpp>
#include <chrono>
#include <thread>
#include <unordered_set>

#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>

#include <boost/polymorphic_pointer_cast.hpp>

namespace services::disk {

    using namespace core::filesystem;

    namespace {
    } // namespace

    // ---- table_storage_t implementations ----

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource)
        : mode_(storage_mode_t::IN_MEMORY)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , block_manager_(std::make_unique<components::table::storage::in_memory_block_manager_t>(
              buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE))
        , table_(std::make_unique<components::table::data_table_t>(
              resource,
              *block_manager_,
              std::vector<components::table::column_definition_t>{})) {}

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns)
        : mode_(storage_mode_t::IN_MEMORY)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_)
        , block_manager_(std::make_unique<components::table::storage::in_memory_block_manager_t>(
              buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE))
        , table_(std::make_unique<components::table::data_table_t>(
              resource, *block_manager_, std::move(columns))) {}

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     std::vector<components::table::column_definition_t> columns,
                                     const std::filesystem::path& otbx_path)
        : mode_(storage_mode_t::DISK)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(
            buffer_manager_, fs_, otbx_path.string());
        bm->create_new_database();
        block_manager_ = std::move(bm);
        table_ = std::make_unique<components::table::data_table_t>(
            resource, *block_manager_, std::move(columns));
    }

    table_storage_t::table_storage_t(std::pmr::memory_resource* resource,
                                     const std::filesystem::path& otbx_path)
        : mode_(storage_mode_t::DISK)
        , buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager_(resource, fs_, buffer_pool_) {
        auto bm = std::make_unique<components::table::storage::single_file_block_manager_t>(
            buffer_manager_, fs_, otbx_path.string());
        bm->load_existing_database();
        block_manager_ = std::move(bm);

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        auto meta_block = block_manager_->meta_block();
        components::table::storage::meta_block_pointer_t meta_ptr;
        meta_ptr.block_pointer = meta_block;
        components::table::storage::metadata_reader_t reader(meta_mgr, meta_ptr);
        table_ = components::table::data_table_t::load_from_disk(resource, *block_manager_, reader);
    }

    void table_storage_t::checkpoint() {
        if (mode_ != storage_mode_t::DISK) {
            return;
        }

        components::table::storage::metadata_manager_t meta_mgr(*block_manager_);
        components::table::storage::metadata_writer_t writer(meta_mgr);
        table_->checkpoint(writer);
        writer.flush();

        auto* disk_bm = static_cast<components::table::storage::single_file_block_manager_t*>(block_manager_.get());
        // Set meta_block_ so write_header() persists it
        disk_bm->set_meta_block(writer.get_block_pointer().block_pointer);
        // Serialize free list to metadata blocks
        auto free_list_ptr = disk_bm->serialize_free_list();
        components::table::storage::database_header_t header;
        header.initialize();
        header.free_list = free_list_ptr.block_pointer;
        disk_bm->write_header(header);
        disk_bm->file_sync();
    }

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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::flush, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::checkpoint_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::checkpoint_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::vacuum_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::vacuum_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::maybe_cleanup>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::maybe_cleanup, msg);
                break;
            }
            // Catalog DDL
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::catalog_append_sequence>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::catalog_append_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::catalog_remove_sequence>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::catalog_remove_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::catalog_append_view>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::catalog_append_view, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::catalog_remove_view>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::catalog_remove_view, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::catalog_append_macro>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::catalog_append_macro, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::catalog_remove_macro>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::catalog_remove_macro, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::create_storage_disk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::create_storage_disk, msg);
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
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_parallel_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_parallel_scan, msg);
                break;
            }
            // MVCC commit/revert
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_commit_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_revert_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_revert_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_t, &manager_disk_t::storage_commit_delete>: {
                co_await actor_zeta::dispatch(this, &manager_disk_t::storage_commit_delete, msg);
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

    manager_disk_t::unique_future<void> manager_disk_t::flush(session_id_t session, wal::id_t wal_id) {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        max_wal_id_ = std::max(max_wal_id_, wal_id);

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

    manager_disk_t::unique_future<wal::id_t> manager_disk_t::checkpoint_all(session_id_t session, wal::id_t current_wal_id) {
        trace(log_, "manager_disk_t::checkpoint_all , session : {} , wal_id : {}", session.data(), current_wal_id);

        // Checkpoint DISK tables and collect schemas from ALL tables
        std::vector<catalog_schema_update_t> schemas;
        for (auto& [name, entry] : storages_) {
            if (entry->table_storage.mode() == storage_mode_t::DISK) {
                trace(log_, "manager_disk_t::checkpoint_all checkpointing : {}", name.to_string());
                entry->table_storage.table().compact();
                entry->table_storage.checkpoint();
            }

            // Collect schema from all tables (including IN_MEMORY) for catalog persistence
            const auto& cols = entry->table_storage.table().columns();
            if (!cols.empty()) {
                std::vector<catalog_column_entry_t> catalog_cols;
                catalog_cols.reserve(cols.size());
                for (const auto& col : cols) {
                    catalog_cols.push_back({col.name(), col.type().type(), col.is_not_null(), col.has_default_value()});
                }
                // Convert storage_mode_t -> table_storage_mode_t
                auto catalog_mode = entry->table_storage.mode() == storage_mode_t::DISK
                    ? table_storage_mode_t::DISK : table_storage_mode_t::IN_MEMORY;
                schemas.push_back({name, std::move(catalog_cols), catalog_mode});
            }
        }

        if (!agents_.empty()) {
            // Update catalog schemas on disk
            if (!schemas.empty()) {
                auto [needs_sched1, future1] = actor_zeta::otterbrix::send(
                    agent(), &agent_disk_t::update_catalog_schemas, std::move(schemas));
                if (needs_sched1) {
                    scheduler_->enqueue(agents_[0].get());
                }
                co_await std::move(future1);
            }

            // Persist WAL ID only if all tables are DISK mode.
            // If any IN_MEMORY tables exist, WAL records are still needed for replay.
            bool has_in_memory = false;
            for (const auto& [name, entry] : storages_) {
                if (entry->table_storage.mode() == storage_mode_t::IN_MEMORY) {
                    has_in_memory = true;
                    break;
                }
            }
            if (current_wal_id > 0 && !has_in_memory) {
                auto [needs_sched2, future2] = actor_zeta::otterbrix::send(
                    agent(), &agent_disk_t::fix_wal_id, wal::id_t{current_wal_id});
                if (needs_sched2) {
                    scheduler_->enqueue(agents_[0].get());
                }
                co_await std::move(future2);
            }

            trace(log_, "manager_disk_t::checkpoint_all complete");
            co_return has_in_memory ? wal::id_t{0} : current_wal_id;
        }

        trace(log_, "manager_disk_t::checkpoint_all complete (no agents)");
        co_return wal::id_t{0};
    }

    manager_disk_t::unique_future<void> manager_disk_t::vacuum_all(
        session_id_t session, uint64_t lowest_active_start_time) {
        trace(log_, "manager_disk_t::vacuum_all , session : {}", session.data());

        for (auto& [name, entry] : storages_) {
            trace(log_, "manager_disk_t::vacuum_all cleaning : {}", name.to_string());
            auto& table = entry->table_storage.table();
            table.cleanup_versions(lowest_active_start_time);
            table.compact();
        }

        trace(log_, "manager_disk_t::vacuum_all complete");
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::maybe_cleanup(
        execution_context_t ctx, uint64_t lowest_active_start_time) {
        auto it = storages_.find(ctx.name);
        if (it == storages_.end()) {
            co_return;
        }

        auto& table = it->second->table_storage.table();
        auto rg = table.row_group();
        auto total = rg->total_rows();
        if (total == 0) {
            co_return;
        }

        auto committed = rg->committed_row_count();
        auto deleted = total - committed;

        // Cleanup if > 30% of rows are deleted
        static constexpr double gc_threshold = 0.3;
        if (static_cast<double>(deleted) / static_cast<double>(total) > gc_threshold) {
            trace(log_, "manager_disk_t::maybe_cleanup: {}, deleted {}/{}, running cleanup",
                  ctx.name.to_string(), deleted, total);
            table.cleanup_versions(lowest_active_start_time);
            table.compact();
        }

        co_return;
    }

    // --- Catalog DDL forwarding to agent_disk_t ---

    manager_disk_t::unique_future<void> manager_disk_t::catalog_append_sequence(
        session_id_t /*session*/, database_name_t database, catalog_sequence_entry_t entry) {
        if (!agents_.empty()) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(
                agent(), &agent_disk_t::append_sequence, std::move(database), std::move(entry));
            if (needs_sched) { scheduler_->enqueue(agents_[0].get()); }
            co_await std::move(future);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::catalog_remove_sequence(
        session_id_t /*session*/, database_name_t database, std::string name) {
        if (!agents_.empty()) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(
                agent(), &agent_disk_t::remove_sequence, std::move(database), std::move(name));
            if (needs_sched) { scheduler_->enqueue(agents_[0].get()); }
            co_await std::move(future);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::catalog_append_view(
        session_id_t /*session*/, database_name_t database, catalog_view_entry_t entry) {
        if (!agents_.empty()) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(
                agent(), &agent_disk_t::append_view, std::move(database), std::move(entry));
            if (needs_sched) { scheduler_->enqueue(agents_[0].get()); }
            co_await std::move(future);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::catalog_remove_view(
        session_id_t /*session*/, database_name_t database, std::string name) {
        if (!agents_.empty()) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(
                agent(), &agent_disk_t::remove_view, std::move(database), std::move(name));
            if (needs_sched) { scheduler_->enqueue(agents_[0].get()); }
            co_await std::move(future);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::catalog_append_macro(
        session_id_t /*session*/, database_name_t database, catalog_macro_entry_t entry) {
        if (!agents_.empty()) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(
                agent(), &agent_disk_t::append_macro, std::move(database), std::move(entry));
            if (needs_sched) { scheduler_->enqueue(agents_[0].get()); }
            co_await std::move(future);
        }
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::catalog_remove_macro(
        session_id_t /*session*/, database_name_t database, std::string name) {
        if (!agents_.empty()) {
            auto [needs_sched, future] = actor_zeta::otterbrix::send(
                agent(), &agent_disk_t::remove_macro, std::move(database), std::move(name));
            if (needs_sched) { scheduler_->enqueue(agents_[0].get()); }
            co_await std::move(future);
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

    void manager_disk_t::create_storage_disk_sync(
        const collection_full_name_t& name,
        std::vector<components::table::column_definition_t> columns,
        const std::filesystem::path& otbx_path) {
        trace(log_, "manager_disk_t::create_storage_disk_sync , name : {} , path : {}", name.to_string(), otbx_path.string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
    }

    void manager_disk_t::load_storage_disk_sync(
        const collection_full_name_t& name,
        const std::filesystem::path& otbx_path) {
        trace(log_, "manager_disk_t::load_storage_disk_sync , name : {} , path : {}", name.to_string(), otbx_path.string());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), otbx_path));
    }

    void manager_disk_t::overlay_column_not_null_sync(
        const collection_full_name_t& name,
        const std::string& col_name) {
        auto* s = get_storage(name);
        if (s) s->overlay_not_null(col_name);
    }

    // --- Direct replay methods (synchronous, no MVCC, for physical WAL replay) ---

    namespace {
        // Deep-copy a data_chunk into a new one using the target resource.
        // Required because deserialized chunks may use a different pmr resource
        // than the storage, and internal validity_mask_t asserts same resource on assign.
        components::vector::data_chunk_t rebuild_chunk(
            std::pmr::memory_resource* target_resource,
            components::vector::data_chunk_t& src) {
            auto count = src.size();
            components::vector::data_chunk_t dst(target_resource, src.types(), count);
            dst.set_cardinality(count);
            for (uint64_t col = 0; col < src.column_count(); col++) {
                for (uint64_t row = 0; row < count; row++) {
                    dst.data[col].set_value(row, src.data[col].value(row));
                }
            }
            return dst;
        }
    } // anonymous namespace

    void manager_disk_t::direct_append_sync(
        const collection_full_name_t& name,
        components::vector::data_chunk_t& data) {
        auto* s = get_storage(name);
        if (!s || data.size() == 0) return;

        // Rebuild data with storage-compatible resource
        auto local = rebuild_chunk(resource(), data);

        // Schema adoption for computing tables
        if (!s->has_schema() && local.column_count() > 0) {
            s->adopt_schema(local.types());
        }

        // Column expansion
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && local.column_count() < table_columns.size()) {
            std::pmr::vector<components::types::complex_logical_type> full_types(resource());
            for (const auto& col_def : table_columns) {
                full_types.push_back(col_def.type());
            }

            std::vector<components::vector::vector_t> expanded_data;
            expanded_data.reserve(table_columns.size());
            for (size_t t = 0; t < table_columns.size(); t++) {
                bool found = false;
                for (uint64_t col = 0; col < local.column_count(); col++) {
                    if (local.data[col].type().has_alias() &&
                        local.data[col].type().alias() == table_columns[t].name()) {
                        expanded_data.push_back(std::move(local.data[col]));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    expanded_data.emplace_back(resource(), full_types[t], local.size());
                    expanded_data.back().validity().set_all_invalid(local.size());
                }
            }
            local.data = std::move(expanded_data);
        }

        // Direct append — no dedup, no NOT NULL enforcement, no MVCC
        s->append(local);
    }

    void manager_disk_t::direct_delete_sync(
        const collection_full_name_t& name,
        const std::pmr::vector<int64_t>& row_ids, uint64_t count) {
        auto* s = get_storage(name);
        if (!s || row_ids.empty()) return;

        // Build a vector_t from row_ids for the storage API
        components::vector::vector_t ids_vec(resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count && i < row_ids.size(); i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        s->delete_rows(ids_vec, count);
    }

    void manager_disk_t::direct_update_sync(
        const collection_full_name_t& name,
        const std::pmr::vector<int64_t>& row_ids,
        components::vector::data_chunk_t& new_data) {
        auto* s = get_storage(name);
        if (!s || row_ids.empty()) return;

        // Build update data matching storage columns (by name).
        // The WAL update chunk may have extra columns from update expressions.
        const auto& table_columns = s->columns();
        auto rows = new_data.size();
        std::pmr::vector<components::types::complex_logical_type> matched_types(resource());
        matched_types.reserve(table_columns.size());
        for (const auto& col_def : table_columns) {
            matched_types.push_back(col_def.type());
        }
        components::vector::data_chunk_t local(resource(), matched_types, rows);
        local.set_cardinality(rows);
        for (size_t t = 0; t < table_columns.size(); t++) {
            bool found = false;
            for (uint64_t c = 0; c < new_data.column_count(); c++) {
                if (new_data.data[c].type().has_alias() &&
                    new_data.data[c].type().alias() == table_columns[t].name()) {
                    // Copy values from source to local
                    for (uint64_t row = 0; row < rows; row++) {
                        local.data[t].set_value(row, new_data.data[c].value(row));
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                // Column not in update data — mark all rows as null
                local.data[t].validity().set_all_invalid(rows);
            }
        }

        auto count = static_cast<uint64_t>(row_ids.size());
        components::vector::vector_t ids_vec(resource(),
            components::types::complex_logical_type(components::types::logical_type::BIGINT),
            count);
        for (uint64_t i = 0; i < count; i++) {
            ids_vec.set_value(i, components::types::logical_value_t(resource(), row_ids[i]));
        }
        s->update(ids_vec, local);
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

    manager_disk_t::unique_future<void> manager_disk_t::create_storage_disk(
        session_id_t session, collection_full_name_t name,
        std::vector<components::table::column_definition_t> columns) {
        trace(log_, "manager_disk_t::create_storage_disk , session : {} , name : {}", session.data(), name.to_string());
        auto otbx_path = config_.path / name.database / "main" / name.collection / "table.otbx";
        std::filesystem::create_directories(otbx_path.parent_path());
        storages_.emplace(name, std::make_unique<collection_storage_entry_t>(resource(), std::move(columns), otbx_path));
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
        if (!s) {
            co_return std::pmr::vector<components::types::complex_logical_type>(resource());
        }
        co_return s->types();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_total_rows(session_id_t /*session*/,
                                                                               collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return 0;
        }
        co_return s->total_rows();
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_calculate_size(session_id_t /*session*/,
                                                                                   collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return 0;
        }
        co_return s->calculate_size();
    }

    manager_disk_t::unique_future<std::vector<components::table::column_definition_t>>
    manager_disk_t::storage_columns(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {
            co_return std::vector<components::table::column_definition_t>{};
        }
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
        if (s) { s->adopt_schema(types); }
        co_return;
    }

    // --- Storage data operations ---

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_scan(
        session_id_t /*session*/, collection_full_name_t name,
        std::unique_ptr<components::table::table_filter_t> filter, int limit,
        components::table::transaction_data txn) {
        auto* s = get_storage(name);
        if (!s) { co_return nullptr };
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_t::storage_fetch(session_id_t /*session*/,
                                  collection_full_name_t name,
                                  components::vector::vector_t row_ids,
                                  uint64_t count) {
        auto* s = get_storage(name);
        if (!s) {co_return nullptr;}
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
        if (!s) {co_return nullptr;}
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { result->append(chunk); });
        co_return std::move(result);
    }

    manager_disk_t::unique_future<std::pair<uint64_t, uint64_t>> manager_disk_t::storage_append(
        execution_context_t ctx,
        std::unique_ptr<components::vector::data_chunk_t> data) {
        auto& name = ctx.name;
        auto& txn = ctx.txn;
        auto* s = get_storage(name);
        if (!s || !data || data->size() == 0) {
            co_return std::make_pair(uint64_t{0}, uint64_t{0});
        }

        // 1. Schema adoption
        if (!s->has_schema() && data->column_count() > 0) {
            s->adopt_schema(data->types());
        }

        // 2. Column expansion — reorder/expand incoming data to match storage columns
        const auto& table_columns = s->columns();
        if (!table_columns.empty() && data->column_count() > 0) {
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
                    if (table_columns[t].has_default_value()) {
                        // Apply DEFAULT value for missing column
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        for (uint64_t row = 0; row < data->size(); row++) {
                            expanded_data.back().set_value(row, table_columns[t].default_value());
                        }
                    } else {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        expanded_data.back().validity().set_all_invalid(data->size());
                    }
                }
            }
            data->data = std::move(expanded_data);
        }

        // 2b. NOT NULL enforcement
        if (!table_columns.empty()) {
            for (size_t col = 0; col < table_columns.size() && col < data->column_count(); col++) {
                if (table_columns[col].is_not_null()) {
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!data->data[col].validity().row_is_valid(row)) {
                            trace(log_, "storage_append: NOT NULL violation on column '{}'",
                                  table_columns[col].name());
                            co_return std::make_pair(uint64_t{0}, uint64_t{0});
                        }
                    }
                }
            }
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

        // 4. Type promotion/conversion (numeric↔numeric, numeric↔string)
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < data->column_count(); i++) {
                auto src_type = data->data[i].type().type();
                auto tgt_type = table_columns[i].type().type();
                if (src_type != tgt_type &&
                    (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                    (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                    auto& src_vec = data->data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, data->size());
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        // 5. Append
        auto actual_count = data->size();
        uint64_t start_row;
        if (txn.transaction_id != 0) {
            start_row = s->append(*data, txn);
        } else {
            start_row = s->append(*data);
        }
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_t::unique_future<std::pair<int64_t, uint64_t>> manager_disk_t::storage_update(
        execution_context_t ctx,
        components::vector::vector_t row_ids,
        std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(ctx.name);
        if (!s) {co_return std::pair<int64_t, uint64_t>{0, 0};}
        co_return s->update(row_ids, *data, ctx.txn);
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_delete_rows(
        execution_context_t ctx,
        components::vector::vector_t row_ids, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (!s) {co_return 0;}
        if (ctx.txn.transaction_id != 0) {
            co_return s->delete_rows(row_ids, count, ctx.txn.transaction_id);
        }
        co_return s->delete_rows(row_ids, count);
    }

    manager_disk_t::unique_future<uint64_t> manager_disk_t::storage_parallel_scan(
        session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {co_return 0;}
        uint64_t total = s->parallel_scan([](components::vector::data_chunk_t&) {});
        co_return total;
    }

    // MVCC commit/revert methods

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_append(
        execution_context_t ctx,
        uint64_t commit_id, int64_t row_start, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (s) s->commit_append(commit_id, row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_revert_append(
        execution_context_t ctx,
        int64_t row_start, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (s) s->revert_append(row_start, count);
        co_return;
    }

    manager_disk_t::unique_future<void> manager_disk_t::storage_commit_delete(
        execution_context_t ctx,
        uint64_t commit_id) {
        auto* s = get_storage(ctx.name);
        if (s) s->commit_all_deletes(ctx.txn.transaction_id, commit_id);
        co_return;
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
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::flush>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::flush, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::checkpoint_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::checkpoint_all, msg);
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
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::create_storage_disk>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::create_storage_disk, msg);
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
            // MVCC commit/revert
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_commit_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_commit_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_revert_append>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_revert_append, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_commit_delete>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_commit_delete, msg);
                break;
            }
            // GC / vacuum
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::vacuum_all>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::vacuum_all, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::maybe_cleanup>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::maybe_cleanup, msg);
                break;
            }
            // Catalog: sequences, views, macros
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::catalog_append_sequence>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::catalog_append_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::catalog_remove_sequence>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::catalog_remove_sequence, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::catalog_append_view>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::catalog_append_view, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::catalog_remove_view>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::catalog_remove_view, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::catalog_append_macro>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::catalog_append_macro, msg);
                break;
            }
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::catalog_remove_macro>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::catalog_remove_macro, msg);
                break;
            }
            // Parallel scan
            case actor_zeta::msg_id<manager_disk_empty_t, &manager_disk_empty_t::storage_parallel_scan>: {
                co_await actor_zeta::dispatch(this, &manager_disk_empty_t::storage_parallel_scan, msg);
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
        if (it == storages_.end()) {return nullptr;}
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
        if (!s) {co_return std::pmr::vector<components::types::complex_logical_type>(resource());}
        co_return s->types();
    }

    manager_disk_empty_t::unique_future<uint64_t>
    manager_disk_empty_t::storage_total_rows(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {co_return 0;}
        co_return s->total_rows();
    }

    manager_disk_empty_t::unique_future<uint64_t>
    manager_disk_empty_t::storage_calculate_size(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {co_return 0;}
        co_return s->calculate_size();
    }

    manager_disk_empty_t::unique_future<std::vector<components::table::column_definition_t>>
    manager_disk_empty_t::storage_columns(session_id_t /*session*/, collection_full_name_t name) {
        auto* s = get_storage(name);
        if (!s) {co_return std::vector<components::table::column_definition_t>{};}
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
        if (!s) {co_return false;}
        co_return s->has_schema();
    }

    manager_disk_empty_t::unique_future<void>
    manager_disk_empty_t::storage_adopt_schema(session_id_t /*session*/,
                                               collection_full_name_t name,
                                               std::pmr::vector<components::types::complex_logical_type> types) {
        auto* s = get_storage(name);
        if (s) {s->adopt_schema(types);}
        co_return;
    }

    manager_disk_empty_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_empty_t::storage_scan(
        session_id_t /*session*/, collection_full_name_t name,
        std::unique_ptr<components::table::table_filter_t> filter, int limit,
        components::table::transaction_data txn) {
        auto* s = get_storage(name);
        if (!s) {co_return nullptr;}
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan(*result, filter.get(), limit, txn);
        co_return std::move(result);
    }

    manager_disk_empty_t::unique_future<std::unique_ptr<components::vector::data_chunk_t>>
    manager_disk_empty_t::storage_fetch(session_id_t /*session*/,
                                        collection_full_name_t name,
                                        components::vector::vector_t row_ids,
                                        uint64_t count) {
        auto* s = get_storage(name);
        if (!s) {co_return nullptr;}
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
        if (!s) {co_return nullptr;}
        auto types = s->types();
        auto result = std::make_unique<components::vector::data_chunk_t>(resource(), types);
        s->scan_segment(start, count, [&result](components::vector::data_chunk_t& chunk) { result->append(chunk); });
        co_return std::move(result);
    }

    manager_disk_empty_t::unique_future<std::pair<uint64_t, uint64_t>> manager_disk_empty_t::storage_append(
        execution_context_t ctx,
        std::unique_ptr<components::vector::data_chunk_t> data) {
        auto& name = ctx.name;
        auto* s = get_storage(name);
        if (!s || !data || data->size() == 0) {co_return std::make_pair(uint64_t{0}, uint64_t{0});}

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
                    if (table_columns[t].has_default_value()) {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        for (uint64_t row = 0; row < data->size(); row++) {
                            expanded_data.back().set_value(row, table_columns[t].default_value());
                        }
                    } else {
                        expanded_data.emplace_back(resource(), full_types[t], data->size());
                        expanded_data.back().validity().set_all_invalid(data->size());
                    }
                }
            }
            data->data = std::move(expanded_data);
        }

        // NOT NULL enforcement
        if (!table_columns.empty()) {
            for (size_t col = 0; col < table_columns.size() && col < data->column_count(); col++) {
                if (table_columns[col].is_not_null()) {
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (!data->data[col].validity().row_is_valid(row)) {
                            co_return std::make_pair(uint64_t{0}, uint64_t{0});
                        }
                    }
                }
            }
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
                    if (keep_rows.empty()) {co_return std::make_pair(uint64_t{0}, uint64_t{0});}
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

        // Type promotion/conversion (numeric↔numeric, numeric↔string)
        if (s->has_schema() && !table_columns.empty()) {
            using components::types::is_numeric;
            using components::types::logical_type;
            for (size_t i = 0; i < table_columns.size() && i < data->column_count(); i++) {
                auto src_type = data->data[i].type().type();
                auto tgt_type = table_columns[i].type().type();
                if (src_type != tgt_type &&
                    (is_numeric(src_type) || src_type == logical_type::STRING_LITERAL) &&
                    (is_numeric(tgt_type) || tgt_type == logical_type::STRING_LITERAL)) {
                    auto& src_vec = data->data[i];
                    auto target_type = table_columns[i].type();
                    if (src_vec.type().has_alias()) {
                        target_type.set_alias(src_vec.type().alias());
                    }
                    components::vector::vector_t casted(resource(), target_type, data->size());
                    for (uint64_t row = 0; row < data->size(); row++) {
                        if (src_vec.validity().row_is_valid(row)) {
                            casted.set_value(row, src_vec.value(row).cast_as(target_type));
                        } else {
                            casted.validity().set_invalid(row);
                        }
                    }
                    data->data[i] = std::move(casted);
                }
            }
        }

        auto actual_count = data->size();
        auto start_row = s->append(*data);
        co_return std::make_pair(start_row, actual_count);
    }

    manager_disk_empty_t::unique_future<std::pair<int64_t, uint64_t>> manager_disk_empty_t::storage_update(
        execution_context_t ctx,
        components::vector::vector_t row_ids,
        std::unique_ptr<components::vector::data_chunk_t> data) {
        auto* s = get_storage(ctx.name);
        if (!s) {co_return std::pair<int64_t, uint64_t>{0, 0};}
        co_return s->update(row_ids, *data, ctx.txn);
    }

    manager_disk_empty_t::unique_future<uint64_t> manager_disk_empty_t::storage_delete_rows(
        execution_context_t ctx,
        components::vector::vector_t row_ids, uint64_t count) {
        auto* s = get_storage(ctx.name);
        if (!s) {co_return 0; }
        co_return s->delete_rows(row_ids, count);
    }

} //namespace services::disk
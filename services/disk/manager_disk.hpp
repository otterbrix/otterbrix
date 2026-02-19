#pragma once

#include "agent_disk.hpp"
#include "result.hpp"
#include "disk_contract.hpp"
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/storage/storage.hpp>
#include <components/storage/table_storage_adapter.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/metadata_manager.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/executor.hpp>
#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/implements.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/mailbox/message.hpp>
#include <actor-zeta/mailbox/make_message.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>
#include <chrono>
#include <thread>
#include <mutex>

namespace services::disk {

    using session_id_t = ::components::session::session_id_t;

    enum class storage_mode_t : uint8_t { IN_MEMORY = 0, DISK = 1 };

    /// Owns data_table_t + its supporting storage infrastructure.
    /// Supports both in-memory (schema-less computing tables) and disk-backed (table.otbx) storage.
    class table_storage_t {
    public:
        /// In-memory mode: computing tables (schema-less)
        explicit table_storage_t(std::pmr::memory_resource* resource);

        /// In-memory mode with explicit columns
        explicit table_storage_t(std::pmr::memory_resource* resource,
                                 std::vector<components::table::column_definition_t> columns);

        /// Disk mode: create new table.otbx
        table_storage_t(std::pmr::memory_resource* resource,
                        std::vector<components::table::column_definition_t> columns,
                        const std::filesystem::path& otbx_path);

        /// Disk mode: load existing table.otbx
        table_storage_t(std::pmr::memory_resource* resource,
                        const std::filesystem::path& otbx_path);

        components::table::data_table_t& table() { return *table_; }
        storage_mode_t mode() const { return mode_; }

        /// Checkpoint (disk mode only, no-op for in-memory)
        void checkpoint();

    private:
        storage_mode_t mode_;
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        std::unique_ptr<components::table::storage::block_manager_t> block_manager_;
        std::unique_ptr<components::table::data_table_t> table_;
    };

    class manager_disk_t final : public actor_zeta::actor::actor_mixin<manager_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t>;

        using run_fn_t = std::function<void()>;

        manager_disk_t(std::pmr::memory_resource*,
                       actor_zeta::scheduler_raw scheduler,
                       actor_zeta::scheduler_raw scheduler_disk,
                       configuration::config_disk config,
                       log_t& log,
                       run_fn_t run_fn = []{ std::this_thread::yield(); });
        ~manager_disk_t();

        void set_run_fn(run_fn_t fn) { run_fn_ = std::move(fn); }

        // Synchronous storage creation for initialization (before schedulers start)
        void create_storage_sync(const collection_full_name_t& name);
        void create_storage_with_columns_sync(const collection_full_name_t& name,
                                               std::vector<components::table::column_definition_t> columns);
        // Disk storage: create new or load existing
        void create_storage_disk_sync(const collection_full_name_t& name,
                                       std::vector<components::table::column_definition_t> columns,
                                       const std::filesystem::path& otbx_path);
        void load_storage_disk_sync(const collection_full_name_t& name,
                                     const std::filesystem::path& otbx_path);

        // Overlay NOT NULL from catalog onto storage column definitions (after WAL replay)
        void overlay_column_not_null_sync(const collection_full_name_t& name,
                                          const std::string& col_name);

        // Synchronous direct replay methods for physical WAL (before schedulers start, no MVCC)
        void direct_append_sync(const collection_full_name_t& name,
                                components::vector::data_chunk_t& data);
        void direct_delete_sync(const collection_full_name_t& name,
                                const std::pmr::vector<int64_t>& row_ids, uint64_t count);
        void direct_update_sync(const collection_full_name_t& name,
                                const std::pmr::vector<int64_t>& row_ids,
                                components::vector::data_chunk_t& new_data);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char* { return "manager_disk"; }

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]]
        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        template<typename ReturnType, typename... Args>
        [[nodiscard]]
        ReturnType enqueue_impl(
            actor_zeta::actor::address_t sender,
            actor_zeta::mailbox::message_id cmd,
            Args&&... args);

        void sync(address_pack pack);

        unique_future<result_load_t> load(session_id_t session);
        unique_future<void> load_indexes(session_id_t session, actor_zeta::address_t dispatcher_address);

        unique_future<void> append_database(session_id_t session, database_name_t database);
        unique_future<void> remove_database(session_id_t session, database_name_t database);

        unique_future<void> append_collection(session_id_t session,
                                              database_name_t database,
                                              collection_name_t collection);
        unique_future<void> remove_collection(session_id_t session,
                                              database_name_t database,
                                              collection_name_t collection);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        unique_future<void> checkpoint_all(session_id_t session);
        unique_future<void> vacuum_all(session_id_t session, uint64_t lowest_active_start_time);
        unique_future<void> maybe_cleanup(execution_context_t ctx,
                                          uint64_t lowest_active_start_time);

        // Catalog DDL (sequences, views, macros)
        unique_future<void> catalog_append_sequence(session_id_t session,
                                                     database_name_t database,
                                                     catalog_sequence_entry_t entry);
        unique_future<void> catalog_remove_sequence(session_id_t session,
                                                     database_name_t database,
                                                     std::string name);
        unique_future<void> catalog_append_view(session_id_t session,
                                                 database_name_t database,
                                                 catalog_view_entry_t entry);
        unique_future<void> catalog_remove_view(session_id_t session,
                                                 database_name_t database,
                                                 std::string name);
        unique_future<void> catalog_append_macro(session_id_t session,
                                                  database_name_t database,
                                                  catalog_macro_entry_t entry);
        unique_future<void> catalog_remove_macro(session_id_t session,
                                                  database_name_t database,
                                                  std::string name);

        // Storage management
        unique_future<void> create_storage(session_id_t session, collection_full_name_t name);
        unique_future<void> create_storage_with_columns(session_id_t session,
                                                         collection_full_name_t name,
                                                         std::vector<components::table::column_definition_t> columns);
        unique_future<void> drop_storage(session_id_t session, collection_full_name_t name);

        // Storage queries
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, collection_full_name_t name);
        unique_future<uint64_t> storage_total_rows(session_id_t session, collection_full_name_t name);
        unique_future<uint64_t> storage_calculate_size(session_id_t session, collection_full_name_t name);
        unique_future<std::vector<components::table::column_definition_t>>
        storage_columns(session_id_t session, collection_full_name_t name);
        unique_future<bool> storage_has_schema(session_id_t session, collection_full_name_t name);
        unique_future<void> storage_adopt_schema(session_id_t session,
                                                  collection_full_name_t name,
                                                  std::pmr::vector<components::types::complex_logical_type> types);

        // Storage data operations
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     collection_full_name_t name,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      collection_full_name_t name,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session,
                             collection_full_name_t name,
                             int64_t start,
                             uint64_t count);
        unique_future<std::pair<uint64_t, uint64_t>> storage_append(execution_context_t ctx,
                                                std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<void> storage_update(execution_context_t ctx,
                                            components::vector::vector_t row_ids,
                                            std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t> storage_delete_rows(execution_context_t ctx,
                                                     components::vector::vector_t row_ids,
                                                     uint64_t count);
        unique_future<uint64_t> storage_parallel_scan(session_id_t session,
                                                       collection_full_name_t name);

        // MVCC commit/revert
        unique_future<void> storage_commit_append(execution_context_t ctx,
                                                   uint64_t commit_id,
                                                   int64_t row_start,
                                                   uint64_t count);
        unique_future<void> storage_revert_append(execution_context_t ctx,
                                                   int64_t row_start,
                                                   uint64_t count);
        unique_future<void> storage_commit_delete(execution_context_t ctx,
                                                   uint64_t commit_id);

        using dispatch_traits = actor_zeta::implements<
            disk_contract,
            &manager_disk_t::load,
            &manager_disk_t::load_indexes,
            &manager_disk_t::append_database,
            &manager_disk_t::remove_database,
            &manager_disk_t::append_collection,
            &manager_disk_t::remove_collection,
            &manager_disk_t::flush,
            &manager_disk_t::checkpoint_all,
            &manager_disk_t::vacuum_all,
            &manager_disk_t::maybe_cleanup,
            // Catalog DDL
            &manager_disk_t::catalog_append_sequence,
            &manager_disk_t::catalog_remove_sequence,
            &manager_disk_t::catalog_append_view,
            &manager_disk_t::catalog_remove_view,
            &manager_disk_t::catalog_append_macro,
            &manager_disk_t::catalog_remove_macro,
            // Storage management
            &manager_disk_t::create_storage,
            &manager_disk_t::create_storage_with_columns,
            &manager_disk_t::drop_storage,
            // Storage queries
            &manager_disk_t::storage_types,
            &manager_disk_t::storage_total_rows,
            &manager_disk_t::storage_calculate_size,
            &manager_disk_t::storage_columns,
            &manager_disk_t::storage_has_schema,
            &manager_disk_t::storage_adopt_schema,
            // Storage data operations
            &manager_disk_t::storage_scan,
            &manager_disk_t::storage_fetch,
            &manager_disk_t::storage_scan_segment,
            &manager_disk_t::storage_append,
            &manager_disk_t::storage_update,
            &manager_disk_t::storage_delete_rows,
            &manager_disk_t::storage_parallel_scan,
            // MVCC commit/revert
            &manager_disk_t::storage_commit_append,
            &manager_disk_t::storage_revert_append,
            &manager_disk_t::storage_commit_delete
        >;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        actor_zeta::scheduler_raw scheduler_disk_;
        run_fn_t run_fn_;
        std::mutex mutex_;

        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        log_t log_;
        core::filesystem::local_file_system_t fs_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        command_storage_t commands_;
        session_id_t load_session_;
        wal::id_t max_wal_id_{0};

        // Storage entries per collection
        struct collection_storage_entry_t {
            table_storage_t table_storage;
            std::unique_ptr<components::storage::storage_t> storage;

            /// In-memory: schema-less
            explicit collection_storage_entry_t(std::pmr::memory_resource* resource)
                : table_storage(resource)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            /// In-memory: with columns
            explicit collection_storage_entry_t(std::pmr::memory_resource* resource,
                                                std::vector<components::table::column_definition_t> columns)
                : table_storage(resource, std::move(columns))
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            /// Disk: create new table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource,
                                       std::vector<components::table::column_definition_t> columns,
                                       const std::filesystem::path& otbx_path)
                : table_storage(resource, std::move(columns), otbx_path)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            /// Disk: load existing table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource,
                                       const std::filesystem::path& otbx_path)
                : table_storage(resource, otbx_path)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}
        };
        std::unordered_map<collection_full_name_t, std::unique_ptr<collection_storage_entry_t>,
                           collection_name_hash> storages_;

        components::storage::storage_t* get_storage(const collection_full_name_t& name);

        void create_agent(int count_agents);
        auto agent() -> actor_zeta::address_t;

        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<result_load_t>> pending_load_;
        std::pmr::vector<unique_future<std::pmr::vector<size_t>>> pending_find_;

        void poll_pending();

        bool is_polling_{false};

        actor_zeta::behavior_t current_behavior_;
    };

    template<typename ReturnType, typename... Args>
    ReturnType manager_disk_t::enqueue_impl(
        actor_zeta::actor::address_t sender,
        actor_zeta::mailbox::message_id cmd,
        Args&&... args) {

        static_assert(actor_zeta::type_traits::is_unique_future_v<ReturnType>,
                      "ReturnType must be unique_future<T>");
        using R = typename actor_zeta::type_traits::is_unique_future<ReturnType>::value_type;

        auto [msg, future] = actor_zeta::detail::make_message<R>(
            resource(),
            std::move(sender),
            cmd,
            std::forward<Args>(args)...);

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

        return std::move(future);
    }

    class manager_disk_empty_t final : public actor_zeta::actor::actor_mixin<manager_disk_empty_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t>;

        manager_disk_empty_t(std::pmr::memory_resource*, actor_zeta::scheduler::sharing_scheduler*);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        void sync(address_pack pack);

        unique_future<result_load_t> load(session_id_t session);
        unique_future<void> load_indexes(session_id_t session, actor_zeta::address_t dispatcher_address);

        unique_future<void> append_database(session_id_t session, database_name_t database);
        unique_future<void> remove_database(session_id_t session, database_name_t database);

        unique_future<void> append_collection(session_id_t session,
                                              database_name_t database,
                                              collection_name_t collection);
        unique_future<void> remove_collection(session_id_t session,
                                              database_name_t database,
                                              collection_name_t collection);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        unique_future<void> checkpoint_all(session_id_t /*session*/) { co_return; }
        unique_future<void> vacuum_all(session_id_t /*session*/, uint64_t /*lowest_active_start_time*/) { co_return; }
        unique_future<void> maybe_cleanup(execution_context_t /*ctx*/,
                                          uint64_t /*lowest_active_start_time*/) { co_return; }

        // Catalog DDL (no-ops for empty manager)
        unique_future<void> catalog_append_sequence(session_id_t /*session*/,
                                                     database_name_t /*database*/,
                                                     catalog_sequence_entry_t /*entry*/) { co_return; }
        unique_future<void> catalog_remove_sequence(session_id_t /*session*/,
                                                     database_name_t /*database*/,
                                                     std::string /*name*/) { co_return; }
        unique_future<void> catalog_append_view(session_id_t /*session*/,
                                                 database_name_t /*database*/,
                                                 catalog_view_entry_t /*entry*/) { co_return; }
        unique_future<void> catalog_remove_view(session_id_t /*session*/,
                                                 database_name_t /*database*/,
                                                 std::string /*name*/) { co_return; }
        unique_future<void> catalog_append_macro(session_id_t /*session*/,
                                                  database_name_t /*database*/,
                                                  catalog_macro_entry_t /*entry*/) { co_return; }
        unique_future<void> catalog_remove_macro(session_id_t /*session*/,
                                                  database_name_t /*database*/,
                                                  std::string /*name*/) { co_return; }

        // Storage management
        unique_future<void> create_storage(session_id_t session, collection_full_name_t name);
        unique_future<void> create_storage_with_columns(session_id_t session,
                                                         collection_full_name_t name,
                                                         std::vector<components::table::column_definition_t> columns);
        unique_future<void> drop_storage(session_id_t session, collection_full_name_t name);

        // Storage queries
        unique_future<std::pmr::vector<components::types::complex_logical_type>>
        storage_types(session_id_t session, collection_full_name_t name);
        unique_future<uint64_t> storage_total_rows(session_id_t session, collection_full_name_t name);
        unique_future<uint64_t> storage_calculate_size(session_id_t session, collection_full_name_t name);
        unique_future<std::vector<components::table::column_definition_t>>
        storage_columns(session_id_t session, collection_full_name_t name);
        unique_future<bool> storage_has_schema(session_id_t session, collection_full_name_t name);
        unique_future<void> storage_adopt_schema(session_id_t session,
                                                  collection_full_name_t name,
                                                  std::pmr::vector<components::types::complex_logical_type> types);

        // Storage data operations
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan(session_id_t session,
                     collection_full_name_t name,
                     std::unique_ptr<components::table::table_filter_t> filter,
                     int limit);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_fetch(session_id_t session,
                      collection_full_name_t name,
                      components::vector::vector_t row_ids,
                      uint64_t count);
        unique_future<std::unique_ptr<components::vector::data_chunk_t>>
        storage_scan_segment(session_id_t session,
                             collection_full_name_t name,
                             int64_t start,
                             uint64_t count);
        unique_future<std::pair<uint64_t, uint64_t>> storage_append(execution_context_t ctx,
                                                std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<void> storage_update(execution_context_t ctx,
                                            components::vector::vector_t row_ids,
                                            std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t> storage_delete_rows(execution_context_t ctx,
                                                     components::vector::vector_t row_ids,
                                                     uint64_t count);
        unique_future<uint64_t> storage_parallel_scan(session_id_t /*session*/,
                                                       collection_full_name_t /*name*/) { co_return 0; }

        // MVCC commit/revert
        unique_future<void> storage_commit_append(execution_context_t /*ctx*/,
                                                   uint64_t /*commit_id*/,
                                                   int64_t /*row_start*/,
                                                   uint64_t /*count*/) { co_return; }
        unique_future<void> storage_revert_append(execution_context_t /*ctx*/,
                                                   int64_t /*row_start*/,
                                                   uint64_t /*count*/) { co_return; }
        unique_future<void> storage_commit_delete(execution_context_t /*ctx*/,
                                                   uint64_t /*commit_id*/) { co_return; }

        using dispatch_traits = actor_zeta::implements<
            disk_contract,
            &manager_disk_empty_t::load,
            &manager_disk_empty_t::load_indexes,
            &manager_disk_empty_t::append_database,
            &manager_disk_empty_t::remove_database,
            &manager_disk_empty_t::append_collection,
            &manager_disk_empty_t::remove_collection,
            &manager_disk_empty_t::flush,
            &manager_disk_empty_t::checkpoint_all,
            &manager_disk_empty_t::vacuum_all,
            &manager_disk_empty_t::maybe_cleanup,
            // Catalog DDL
            &manager_disk_empty_t::catalog_append_sequence,
            &manager_disk_empty_t::catalog_remove_sequence,
            &manager_disk_empty_t::catalog_append_view,
            &manager_disk_empty_t::catalog_remove_view,
            &manager_disk_empty_t::catalog_append_macro,
            &manager_disk_empty_t::catalog_remove_macro,
            // Storage management
            &manager_disk_empty_t::create_storage,
            &manager_disk_empty_t::create_storage_with_columns,
            &manager_disk_empty_t::drop_storage,
            // Storage queries
            &manager_disk_empty_t::storage_types,
            &manager_disk_empty_t::storage_total_rows,
            &manager_disk_empty_t::storage_calculate_size,
            &manager_disk_empty_t::storage_columns,
            &manager_disk_empty_t::storage_has_schema,
            &manager_disk_empty_t::storage_adopt_schema,
            // Storage data operations
            &manager_disk_empty_t::storage_scan,
            &manager_disk_empty_t::storage_fetch,
            &manager_disk_empty_t::storage_scan_segment,
            &manager_disk_empty_t::storage_append,
            &manager_disk_empty_t::storage_update,
            &manager_disk_empty_t::storage_delete_rows,
            &manager_disk_empty_t::storage_parallel_scan,
            // MVCC commit/revert
            &manager_disk_empty_t::storage_commit_append,
            &manager_disk_empty_t::storage_revert_append,
            &manager_disk_empty_t::storage_commit_delete
        >;

    private:
        void create_agent(int count_agents);

        components::storage::storage_t* get_storage(const collection_full_name_t& name);

        struct collection_storage_entry_t {
            table_storage_t table_storage;
            std::unique_ptr<components::storage::storage_t> storage;

            /// In-memory: schema-less
            explicit collection_storage_entry_t(std::pmr::memory_resource* resource)
                : table_storage(resource)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            /// In-memory: with columns
            explicit collection_storage_entry_t(std::pmr::memory_resource* resource,
                                                std::vector<components::table::column_definition_t> columns)
                : table_storage(resource, std::move(columns))
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            /// Disk: create new table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource,
                                       std::vector<components::table::column_definition_t> columns,
                                       const std::filesystem::path& otbx_path)
                : table_storage(resource, std::move(columns), otbx_path)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            /// Disk: load existing table.otbx
            collection_storage_entry_t(std::pmr::memory_resource* resource,
                                       const std::filesystem::path& otbx_path)
                : table_storage(resource, otbx_path)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}
        };
        std::unordered_map<collection_full_name_t, std::unique_ptr<collection_storage_entry_t>,
                           collection_name_hash> storages_;

        std::pmr::memory_resource* resource_;
        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<result_load_t>> pending_load_;
        std::pmr::vector<unique_future<std::pmr::vector<size_t>>> pending_find_;
    };

} //namespace services::disk
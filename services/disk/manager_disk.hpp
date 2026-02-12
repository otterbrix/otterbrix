#pragma once

#include "agent_disk.hpp"
#include "index_agent_disk.hpp"
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

namespace services::disk {

    using session_id_t = ::components::session::session_id_t;
    using document_ids_t = components::operators::operator_write_data_t::ids_t;

    /// Owns data_table_t + its supporting storage infrastructure.
    /// Moved here from context_collection_t as part of storage separation.
    class table_storage_t {
    public:
        explicit table_storage_t(std::pmr::memory_resource* resource)
            : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(
                  resource,
                  block_manager_,
                  std::vector<components::table::column_definition_t>{})) {}

        explicit table_storage_t(std::pmr::memory_resource* resource,
                                 std::vector<components::table::column_definition_t> columns)
            : buffer_pool_(resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager_(resource, fs_, buffer_pool_)
            , block_manager_(buffer_manager_, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE)
            , table_(std::make_unique<components::table::data_table_t>(resource, block_manager_, std::move(columns))) {}

        components::table::data_table_t& table() { return *table_; }

    private:
        core::filesystem::local_file_system_t fs_;
        components::table::storage::buffer_pool_t buffer_pool_;
        components::table::storage::standard_buffer_manager_t buffer_manager_;
        components::table::storage::in_memory_block_manager_t block_manager_;
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

        unique_future<void> write_data_chunk(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<void> remove_documents(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             document_ids_t documents);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

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
        unique_future<uint64_t> storage_append(session_id_t session,
                                                collection_full_name_t name,
                                                std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<void> storage_update(session_id_t session,
                                            collection_full_name_t name,
                                            components::vector::vector_t row_ids,
                                            std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t> storage_delete_rows(session_id_t session,
                                                     collection_full_name_t name,
                                                     components::vector::vector_t row_ids,
                                                     uint64_t count);

        using dispatch_traits = actor_zeta::implements<
            disk_contract,
            &manager_disk_t::load,
            &manager_disk_t::load_indexes,
            &manager_disk_t::append_database,
            &manager_disk_t::remove_database,
            &manager_disk_t::append_collection,
            &manager_disk_t::remove_collection,
            &manager_disk_t::write_data_chunk,
            &manager_disk_t::remove_documents,
            &manager_disk_t::flush,
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
            &manager_disk_t::storage_delete_rows
        >;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        actor_zeta::scheduler_raw scheduler_disk_;
        run_fn_t run_fn_;
        spin_lock lock_;

        actor_zeta::address_t manager_wal_ = actor_zeta::address_t::empty_address();
        log_t log_;
        core::filesystem::local_file_system_t fs_;
        configuration::config_disk config_;
        std::vector<agent_disk_ptr> agents_;
        command_storage_t commands_;
        session_id_t load_session_;

        // Storage entries per collection
        struct collection_storage_entry_t {
            table_storage_t table_storage;
            std::unique_ptr<components::storage::storage_t> storage;

            explicit collection_storage_entry_t(std::pmr::memory_resource* resource)
                : table_storage(resource)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            explicit collection_storage_entry_t(std::pmr::memory_resource* resource,
                                                std::vector<components::table::column_definition_t> columns)
                : table_storage(resource, std::move(columns))
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
        std::pmr::vector<unique_future<index_disk_t::result>> pending_find_;

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

        std::lock_guard<spin_lock> guard(lock_);
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

        unique_future<void> write_data_chunk(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<void> remove_documents(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             document_ids_t documents);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

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
        unique_future<uint64_t> storage_append(session_id_t session,
                                                collection_full_name_t name,
                                                std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<void> storage_update(session_id_t session,
                                            collection_full_name_t name,
                                            components::vector::vector_t row_ids,
                                            std::unique_ptr<components::vector::data_chunk_t> data);
        unique_future<uint64_t> storage_delete_rows(session_id_t session,
                                                     collection_full_name_t name,
                                                     components::vector::vector_t row_ids,
                                                     uint64_t count);

        using dispatch_traits = actor_zeta::implements<
            disk_contract,
            &manager_disk_empty_t::load,
            &manager_disk_empty_t::load_indexes,
            &manager_disk_empty_t::append_database,
            &manager_disk_empty_t::remove_database,
            &manager_disk_empty_t::append_collection,
            &manager_disk_empty_t::remove_collection,
            &manager_disk_empty_t::write_data_chunk,
            &manager_disk_empty_t::remove_documents,
            &manager_disk_empty_t::flush,
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
            &manager_disk_empty_t::storage_delete_rows
        >;

    private:
        void create_agent(int count_agents);

        components::storage::storage_t* get_storage(const collection_full_name_t& name);

        struct collection_storage_entry_t {
            table_storage_t table_storage;
            std::unique_ptr<components::storage::storage_t> storage;

            explicit collection_storage_entry_t(std::pmr::memory_resource* resource)
                : table_storage(resource)
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}

            explicit collection_storage_entry_t(std::pmr::memory_resource* resource,
                                                std::vector<components::table::column_definition_t> columns)
                : table_storage(resource, std::move(columns))
                , storage(std::make_unique<components::storage::table_storage_adapter_t>(
                      table_storage.table(), resource)) {}
        };
        std::unordered_map<collection_full_name_t, std::unique_ptr<collection_storage_entry_t>,
                           collection_name_hash> storages_;

        std::pmr::memory_resource* resource_;
        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<result_load_t>> pending_load_;
        std::pmr::vector<unique_future<index_disk_t::result>> pending_find_;
    };

} //namespace services::disk
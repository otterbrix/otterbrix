#pragma once

#include "agent_disk.hpp"
#include "index_agent_disk.hpp"
#include "result.hpp"
#include "disk_contract.hpp"
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <components/physical_plan/base/operators/operator_write_data.hpp>
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

namespace services::collection {
    class context_collection_t;
}

namespace services::disk {

    using session_id_t = ::components::session::session_id_t;
    using document_ids_t = components::base::operators::operator_write_data_t::ids_t;

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

        unique_future<void> write_documents(session_id_t session,
                                            database_name_t database,
                                            collection_name_t collection,
                                            std::pmr::vector<document_ptr> documents);

        unique_future<void> remove_documents(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             document_ids_t documents);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        unique_future<actor_zeta::address_t> create_index_agent(session_id_t session,
                                               components::logical_plan::node_create_index_ptr index,
                                               services::collection::context_collection_t* collection);
        unique_future<void> drop_index_agent(session_id_t session,
                                             index_name_t index_name,
                                             services::collection::context_collection_t* collection);
        unique_future<void> drop_index_agent_success(session_id_t session);
        unique_future<void> index_insert_many(session_id_t session,
                                              index_name_t index_name,
                                              std::vector<std::pair<components::document::value_t, components::document::document_id_t>> values);
        unique_future<void> index_insert(session_id_t session,
                                         index_name_t index_name,
                                         components::types::logical_value_t key,
                                         components::document::document_id_t doc_id);
        unique_future<void> index_remove(session_id_t session,
                                         index_name_t index_name,
                                         components::types::logical_value_t key,
                                         components::document::document_id_t doc_id);

        unique_future<void> index_insert_by_agent(session_id_t session,
                                                  actor_zeta::address_t agent_address,
                                                  components::types::logical_value_t key,
                                                  components::document::document_id_t doc_id);
        unique_future<void> index_remove_by_agent(session_id_t session,
                                                  actor_zeta::address_t agent_address,
                                                  components::types::logical_value_t key,
                                                  components::document::document_id_t doc_id);
        unique_future<index_disk_t::result> index_find_by_agent(session_id_t session,
                                                                 actor_zeta::address_t agent_address,
                                                                 components::types::logical_value_t key,
                                                                 components::expressions::compare_type compare);

        using dispatch_traits = actor_zeta::implements<
            disk_contract,
            &manager_disk_t::load,
            &manager_disk_t::load_indexes,
            &manager_disk_t::append_database,
            &manager_disk_t::remove_database,
            &manager_disk_t::append_collection,
            &manager_disk_t::remove_collection,
            &manager_disk_t::write_documents,
            &manager_disk_t::remove_documents,
            &manager_disk_t::flush,
            &manager_disk_t::create_index_agent,
            &manager_disk_t::drop_index_agent,
            &manager_disk_t::drop_index_agent_success,
            &manager_disk_t::index_insert_many,
            &manager_disk_t::index_insert,
            &manager_disk_t::index_remove,
            &manager_disk_t::index_insert_by_agent,
            &manager_disk_t::index_remove_by_agent,
            &manager_disk_t::index_find_by_agent
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
        index_agent_disk_storage_t index_agents_;
        command_storage_t commands_;
        file_ptr metafile_indexes_;
        session_id_t load_session_;

        struct removed_index_t {
            std::size_t size;
            command_t command;
        };
        std::pmr::unordered_map<session_id_t, removed_index_t> removed_indexes_;

        void create_agent(int count_agents);
        auto agent() -> actor_zeta::address_t;
        void write_index_impl(const components::logical_plan::node_create_index_ptr& index);
        unique_future<void> load_indexes_impl(session_id_t session, actor_zeta::address_t dispatcher_address);
        std::vector<components::logical_plan::node_create_index_ptr>
        read_indexes_impl(const collection_name_t& collection_name) const;
        std::vector<components::logical_plan::node_create_index_ptr> read_indexes_impl() const;
        void remove_index_impl(const index_name_t& index_name);
        void remove_all_indexes_from_collection_impl(const collection_name_t& collection_name);

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

        unique_future<void> write_documents(session_id_t session,
                                            database_name_t database,
                                            collection_name_t collection,
                                            std::pmr::vector<document_ptr> documents);

        unique_future<void> remove_documents(session_id_t session,
                                             database_name_t database,
                                             collection_name_t collection,
                                             document_ids_t documents);

        unique_future<void> flush(session_id_t session, wal::id_t wal_id);

        unique_future<actor_zeta::address_t> create_index_agent(session_id_t session,
                                               components::logical_plan::node_create_index_ptr index,
                                               services::collection::context_collection_t* collection);
        unique_future<void> drop_index_agent(session_id_t session,
                                             index_name_t index_name,
                                             services::collection::context_collection_t* collection);
        unique_future<void> drop_index_agent_success(session_id_t session);

        unique_future<void> index_insert_many(session_id_t session,
                                              index_name_t index_name,
                                              std::vector<std::pair<components::document::value_t, components::document::document_id_t>> values);
        unique_future<void> index_insert(session_id_t session,
                                         index_name_t index_name,
                                         components::types::logical_value_t key,
                                         components::document::document_id_t doc_id);
        unique_future<void> index_remove(session_id_t session,
                                         index_name_t index_name,
                                         components::types::logical_value_t key,
                                         components::document::document_id_t doc_id);

        unique_future<void> index_insert_by_agent(session_id_t session,
                                                  actor_zeta::address_t agent_address,
                                                  components::types::logical_value_t key,
                                                  components::document::document_id_t doc_id);
        unique_future<void> index_remove_by_agent(session_id_t session,
                                                  actor_zeta::address_t agent_address,
                                                  components::types::logical_value_t key,
                                                  components::document::document_id_t doc_id);
        unique_future<index_disk_t::result> index_find_by_agent(session_id_t session,
                                                                 actor_zeta::address_t agent_address,
                                                                 components::types::logical_value_t key,
                                                                 components::expressions::compare_type compare);

        using dispatch_traits = actor_zeta::implements<
            disk_contract,
            &manager_disk_empty_t::load,
            &manager_disk_empty_t::load_indexes,
            &manager_disk_empty_t::append_database,
            &manager_disk_empty_t::remove_database,
            &manager_disk_empty_t::append_collection,
            &manager_disk_empty_t::remove_collection,
            &manager_disk_empty_t::write_documents,
            &manager_disk_empty_t::remove_documents,
            &manager_disk_empty_t::flush,
            &manager_disk_empty_t::create_index_agent,
            &manager_disk_empty_t::drop_index_agent,
            &manager_disk_empty_t::drop_index_agent_success,
            &manager_disk_empty_t::index_insert_many,
            &manager_disk_empty_t::index_insert,
            &manager_disk_empty_t::index_remove,
            &manager_disk_empty_t::index_insert_by_agent,
            &manager_disk_empty_t::index_remove_by_agent,
            &manager_disk_empty_t::index_find_by_agent
        >;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler::sharing_scheduler* scheduler_;
        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<result_load_t>> pending_load_;
        std::pmr::vector<unique_future<index_disk_t::result>> pending_find_;
    };

} //namespace services::disk
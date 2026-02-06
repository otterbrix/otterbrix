#pragma once

#include <unordered_map>
#include <variant>
#include <functional>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include <core/executor.hpp>
#include <core/spinlock/spinlock.hpp>

#include <components/catalog/catalog.hpp>
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/base/operators/operator_write_data.hpp>
#include <services/disk/result.hpp>
#include <services/disk/disk_contract.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_contract.hpp>
#include <services/collection/executor.hpp>
#include <services/collection/context_storage.hpp>
#include <services/loader/loaded_state.hpp>
#include <core/btree/btree.hpp>

namespace services::dispatcher {

    class manager_dispatcher_t final : public actor_zeta::actor::actor_mixin<manager_dispatcher_t> {
        using database_storage_t = std::pmr::set<database_name_t>;
        using collection_storage_t =
            core::pmr::btree::btree_t<collection_full_name_t, std::unique_ptr<services::collection::context_collection_t>>;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using recomputed_types = components::base::operators::operator_write_data_t::updated_types_map_t;

        using sync_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;

        using run_fn_t = std::function<void()>;

        manager_dispatcher_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, log_t& log,
                             run_fn_t run_fn = []{ std::this_thread::yield(); });
        ~manager_dispatcher_t();

        void set_run_fn(run_fn_t fn) { run_fn_ = std::move(fn); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]]
        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(sync_pack pack);

        void init_from_state(
            std::pmr::set<database_name_t> databases,
            loader::document_map_t documents,
            loader::schema_map_t schemas);

        components::catalog::catalog& mutable_catalog() { return catalog_; }

        unique_future<components::cursor::cursor_t_ptr> execute_plan(
            components::session::session_id_t session,
            components::logical_plan::node_ptr plan,
            components::logical_plan::parameter_node_ptr params);
        unique_future<size_t> size(components::session::session_id_t session,
                                   std::string database_name,
                                   std::string collection);
        unique_future<components::cursor::cursor_t_ptr> get_schema(
            components::session::session_id_t session,
            std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids);
        unique_future<void> close_cursor(components::session::session_id_t session);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &manager_dispatcher_t::execute_plan,
            &manager_dispatcher_t::size,
            &manager_dispatcher_t::get_schema,
            &manager_dispatcher_t::close_cursor
        >;

        const components::catalog::catalog& current_catalog() const { return catalog_; }

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        run_fn_t run_fn_;
        components::catalog::catalog catalog_;

        database_storage_t databases_;
        collection_storage_t collections_;
        services::collection::executor::executor_ptr executor_{nullptr,
                                                               actor_zeta::pmr::deleter_t(std::pmr::null_memory_resource())};
        actor_zeta::address_t executor_address_{actor_zeta::address_t::empty_address()};

        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();

        spin_lock lock_;

        std::unordered_map<components::session::session_id_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;

        recomputed_types update_result_;

        components::cursor::cursor_t_ptr check_namespace_exists(const components::catalog::table_id id) const;
        components::cursor::cursor_t_ptr check_collection_exists(const components::catalog::table_id id) const;
        components::cursor::cursor_t_ptr check_type_exists(const std::string& alias) const;
        components::cursor::cursor_t_ptr
        check_collections_format_(components::logical_plan::node_ptr& logical_plan) const;

        components::logical_plan::node_ptr create_logic_plan(components::logical_plan::node_ptr plan);
        void update_catalog(components::logical_plan::node_ptr node);

        services::collection::executor::execute_result_t create_database_(components::logical_plan::node_ptr logical_plan);
        services::collection::executor::execute_result_t drop_database_(components::logical_plan::node_ptr logical_plan);
        services::collection::executor::execute_result_t create_collection_(components::logical_plan::node_ptr logical_plan);
        services::collection::executor::execute_result_t drop_collection_(components::logical_plan::node_ptr logical_plan);

        unique_future<services::collection::executor::execute_result_t> execute_plan_impl(
            components::session::session_id_t session,
            components::logical_plan::node_ptr logical_plan,
            components::logical_plan::storage_parameters parameters,
            components::catalog::used_format_t used_format);

        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<components::cursor::cursor_t_ptr>> pending_cursor_;
        std::pmr::vector<unique_future<size_t>> pending_size_;
        std::pmr::vector<unique_future<services::collection::executor::execute_result_t>> pending_execute_;

        void poll_pending();

        actor_zeta::behavior_t current_behavior_;
    };

} // namespace services::dispatcher
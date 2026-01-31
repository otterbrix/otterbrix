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

namespace services::dispatcher {

    class manager_dispatcher_t;

    class dispatcher_t final : public actor_zeta::basic_actor<dispatcher_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using recomputed_types = components::base::operators::operator_write_data_t::updated_types_map_t;
        
        dispatcher_t(std::pmr::memory_resource* resource,
                     actor_zeta::address_t manager_dispatcher,
                     actor_zeta::address_t memory_storage,
                     actor_zeta::address_t wal_address,
                     actor_zeta::address_t disk_address,
                     log_t& log);
        ~dispatcher_t();

        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        unique_future<void> load(components::session::session_id_t session);

        unique_future<components::cursor::cursor_t_ptr> execute_plan(
            components::session::session_id_t session,
            components::logical_plan::node_ptr plan,
            components::logical_plan::parameter_node_ptr params);

        unique_future<size_t> size(components::session::session_id_t session,
                                   std::string database_name,
                                   std::string collection);

        unique_future<void> close_cursor(components::session::session_id_t session);

        using dispatch_traits = actor_zeta::dispatch_traits<
            &dispatcher_t::load,
            &dispatcher_t::execute_plan,
            &dispatcher_t::size,
            &dispatcher_t::close_cursor
        >;

        const components::catalog::catalog& current_catalog();

    private:
        log_t log_;
        components::catalog::catalog catalog_;
        actor_zeta::address_t manager_dispatcher_;
        actor_zeta::address_t memory_storage_;
        // Addresses for WAL and Disk - polymorphic dispatch via interface contracts
        actor_zeta::address_t wal_address_;
        actor_zeta::address_t disk_address_;

        // Cursor storage
        std::unordered_map<components::session::session_id_t, std::unique_ptr<components::cursor::cursor_t>> cursor_;

        // Update result for delete operations
        recomputed_types update_result_;

        // WAL replay state
        components::session::session_id_t load_session_;
        services::wal::id_t last_wal_id_{0};
        std::size_t load_count_answers_{0};

        // Helper methods
        components::cursor::cursor_t_ptr check_namespace_exists(const components::catalog::table_id id) const;
        components::cursor::cursor_t_ptr check_collection_exists(const components::catalog::table_id id) const;
        components::cursor::cursor_t_ptr check_type_exists(const std::string& alias) const;
        components::cursor::cursor_t_ptr
        check_collections_format_(components::logical_plan::node_ptr& logical_plan) const;

        components::logical_plan::node_ptr create_logic_plan(components::logical_plan::node_ptr plan);
        void update_catalog(components::logical_plan::node_ptr node);

        // Pending coroutines storage (CRITICAL per documentation!)
        // Coroutines with co_await MUST be stored, otherwise refcount underflow
        std::vector<unique_future<void>> pending_void_;
        std::vector<unique_future<components::cursor::cursor_t_ptr>> pending_cursor_;
        std::vector<unique_future<size_t>> pending_size_;

        // Poll and clean up completed coroutines
        void poll_pending();
    };

    using dispatcher_ptr = std::unique_ptr<dispatcher_t, actor_zeta::pmr::deleter_t>;

    class manager_dispatcher_t final : public actor_zeta::actor::actor_mixin<manager_dispatcher_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // Sync pack with addresses - polymorphic dispatch via interface contracts
        using sync_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t>;

        manager_dispatcher_t(std::pmr::memory_resource*, actor_zeta::scheduler_raw, log_t& log);
        ~manager_dispatcher_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        // Custom enqueue_impl for SYNC actor with coroutine behavior()
        // Hides actor_mixin::enqueue_impl - stores behavior and spin-waits until done
        [[nodiscard]]
        std::pair<bool, actor_zeta::detail::enqueue_result> enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        // Sync methods - called directly after constructor, before message processing
        void sync(sync_pack pack);
        unique_future<void> create(components::session::session_id_t session);
        unique_future<void> load(components::session::session_id_t session);
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

        // dispatch_traits must be defined AFTER all method declarations
        // Note: sync is NOT in dispatch_traits - called directly
        using dispatch_traits = actor_zeta::dispatch_traits<
            &manager_dispatcher_t::create,
            &manager_dispatcher_t::load,
            &manager_dispatcher_t::execute_plan,
            &manager_dispatcher_t::size,
            &manager_dispatcher_t::get_schema,
            &manager_dispatcher_t::close_cursor
        >;

        const components::catalog::catalog& current_catalog();

        void create_dispatcher() {
            auto ptr = actor_zeta::spawn<dispatcher_t>(resource(), address(), memory_storage_, wal_address_, disk_address_, log_);
            dispatchers_.emplace_back(std::move(ptr));
        }

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;

        actor_zeta::address_t memory_storage_ = actor_zeta::address_t::empty_address();
        // Addresses for WAL and Disk - polymorphic dispatch via interface contracts
        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        std::vector<dispatcher_ptr> dispatchers_;
        spin_lock lock_;

        // Pending coroutines storage (CRITICAL per documentation!)
        std::vector<unique_future<void>> pending_void_;
        std::vector<unique_future<components::cursor::cursor_t_ptr>> pending_cursor_;
        std::vector<unique_future<size_t>> pending_size_;

        void poll_pending();
        auto dispatcher() -> actor_zeta::address_t;

        // Stored behavior coroutine for SYNC actor polling
        actor_zeta::behavior_t current_behavior_;
    };

} // namespace services::dispatcher
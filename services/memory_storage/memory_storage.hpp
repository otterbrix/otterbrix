#pragma once

#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/base/operators/operator.hpp>
#include <components/session/session.hpp>
#include <core/btree/btree.hpp>
#include <core/executor.hpp>
#include <core/spinlock/spinlock.hpp>
#include <memory_resource>
#include <services/collection/executor.hpp>
#include <services/disk/result.hpp>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>

namespace services {

    class memory_storage_t final : public actor_zeta::actor::actor_mixin<memory_storage_t> {
        struct load_buffer_t {
            std::pmr::vector<collection_full_name_t> collections;

            explicit load_buffer_t(std::pmr::memory_resource* resource);
        };

        using database_storage_t = std::pmr::set<database_name_t>;
        using collection_storage_t =
            core::pmr::btree::btree_t<collection_full_name_t, std::unique_ptr<collection::context_collection_t>>;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using address_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t>;
        enum class unpack_rules : uint64_t
        {
            manager_dispatcher = 0,
            manager_disk = 1
        };

        memory_storage_t(std::pmr::memory_resource* resource, actor_zeta::scheduler_raw scheduler, log_t& log);
        ~memory_storage_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        void behavior(actor_zeta::mailbox::message* msg);

        // Sync methods - called directly after constructor, before message processing
        void sync(address_pack pack);

        // execute_plan() returns cursor via future (NOT via callback!)
        // Uses co_await on executor_t::execute_plan()
        unique_future<collection::executor::execute_result_t> execute_plan(
            components::session::session_id_t session,
            components::logical_plan::node_ptr logical_plan,
            components::logical_plan::storage_parameters parameters,
            components::catalog::used_format_t used_format);

        // size() returns size_t via future (not callback!)
        unique_future<size_t> size(components::session::session_id_t session, collection_full_name_t name);
        unique_future<void> close_cursor(components::session::session_id_t session,
                                         std::set<collection_full_name_t> collections);
        unique_future<void> load(components::session::session_id_t session, disk::result_load_t result);

        // dispatch_traits must be defined AFTER all method declarations
        // dispatch_traits - only methods called externally
        // Note: sync is NOT in dispatch_traits - called directly
        using dispatch_traits = actor_zeta::dispatch_traits<
            &memory_storage_t::load,
            &memory_storage_t::execute_plan,
            &memory_storage_t::size,
            &memory_storage_t::close_cursor
        >;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        database_storage_t databases_;
        collection_storage_t collections_;
        log_t log_;

        actor_zeta::address_t manager_dispatcher_{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t manager_disk_{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t executor_address_{actor_zeta::address_t::empty_address()};

        std::unique_ptr<load_buffer_t> load_buffer_;
        spin_lock lock_;
        collection::executor::executor_ptr executor_{nullptr,
                                                     actor_zeta::pmr::deleter_t(std::pmr::null_memory_resource())};

    private:
        // Helper methods return cursor directly (not via callback)
        collection::executor::execute_result_t create_database_(components::logical_plan::node_ptr logical_plan);
        collection::executor::execute_result_t drop_database_(components::logical_plan::node_ptr logical_plan);
        collection::executor::execute_result_t create_collection_(components::logical_plan::node_ptr logical_plan);
        collection::executor::execute_result_t drop_collection_(components::logical_plan::node_ptr logical_plan);

        // execute_plan_impl - now coroutine with co_await on executor
        unique_future<collection::executor::execute_result_t> execute_plan_impl(
            components::session::session_id_t session,
            components::logical_plan::node_ptr logical_plan,
            components::logical_plan::storage_parameters parameters,
            components::catalog::used_format_t used_format);

        // Pending coroutines storage (CRITICAL per documentation!)
        // Coroutines with co_await MUST be stored, otherwise refcount underflow
        std::vector<unique_future<void>> pending_void_;
        std::vector<unique_future<collection::executor::execute_result_t>> pending_execute_;
        std::vector<unique_future<size_t>> pending_size_;

        // Poll and clean up completed coroutines
        void poll_pending();
    };

} // namespace services

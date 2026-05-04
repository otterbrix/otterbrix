#pragma once

#include <functional>
#include <unordered_map>
#include <variant>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/behavior_t.hpp>
#include <actor-zeta/detail/future.hpp>
#include <actor-zeta/detail/queue/enqueue_result.hpp>

#include <core/executor.hpp>
#include <mutex>

#include <components/compute/function.hpp>
#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator_write_data.hpp>
#include <components/table/transaction_manager.hpp>
#include <services/collection/context_storage.hpp>
#include <services/collection/executor.hpp>
#include <services/disk/disk_contract.hpp>
#include <services/dispatcher/versioned_plan_cache.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_contract.hpp>

namespace services::dispatcher {

    class catalog_view_t;

    class manager_dispatcher_t final : public actor_zeta::actor::actor_mixin<manager_dispatcher_t> {
        using collection_storage_t = std::pmr::set<collection_full_name_t>;

    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using sync_pack = std::tuple<actor_zeta::address_t, actor_zeta::address_t, actor_zeta::address_t>;

        using run_fn_t = std::function<void()>;

        manager_dispatcher_t(
            std::pmr::memory_resource*,
            actor_zeta::scheduler_raw,
            log_t& log,
            run_fn_t run_fn = [] { std::this_thread::yield(); });
        ~manager_dispatcher_t();

        void set_run_fn(run_fn_t fn) { run_fn_ = std::move(fn); }

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        void sync(sync_pack pack);

        void init_from_state(std::pmr::set<collection_full_name_t> collections);

        unique_future<components::cursor::cursor_t_ptr>
        execute_plan(components::session::session_id_t session,
                     components::logical_plan::node_ptr plan,
                     components::logical_plan::parameter_node_ptr params);
        unique_future<size_t>
        size(components::session::session_id_t session, std::string database_name, std::string collection);
        unique_future<components::cursor::cursor_t_ptr>
        get_schema(components::session::session_id_t session,
                   std::pmr::vector<std::pair<database_name_t, collection_name_t>> ids);
        unique_future<bool> register_udf(components::session::session_id_t session,
                                         components::compute::function_ptr function);
        unique_future<bool> unregister_udf(components::session::session_id_t session,
                                           std::string function_name,
                                           std::pmr::vector<components::types::complex_logical_type> inputs);
        unique_future<void> close_cursor(components::session::session_id_t session);

        // Transaction lifecycle (actor-callable by executor)
        unique_future<components::table::transaction_data> begin_transaction(components::session::session_id_t session);
        unique_future<uint64_t> commit_transaction(components::session::session_id_t session);
        unique_future<void> abort_transaction(components::session::session_id_t session);

        using dispatch_traits = actor_zeta::dispatch_traits<&manager_dispatcher_t::execute_plan,
                                                            &manager_dispatcher_t::size,
                                                            &manager_dispatcher_t::get_schema,
                                                            &manager_dispatcher_t::register_udf,
                                                            &manager_dispatcher_t::unregister_udf,
                                                            &manager_dispatcher_t::close_cursor,
                                                            &manager_dispatcher_t::begin_transaction,
                                                            &manager_dispatcher_t::commit_transaction,
                                                            &manager_dispatcher_t::abort_transaction>;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        run_fn_t run_fn_; // Yield function for cooperative scheduling

        static constexpr std::size_t executor_pool_size_ = 4;

        collection_storage_t collections_;
        std::pmr::vector<services::collection::executor::executor_ptr> executors_;
        std::pmr::vector<actor_zeta::address_t> executor_addresses_;

        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t index_address_ = actor_zeta::address_t::empty_address();

        std::mutex mutex_;

        components::table::transaction_manager_t txn_manager_;

        // M5 plan cache: snapshot-isolated resolution keyed by (plan_hash, catalog_version).
        // pin_version() in begin_transaction → unpin_version() in commit/abort. Probe/store
        // wrap validate_* in execute_plan; on a miss, the freshly resolved data is cached at
        // the current catalog_version_. last_seen_version_ tracks the most-recent disk-side
        // catalog_version_ — refreshed at the start of every execute_plan via
        // refresh_invalidations_(), which also reseeds the cache on overflow.
        versioned_plan_cache_t plan_cache_;
        std::uint64_t last_seen_version_{0};

        unique_future<void> refresh_invalidations_(components::session::session_id_t session);

        components::logical_plan::node_ptr create_logic_plan(components::logical_plan::node_ptr plan);

        unique_future<components::cursor::cursor_t_ptr>
        execute_ddl_inline(components::session::session_id_t session,
                           components::logical_plan::node_ptr logical_plan,
                           components::table::transaction_data txn,
                           catalog_view_t& view);

        services::collection::executor::execute_result_t
        create_database_(components::logical_plan::node_ptr logical_plan);
        services::collection::executor::execute_result_t
        drop_database_(components::logical_plan::node_ptr logical_plan);
        services::collection::executor::execute_result_t
        create_collection_(components::logical_plan::node_ptr logical_plan);
        services::collection::executor::execute_result_t
        drop_collection_(components::logical_plan::node_ptr logical_plan);

        unique_future<services::collection::executor::execute_result_t>
        execute_plan_impl(components::session::session_id_t session,
                          components::logical_plan::node_ptr logical_plan,
                          components::logical_plan::storage_parameters parameters,
                          components::table::transaction_data txn);

        actor_zeta::behavior_t current_behavior_;
    };

} // namespace services::dispatcher
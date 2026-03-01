#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/table_metadata.hpp>
#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/vector/data_chunk.hpp>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/table/row_version_manager.hpp>
#include <core/btree/btree.hpp>
#include <services/collection/context_storage.hpp>
#include <stack>

namespace components::table {
    class transaction_manager_t;
}

namespace services::collection::executor {

    struct execute_result_t {
        components::cursor::cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t updates;
    };

    using function_result_t = components::compute::function_uid;

    struct plan_t {
        std::stack<components::operators::operator_ptr> sub_plans;
        components::logical_plan::storage_parameters parameters;
        services::context_storage_t context_storage_;

        explicit plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                        components::logical_plan::storage_parameters parameters,
                        services::context_storage_t&& context_storage);
    };
    using plan_storage_t = core::pmr::btree::btree_t<components::session::session_id_t, plan_t>;

    // Internal result with MVCC tracking (not exposed to dispatcher)
    struct sub_plan_result_t {
        components::cursor::cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t updates;
        int64_t append_row_start{0};
        uint64_t append_row_count{0};
        size_t delete_count{0};
        uint64_t delete_txn_id{0};

        // Physical WAL data (captured in intercept_dml_io_ for physical WAL writes)
        std::unique_ptr<components::vector::data_chunk_t> wal_insert_data;
        std::pmr::vector<int64_t> wal_row_ids{std::pmr::get_default_resource()};
        std::unique_ptr<components::vector::data_chunk_t> wal_update_data;
        collection_full_name_t wal_collection;
    };

    class executor_t final : public actor_zeta::basic_actor<executor_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        executor_t(std::pmr::memory_resource* resource,
                   actor_zeta::address_t parent_address,
                   actor_zeta::address_t wal_address,
                   actor_zeta::address_t disk_address,
                   actor_zeta::address_t index_address,
                   components::table::transaction_manager_t* txn_manager,
                   log_t&& log);
        ~executor_t() = default;

        unique_future<execute_result_t> execute_plan(components::session::session_id_t session,
                                                     components::logical_plan::node_ptr logical_plan,
                                                     components::logical_plan::storage_parameters parameters,
                                                     services::context_storage_t context_storage,
                                                     components::table::transaction_data txn);

        unique_future<function_result_t> register_udf(components::session::session_id_t session,
                                                      components::compute::function_ptr function);

        using dispatch_traits = actor_zeta::dispatch_traits<&executor_t::execute_plan, &executor_t::register_udf>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        plan_t traverse_plan_(components::operators::operator_ptr&& plan,
                              components::logical_plan::storage_parameters&& parameters,
                              services::context_storage_t&& context_storage);

        unique_future<sub_plan_result_t> execute_sub_plan_(components::session::session_id_t session,
                                                           plan_t plan_data,
                                                           components::table::transaction_data txn);

        unique_future<sub_plan_result_t> intercept_dml_io_(components::operators::operator_t::ptr waiting_op,
                                                           components::pipeline::context_t* ctx);

    private:
        actor_zeta::address_t parent_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t index_address_ = actor_zeta::address_t::empty_address();
        components::table::transaction_manager_t* txn_manager_{nullptr};
        log_t log_;
        components::compute::function_registry_t function_registry_;

        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<execute_result_t>> pending_execute_;

        void poll_pending();
    };

    using executor_ptr = std::unique_ptr<executor_t, actor_zeta::pmr::deleter_t>;
} // namespace services::collection::executor

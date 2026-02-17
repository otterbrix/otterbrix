#pragma once

#include <components/catalog/table_metadata.hpp>
#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <services/collection/collection.hpp>
#include <services/collection/context_storage.hpp>
#include <stack>

namespace services::collection::executor {

    struct execute_result_t {
        components::cursor::cursor_t_ptr cursor;
        components::operators::operator_write_data_t::updated_types_map_t updates;
    };

    struct function_result_t {
        std::string function_name;
        components::compute::function_uid function_id;
        std::vector<components::compute::kernel_signature_t> function_signatures;
    };

    struct plan_t {
        std::stack<components::operators::operator_ptr> sub_plans;
        components::logical_plan::storage_parameters parameters;
        services::context_storage_t context_storage_;

        explicit plan_t(std::stack<components::operators::operator_ptr>&& sub_plans,
                        components::logical_plan::storage_parameters parameters,
                        services::context_storage_t&& context_storage);
    };
    using plan_storage_t = core::pmr::btree::btree_t<components::session::session_id_t, plan_t>;

    class executor_t final : public actor_zeta::basic_actor<executor_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        executor_t(std::pmr::memory_resource* resource,
                   actor_zeta::address_t parent_address,
                   actor_zeta::address_t wal_address,
                   actor_zeta::address_t disk_address,
                   log_t&& log);
        ~executor_t() = default;

        unique_future<execute_result_t> execute_plan(components::session::session_id_t session,
                                                     components::logical_plan::node_ptr logical_plan,
                                                     components::logical_plan::storage_parameters parameters,
                                                     services::context_storage_t context_storage);

        unique_future<std::optional<function_result_t>> register_udf(components::session::session_id_t session,
                                                                     components::compute::function_ptr function);

        using dispatch_traits = actor_zeta::dispatch_traits<&executor_t::execute_plan, &executor_t::register_udf>;

        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        plan_t traverse_plan_(components::operators::operator_ptr&& plan,
                              components::logical_plan::storage_parameters&& parameters,
                              services::context_storage_t&& context_storage);

        unique_future<execute_result_t> execute_sub_plan_(components::session::session_id_t session, plan_t plan_data);

        unique_future<components::cursor::cursor_t_ptr>
        aggregate_document_impl_(const components::session::session_id_t& session,
                                 context_collection_t* context_,
                                 components::operators::operator_ptr plan);
        unique_future<components::cursor::cursor_t_ptr>
        update_document_impl_(const components::session::session_id_t& session,
                              context_collection_t* context_,
                              components::operators::operator_ptr plan);
        unique_future<components::cursor::cursor_t_ptr>
        insert_document_impl_(const components::session::session_id_t& session,
                              context_collection_t* context_,
                              components::operators::operator_ptr plan);
        unique_future<components::cursor::cursor_t_ptr>
        delete_document_impl_(const components::session::session_id_t& session,
                              context_collection_t* context_,
                              components::operators::operator_ptr plan);

    private:
        actor_zeta::address_t parent_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t wal_address_ = actor_zeta::address_t::empty_address();
        actor_zeta::address_t disk_address_ = actor_zeta::address_t::empty_address();
        log_t log_;
        components::compute::function_registry_t function_registry_;

        std::pmr::vector<unique_future<void>> pending_void_;
        std::pmr::vector<unique_future<execute_result_t>> pending_execute_;

        void poll_pending();
    };

    using executor_ptr = std::unique_ptr<executor_t, actor_zeta::pmr::deleter_t>;
} // namespace services::collection::executor
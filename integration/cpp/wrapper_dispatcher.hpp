#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <vector>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <core/spinlock/spinlock.hpp>

#include <components/catalog/table_id.hpp>
#include <components/compute/function.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/session/session.hpp>
#include <components/sql/transformer/transformer.hpp>

#include <services/dispatcher/dispatcher.hpp>

namespace otterbrix {

    using components::session::session_id_t;

    class wrapper_dispatcher_t final : public actor_zeta::actor::actor_mixin<wrapper_dispatcher_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        using dispatch_traits = actor_zeta::dispatch_traits<>;

        /// blocking method
        wrapper_dispatcher_t(std::pmr::memory_resource*,
                             services::dispatcher::manager_dispatcher_t* manager_dispatcher,
                             actor_zeta::scheduler_raw scheduler,
                             log_t& log);
        ~wrapper_dispatcher_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

        [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
        enqueue_impl(actor_zeta::mailbox::message_ptr msg);

        auto create_collection(const session_id_t& session,
                               const database_name_t& database,
                               const collection_name_t& collection,
                               std::vector<components::table::column_definition_t> column_definitions = {},
                               std::vector<components::table::table_constraint_t> constraints = {})
            -> components::cursor::cursor_t_ptr;
        auto drop_collection(const session_id_t& session,
                             const database_name_t& database,
                             const collection_name_t& collection) -> components::cursor::cursor_t_ptr;
        auto find(const session_id_t& session,
                  components::logical_plan::node_aggregate_ptr condition,
                  components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;
        auto find_one(const session_id_t& session,
                      components::logical_plan::node_aggregate_ptr condition,
                      components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;
        auto delete_one(const session_id_t& session,
                        components::logical_plan::node_match_ptr condition,
                        components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;
        auto delete_many(const session_id_t& session,
                         components::logical_plan::node_match_ptr condition,
                         components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;
        auto update_one(const session_id_t& session,
                        components::logical_plan::node_match_ptr condition,
                        components::logical_plan::parameter_node_ptr params,
                        const std::pmr::vector<components::expressions::update_expr_ptr>& updates,
                        bool upsert) -> components::cursor::cursor_t_ptr;
        auto update_many(const session_id_t& session,
                         components::logical_plan::node_match_ptr condition,
                         components::logical_plan::parameter_node_ptr params,
                         const std::pmr::vector<components::expressions::update_expr_ptr>& updates,
                         bool upsert) -> components::cursor::cursor_t_ptr;
        auto register_udf(const session_id_t& session, components::compute::function_ptr function) -> bool;
        auto unregister_udf(const session_id_t& session,
                            const std::string& function_name,
                            const std::pmr::vector<components::types::complex_logical_type>& inputs) -> bool;
        auto create_index(const session_id_t& session, components::logical_plan::node_create_index_ptr node)
            -> components::cursor::cursor_t_ptr;
        auto create_index(const session_id_t& session,
                          const std::string& dbname,
                          const std::string& relname,
                          components::logical_plan::node_create_index_ptr node) -> components::cursor::cursor_t_ptr;
        auto drop_index(const session_id_t& session, components::logical_plan::node_drop_index_ptr node)
            -> components::cursor::cursor_t_ptr;
        auto execute_plan(const session_id_t& session,
                          components::logical_plan::node_ptr plan,
                          components::logical_plan::parameter_node_ptr params = nullptr)
            -> components::cursor::cursor_t_ptr;
        auto execute_sql(const session_id_t& session, const std::string& query) -> components::cursor::cursor_t_ptr;
        auto set_timezone(const session_id_t& session, std::string timezone_name) -> components::cursor::cursor_t_ptr;

    private:
        std::pmr::memory_resource* resource_;
        services::dispatcher::manager_dispatcher_t* manager_dispatcher_;
        actor_zeta::scheduler_raw scheduler_;
        log_t log_;
        std::atomic_int i = 0;

        std::mutex event_loop_mutex_;
        std::condition_variable event_loop_cv_;

        template<typename T>
        T wait_future(unique_future<T>& future);
        void wait_future_void(unique_future<void>& future);

        auto send_plan(const session_id_t& session,
                       components::logical_plan::node_ptr node,
                       components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;
    };

    template<typename T>
    T wrapper_dispatcher_t::wait_future(unique_future<T>& future) {
        while (!future.is_ready()) {
            std::unique_lock<std::mutex> lock(event_loop_mutex_);
            if (!future.is_ready()) {
                // 100µs poll: event-loop managers return from enqueue instantly
                // and never notify event_loop_cv_, so the tick bounds per-query
                // handoff latency. Matched to the manager loops' 100µs cadence.
                event_loop_cv_.wait_for(lock, std::chrono::microseconds(100));
            }
        }
        return std::move(future).take_ready();
    }

} // namespace otterbrix

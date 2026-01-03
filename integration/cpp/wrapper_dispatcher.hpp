#pragma once

#include <atomic>
#include <mutex>
#include <condition_variable>

#include <actor-zeta.hpp>
#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>

#include <core/spinlock/spinlock.hpp>

#include <components/catalog/table_id.hpp>
#include <components/cursor/cursor.hpp>
#include <components/document/document.hpp>
#include <components/expressions/update_expression.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/session/session.hpp>
#include <components/sql/transformer/transformer.hpp>

namespace otterbrix {

    using components::document::document_ptr;
    using components::session::session_id_t;

    // ============================================================================
    // wrapper_dispatcher_t - "boundary" between sync and async worlds
    // All methods use typed send + wait_future (event loop with mutex+cv)
    // NO CALLBACKS - results returned via future directly!
    // ============================================================================
    class wrapper_dispatcher_t final : public actor_zeta::actor::actor_mixin<wrapper_dispatcher_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        // dispatch_traits is EMPTY - no callback methods!
        // All results come via future from typed send
        using dispatch_traits = actor_zeta::dispatch_traits<>;

        /// blocking method
        wrapper_dispatcher_t(std::pmr::memory_resource*, actor_zeta::address_t, log_t& log);
        ~wrapper_dispatcher_t();

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        auto make_type() const noexcept -> const char*;
        void behavior(actor_zeta::mailbox::message* msg);

        // === Blocking public API (for clients) ===
        // All methods use typed send + wait_future
        auto load() -> void;
        auto create_database(const session_id_t& session, const database_name_t& database)
            -> components::cursor::cursor_t_ptr;
        auto drop_database(const session_id_t& session, const database_name_t& database)
            -> components::cursor::cursor_t_ptr;
        auto create_collection(const session_id_t& session,
                               const database_name_t& database,
                               const collection_name_t& collection,
                               std::pmr::vector<components::types::complex_logical_type> schema = {})
            -> components::cursor::cursor_t_ptr;
        auto drop_collection(const session_id_t& session,
                             const database_name_t& database,
                             const collection_name_t& collection) -> components::cursor::cursor_t_ptr;
        auto insert_one(const session_id_t& session,
                        const database_name_t& database,
                        const collection_name_t& collection,
                        document_ptr document) -> components::cursor::cursor_t_ptr;
        auto insert_many(const session_id_t& session,
                         const database_name_t& database,
                         const collection_name_t& collection,
                         const std::pmr::vector<document_ptr>& documents) -> components::cursor::cursor_t_ptr;
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
        auto size(const session_id_t& session, const database_name_t& database, const collection_name_t& collection)
            -> size_t;
        auto create_index(const session_id_t& session, components::logical_plan::node_create_index_ptr node)
            -> components::cursor::cursor_t_ptr;
        auto drop_index(const session_id_t& session, components::logical_plan::node_drop_index_ptr node)
            -> components::cursor::cursor_t_ptr;
        auto execute_plan(const session_id_t& session,
                          components::logical_plan::node_ptr plan,
                          components::logical_plan::parameter_node_ptr params = nullptr)
            -> components::cursor::cursor_t_ptr;
        auto execute_sql(const session_id_t& session, const std::string& query) -> components::cursor::cursor_t_ptr;

        auto get_schema(const session_id_t& session,
                        const std::pmr::vector<std::pair<database_name_t, collection_name_t>>& ids)
            -> components::cursor::cursor_t_ptr;

    private:
        std::pmr::memory_resource* resource_;
        actor_zeta::address_t manager_dispatcher_;
        components::sql::transform::transformer transformer_;
        log_t log_;
        std::atomic_int i = 0;

        // Mutex + CV for blocking wait on future (event loop pattern)
        std::mutex wait_mutex_;
        std::condition_variable wait_cv_;

        // Event loop wait on future
        // This is the main method for blocking wait on future from typed send
        template<typename T>
        T wait_future(unique_future<T>& future);

        // Helper for void futures
        void wait_future_void(unique_future<void>& future);

        auto send_plan(const session_id_t& session,
                       components::logical_plan::node_ptr node,
                       components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;
    };

    // Template implementation
    template<typename T>
    T wrapper_dispatcher_t::wait_future(unique_future<T>& future) {
        // Event loop with mutex + cv on future
        std::unique_lock<std::mutex> lock(wait_mutex_);
        while (!future.available()) {
            wait_cv_.wait_for(lock, std::chrono::milliseconds(1));
        }
        return std::move(future).get();
    }

} // namespace otterbrix
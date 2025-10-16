#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <variant>

#include <actor-zeta.hpp>

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
#include <integration/cpp/impl/session_blocker.hpp>

namespace otterbrix {

    using components::document::document_ptr;
    using components::session::session_id_t;

    class wrapper_dispatcher_t final : public actor_zeta::cooperative_supervisor<wrapper_dispatcher_t> {
    public:
        /// blocking method
        wrapper_dispatcher_t(std::pmr::memory_resource*, actor_zeta::address_t, log_t& log);
        ~wrapper_dispatcher_t();
        auto load() -> void;
        [[deprecated]] auto create_database(const session_id_t& session, const database_name_t& database)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto drop_database(const session_id_t& session, const database_name_t& database)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto create_collection(const session_id_t& session,
                                              const database_name_t& database,
                                              const collection_name_t& collection,
                                              std::pmr::vector<components::types::complex_logical_type> schema = {})
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto drop_collection(const session_id_t& session,
                                            const database_name_t& database,
                                            const collection_name_t& collection) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto insert_one(const session_id_t& session,
                                       const database_name_t& database,
                                       const collection_name_t& collection,
                                       document_ptr document) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto insert_many(const session_id_t& session,
                                        const database_name_t& database,
                                        const collection_name_t& collection,
                                        const std::pmr::vector<document_ptr>& documents)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto find(const session_id_t& session,
                                 components::logical_plan::node_aggregate_ptr condition,
                                 components::logical_plan::parameter_node_ptr params)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto find_one(const session_id_t& session,
                                     components::logical_plan::node_aggregate_ptr condition,
                                     components::logical_plan::parameter_node_ptr params)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto delete_one(const session_id_t& session,
                                       components::logical_plan::node_match_ptr condition,
                                       components::logical_plan::parameter_node_ptr params)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto delete_many(const session_id_t& session,
                                        components::logical_plan::node_match_ptr condition,
                                        components::logical_plan::parameter_node_ptr params)
            -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto update_one(const session_id_t& session,
                                       components::logical_plan::node_match_ptr condition,
                                       components::logical_plan::parameter_node_ptr params,
                                       const std::pmr::vector<components::expressions::update_expr_ptr>& updates,
                                       bool upsert) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto update_many(const session_id_t& session,
                                        components::logical_plan::node_match_ptr condition,
                                        components::logical_plan::parameter_node_ptr params,
                                        const std::pmr::vector<components::expressions::update_expr_ptr>& updates,
                                        bool upsert) -> components::cursor::cursor_t_ptr;
        [[deprecated]] auto size(const session_id_t& session,
                                 const database_name_t& database,
                                 const collection_name_t& collection) -> size_t;
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

        actor_zeta::behavior_t behavior();
        auto make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*;
        auto make_type() const noexcept -> const char* const;

    protected:
        auto enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void final;

    private:
        // Behaviors
        actor_zeta::behavior_t load_finish_;
        actor_zeta::behavior_t execute_plan_finish_;
        actor_zeta::behavior_t size_finish_;
        actor_zeta::behavior_t schema_finish_;
        /// async method
        auto load_finish(const session_id_t& session) -> void;
        auto execute_plan_finish(const session_id_t& session, components::cursor::cursor_t_ptr cursor) -> void;
        auto size_finish(const session_id_t& session, size_t size) -> void;
        auto schema_finish(const session_id_t& session, components::cursor::cursor_t_ptr cursor) -> void;

        // due to hashing, incoming session can be unusable, and has to be replaced
        session_id_t init(const session_id_t& session);
        components::cursor::cursor_t_ptr wait_result(const session_id_t& session);
        size_t wait_size(const session_id_t& session);
        void wait(const session_id_t& session);
        void notify(const session_id_t& session);

        auto send_plan(const session_id_t& session,
                       components::logical_plan::node_ptr node,
                       components::logical_plan::parameter_node_ptr params) -> components::cursor::cursor_t_ptr;

        actor_zeta::address_t manager_dispatcher_;
        components::sql::transform::transformer transformer_;
        log_t log_;
        std::atomic_int i = 0;
        std::mutex output_mtx_;
        spin_lock input_mtx_;
        std::condition_variable cv_;
        impl::session_block_t blocker_;
        components::cursor::cursor_t_ptr cursor_store_;
        size_t size_store_;
        bool bool_store_;
    };
} // namespace otterbrix
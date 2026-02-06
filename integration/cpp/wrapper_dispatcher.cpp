#include "wrapper_dispatcher.hpp"
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <thread>

using namespace components::cursor;

namespace otterbrix {

    wrapper_dispatcher_t::wrapper_dispatcher_t(std::pmr::memory_resource* mr,
                                               actor_zeta::address_t manager_dispatcher,
                                               log_t& log)
        : actor_zeta::actor::actor_mixin<wrapper_dispatcher_t>()
        , resource_(mr)
        , manager_dispatcher_(manager_dispatcher)
        , log_(log.clone()) {}

    wrapper_dispatcher_t::~wrapper_dispatcher_t() { trace(log_, "delete wrapper_dispatcher_t"); }

    void wrapper_dispatcher_t::behavior(actor_zeta::mailbox::message* /*msg*/) {
    }

    auto wrapper_dispatcher_t::make_type() const noexcept -> const char* { return "wrapper_dispatcher"; }

    void wrapper_dispatcher_t::wait_future_void(unique_future<void>& future) {
        while (!future.available()) {
            std::unique_lock<std::mutex> lock(event_loop_mutex_);
            if (!future.available()) {
                event_loop_cv_.wait_for(lock, std::chrono::milliseconds(10));
            }
        }

        event_loop_cv_.notify_all();

        std::move(future).get();
    }


    auto wrapper_dispatcher_t::create_database(const session_id_t& session, const database_name_t& database)
        -> cursor_t_ptr {
        auto plan = components::logical_plan::make_node_create_database(resource(), {database, {}});
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::drop_database(const components::session::session_id_t& session,
                                             const database_name_t& database) -> cursor_t_ptr {
        auto plan = components::logical_plan::make_node_drop_database(resource(), {database, {}});
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::create_collection(const session_id_t& session,
                                                 const database_name_t& database,
                                                 const collection_name_t& collection,
                                                 std::pmr::vector<components::types::complex_logical_type> schema)
        -> cursor_t_ptr {
        auto plan = components::logical_plan::make_node_create_collection(resource(),
                                                                          {database, collection},
                                                                          std::move(schema));
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::drop_collection(const components::session::session_id_t& session,
                                               const database_name_t& database,
                                               const collection_name_t& collection) -> cursor_t_ptr {
        auto plan = components::logical_plan::make_node_drop_collection(resource(), {database, collection});
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::insert_one(const session_id_t& session,
                                          const database_name_t& database,
                                          const collection_name_t& collection,
                                          document_ptr document) -> cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::insert_one session: {}, collection name: {} ", session.data(), collection);
        auto plan = components::logical_plan::make_node_insert(resource(), {database, collection}, {document});
        return send_plan(session, std::move(plan), components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::insert_many(const session_id_t& session,
                                           const database_name_t& database,
                                           const collection_name_t& collection,
                                           const std::pmr::vector<document_ptr>& documents) -> cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        auto plan = components::logical_plan::make_node_insert(resource(), {database, collection}, documents);
        return send_plan(session, std::move(plan), components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::find(const session_id_t& session,
                                    components::logical_plan::node_aggregate_ptr condition,
                                    components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::find session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        return send_plan(session, std::move(condition), std::move(params));
    }

    auto wrapper_dispatcher_t::find_one(const components::session::session_id_t& session,
                                        components::logical_plan::node_aggregate_ptr condition,
                                        components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::find_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        return send_plan(session, condition, std::move(params));
    }

    auto wrapper_dispatcher_t::delete_one(const components::session::session_id_t& session,
                                          components::logical_plan::node_match_ptr condition,
                                          components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        auto plan =
            components::logical_plan::make_node_delete_one(resource(), condition->collection_full_name(), condition);
        return send_plan(session, std::move(plan), std::move(params));
    }

    auto wrapper_dispatcher_t::delete_many(const components::session::session_id_t& session,
                                           components::logical_plan::node_match_ptr condition,
                                           components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_many session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        auto plan =
            components::logical_plan::make_node_delete_many(resource(), condition->collection_full_name(), condition);
        return send_plan(session, std::move(plan), std::move(params));
    }

    auto wrapper_dispatcher_t::update_one(const components::session::session_id_t& session,
                                          components::logical_plan::node_match_ptr condition,
                                          components::logical_plan::parameter_node_ptr params,
                                          const std::pmr::vector<components::expressions::update_expr_ptr>& updates,
                                          bool upsert) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::update_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        auto plan = components::logical_plan::make_node_update_one(resource(),
                                                                   condition->collection_full_name(),
                                                                   condition,
                                                                   updates,
                                                                   upsert);
        return send_plan(session, std::move(plan), std::move(params));
    }

    auto wrapper_dispatcher_t::update_many(const components::session::session_id_t& session,
                                           components::logical_plan::node_match_ptr condition,
                                           components::logical_plan::parameter_node_ptr params,
                                           const std::pmr::vector<components::expressions::update_expr_ptr>& updates,
                                           bool upsert) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::update_many session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        auto plan = components::logical_plan::make_node_update_many(resource(),
                                                                    condition->collection_full_name(),
                                                                    condition,
                                                                    updates,
                                                                    upsert);
        return send_plan(session, std::move(plan), std::move(params));
    }

    auto wrapper_dispatcher_t::size(const session_id_t& session,
                                    const database_name_t& database,
                                    const collection_name_t& collection) -> size_t {
        trace(log_, "wrapper_dispatcher_t::size session: {}, collection name : {} ", session.data(), collection);
        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_,
                                                  &services::dispatcher::manager_dispatcher_t::size,
                                                  session, database, collection);
        return wait_future(future);
    }

    auto wrapper_dispatcher_t::create_index(const session_id_t& session,
                                            components::logical_plan::node_create_index_ptr node)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), node->name());
        return send_plan(session, node, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::drop_index(const session_id_t& session,
                                          components::logical_plan::node_drop_index_ptr node)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::drop_index session: {}, index: {}", session.data(), node->name());
        return send_plan(session, node, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::execute_plan(const session_id_t& session,
                                            components::logical_plan::node_ptr plan,
                                            components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        using namespace components::logical_plan;
        if (!params) {
            params = make_parameter_node(resource());
        }
        trace(log_, "wrapper_dispatcher_t::execute session: {}", session.data());
        return send_plan(session, std::move(plan), std::move(params));
    }

    cursor_t_ptr wrapper_dispatcher_t::execute_sql(const components::session::session_id_t& session,
                                                   const std::string& query) {
        using namespace components::sql::transform;

        trace(log_, "wrapper_dispatcher_t::execute sql session: {}", session.data());
        std::pmr::monotonic_buffer_resource parser_arena(resource());
        auto parse_result = linitial(raw_parser(&parser_arena, query.c_str()));
        transformer local_transformer(resource());
        if (auto result = local_transformer.transform(pg_cell_to_node_cast(parse_result)).finalize();
            std::holds_alternative<bind_error>(result)) {
            return make_cursor(resource(),
                               components::cursor::error_code_t::sql_parse_error,
                               std::get<bind_error>(std::move(result)).what());
        } else {
            auto view = std::get<result_view>(std::move(result));
            return execute_plan(session, std::move(view.node), std::move(view.params));
        }
    }

    auto wrapper_dispatcher_t::get_schema(const components::session::session_id_t& session,
                                          const std::pmr::vector<std::pair<database_name_t, collection_name_t>>& ids)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::get_schema session: {}", session.data());
        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_,
                                                  &services::dispatcher::manager_dispatcher_t::get_schema,
                                                  session, ids);
        return wait_future(future);
    }

    cursor_t_ptr wrapper_dispatcher_t::send_plan(const session_id_t& session,
                                                 components::logical_plan::node_ptr node,
                                                 components::logical_plan::parameter_node_ptr params) {
        trace(log_, "wrapper_dispatcher_t::send_plan session: {}, {} ", session.data(), node->to_string());
        assert(params);

        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_,
                                                  &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                  session, std::move(node), std::move(params));

        return wait_future(future);
    }

} // namespace otterbrix
#include "wrapper_dispatcher.hpp"
#include "route.hpp"
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <core/system_command.hpp>

using namespace components::cursor;

namespace otterbrix {

    wrapper_dispatcher_t::wrapper_dispatcher_t(std::pmr::memory_resource* mr,
                                               actor_zeta::address_t manager_dispatcher,
                                               log_t& log)
        : actor_zeta::cooperative_supervisor<wrapper_dispatcher_t>(mr)
        , load_finish_(actor_zeta::make_behavior(resource(),
                                                 core::handler_id(core::route::load_finish),
                                                 this,
                                                 &wrapper_dispatcher_t::load_finish))
        , execute_plan_finish_(actor_zeta::make_behavior(resource(),
                                                         dispatcher::handler_id(dispatcher::route::execute_plan_finish),
                                                         this,
                                                         &wrapper_dispatcher_t::execute_plan_finish))
        , size_finish_(actor_zeta::make_behavior(resource(),
                                                 collection::handler_id(collection::route::size_finish),
                                                 this,
                                                 &wrapper_dispatcher_t::size_finish))
        , schema_finish_(actor_zeta::make_behavior(resource(),
                                                   collection::handler_id(collection::route::schema_finish),
                                                   this,
                                                   &wrapper_dispatcher_t::schema_finish))
        , manager_dispatcher_(manager_dispatcher)
        , transformer_(mr)
        , log_(log.clone())
        , blocker_(mr) {}

    wrapper_dispatcher_t::~wrapper_dispatcher_t() { trace(log_, "delete wrapper_dispatcher_t"); }

    actor_zeta::behavior_t wrapper_dispatcher_t::behavior() {
        return actor_zeta::make_behavior(resource(), [this](actor_zeta::message* msg) -> void {
            switch (msg->command()) {
                case core::handler_id(core::route::load_finish): {
                    load_finish_(msg);
                    break;
                }
                case dispatcher::handler_id(dispatcher::route::execute_plan_finish): {
                    execute_plan_finish_(msg);
                    break;
                }
                case collection::handler_id(collection::route::size_finish): {
                    size_finish_(msg);
                    break;
                }
                case collection::handler_id(collection::route::schema_finish): {
                    schema_finish_(msg);
                    break;
                }
            }
        });
    }

    auto wrapper_dispatcher_t::make_type() const noexcept -> const char* const { return "wrapper_dispatcher"; }

    auto wrapper_dispatcher_t::load() -> void {
        session_id_t session;
        trace(log_, "wrapper_dispatcher_t::load session: {}", session.data());
        session_id_t approved_session = init(session);
        actor_zeta::send(manager_dispatcher_, address(), core::handler_id(core::route::load), approved_session);
        wait(approved_session);
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
        session_id_t approved_session = init(session);
        auto plan = components::logical_plan::make_node_insert(resource(), {database, collection}, {document});
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         plan,
                         components::logical_plan::make_parameter_node(resource()));
        return wait_result(approved_session);
    }

    auto wrapper_dispatcher_t::insert_many(const session_id_t& session,
                                           const database_name_t& database,
                                           const collection_name_t& collection,
                                           const std::pmr::vector<document_ptr>& documents) -> cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::insert_many session: {}, collection name: {} ", session.data(), collection);
        session_id_t approved_session = init(session);
        auto plan = components::logical_plan::make_node_insert(resource(), {database, collection}, documents);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         plan,
                         components::logical_plan::make_parameter_node(resource()));
        return wait_result(approved_session);
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
        session_id_t approved_session = init(session);
        auto plan =
            components::logical_plan::make_node_delete_one(resource(), condition->collection_full_name(), condition);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         plan,
                         std::move(params));
        return wait_result(approved_session);
    }

    auto wrapper_dispatcher_t::delete_many(const components::session::session_id_t& session,
                                           components::logical_plan::node_match_ptr condition,
                                           components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_many session: {}, database: {} collection: {} ",
              session.data(),
              condition->collection_full_name().database,
              condition->collection_full_name().collection);
        session_id_t approved_session = init(session);
        auto plan =
            components::logical_plan::make_node_delete_many(resource(), condition->collection_full_name(), condition);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         plan,
                         std::move(params));
        return wait_result(approved_session);
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
        session_id_t approved_session = init(session);
        auto plan = components::logical_plan::make_node_update_one(resource(),
                                                                   condition->collection_full_name(),
                                                                   condition,
                                                                   updates,
                                                                   upsert);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         plan,
                         std::move(params));
        return wait_result(approved_session);
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
        session_id_t approved_session = init(session);
        auto plan = components::logical_plan::make_node_update_many(resource(),
                                                                    condition->collection_full_name(),
                                                                    condition,
                                                                    updates,
                                                                    upsert);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         plan,
                         std::move(params));
        return wait_result(approved_session);
    }

    auto wrapper_dispatcher_t::size(const session_id_t& session,
                                    const database_name_t& database,
                                    const collection_name_t& collection) -> size_t {
        trace(log_, "wrapper_dispatcher_t::size session: {}, collection name : {} ", session.data(), collection);
        session_id_t approved_session = init(session);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         collection::handler_id(collection::route::size),
                         approved_session,
                         database,
                         collection);
        return wait_size(approved_session);
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
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), node->name());
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
        trace(log_, "wrapper_dispatcher_t::execute sql session: {}", session.data());
        auto params = components::logical_plan::make_parameter_node(resource());
        auto parse_result = raw_parser(query.c_str())->lst.front().data;
        auto node =
            transformer_.transform(components::sql::transform::pg_cell_to_node_cast(parse_result), params.get());
        return execute_plan(session, node, params);
    }

    auto wrapper_dispatcher_t::get_schema(const components::session::session_id_t& session,
                                          const std::pmr::vector<std::pair<database_name_t, collection_name_t>>& ids)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::get_schema session: {}", session.data());
        session_id_t approved_session = init(session);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         collection::handler_id(collection::route::schema),
                         approved_session,
                         ids);
        return wait_result(approved_session);
    }

    auto wrapper_dispatcher_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t* {
        assert("wrapper_dispatcher_t::executor_impl");
        return nullptr;
    }

    auto wrapper_dispatcher_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        std::unique_lock<spin_lock> _(input_mtx_);
        auto tmp = std::move(msg);
        trace(log_, "wrapper_dispatcher_t::enqueue_base msg type: {}", tmp->command().integer_value());
        behavior()(tmp.get());
    }

    auto wrapper_dispatcher_t::load_finish(const session_id_t& session) -> void {
        trace(log_, "wrapper_dispatcher_t::load_finish");
        notify(session);
    }

    void wrapper_dispatcher_t::execute_plan_finish(const session_id_t& session, cursor_t_ptr cursor) {
        trace(log_, "wrapper_dispatcher_t::execute_plan_finish session: {} {}", session.data(), cursor->is_success());
        std::unique_lock<std::mutex> lk(output_mtx_);
        cursor_store_ = std::move(cursor);
        notify(session);
    }

    auto wrapper_dispatcher_t::size_finish(const session_id_t& session, size_t size) -> void {
        trace(log_, "wrapper_dispatcher_t::size_finish session: {} {}", session.data(), size);
        std::unique_lock<std::mutex> lk(output_mtx_);
        size_store_ = size;
        notify(session);
    }

    auto wrapper_dispatcher_t::schema_finish(const components::session::session_id_t& session,
                                             components::cursor::cursor_t_ptr cursor) -> void {
        trace(log_, "wrapper_dispatcher_t::schema_finish session: {} {}", session.data(), cursor->is_success());
        std::unique_lock<std::mutex> lk(output_mtx_);
        cursor_store_ = std::move(cursor);
        notify(session);
    }

    session_id_t wrapper_dispatcher_t::init(const session_id_t& session) {
        session_id_t res_session = session;
        while (!blocker_.set_value(res_session, false)) {
            res_session = session_id_t();
            trace(log_, "wrapper_dispatcher_t::init session hash collision! New session is: {}", res_session.data());
        }
        return res_session;
    }

    components::cursor::cursor_t_ptr wrapper_dispatcher_t::wait_result(const session_id_t& session) {
        std::unique_lock<std::mutex> lk(output_mtx_);
        cv_.wait(lk, [this, &session]() { return blocker_.value(session); });
        blocker_.remove_session(session);

        if (cursor_store_->is_error()) {
            //todo: handling error
            std::cerr << cursor_store_->get_error().what << std::endl;
        }

        return std::move(cursor_store_);
    }

    size_t wrapper_dispatcher_t::wait_size(const session_id_t& session) {
        std::unique_lock<std::mutex> lk(output_mtx_);
        cv_.wait(lk, [this, &session]() { return blocker_.value(session); });
        blocker_.remove_session(session);
        return std::move(size_store_);
    }

    void wrapper_dispatcher_t::wait(const session_id_t& session) {
        std::unique_lock<std::mutex> lk(output_mtx_);
        cv_.wait(lk, [this, &session]() { return blocker_.value(session); });
        blocker_.remove_session(session);
    }

    void wrapper_dispatcher_t::notify(const session_id_t& session) {
        blocker_.set_value(session, true);
        // multiple threads can be waiting
        cv_.notify_all();
    }

    cursor_t_ptr wrapper_dispatcher_t::send_plan(const session_id_t& session,
                                                 components::logical_plan::node_ptr node,
                                                 components::logical_plan::parameter_node_ptr params) {
        trace(log_, "wrapper_dispatcher_t::send_plan session: {}, {} ", session.data(), node->to_string());
        session_id_t approved_session = init(session);
        assert(params);
        actor_zeta::send(manager_dispatcher_,
                         address(),
                         dispatcher::handler_id(dispatcher::route::execute_plan),
                         approved_session,
                         std::move(node),
                         std::move(params));
        return wait_result(approved_session);
    }

} // namespace otterbrix

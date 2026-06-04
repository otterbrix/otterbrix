#include "wrapper_dispatcher.hpp"
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/utils.hpp>
#include <core/executor.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <thread>

using namespace components::cursor;

namespace otterbrix {

    wrapper_dispatcher_t::wrapper_dispatcher_t(std::pmr::memory_resource* resource,
                                               services::dispatcher::manager_dispatcher_t* manager_dispatcher,
                                               actor_zeta::scheduler_raw scheduler,
                                               log_t& log)
        : actor_zeta::actor::actor_mixin<wrapper_dispatcher_t>()
        , resource_(resource)
        , manager_dispatcher_(manager_dispatcher)
        , scheduler_(scheduler)
        , log_(log.clone()) {}

    wrapper_dispatcher_t::~wrapper_dispatcher_t() { trace(log_, "delete wrapper_dispatcher_t"); }

    actor_zeta::behavior_t wrapper_dispatcher_t::behavior(actor_zeta::mailbox::message* /*msg*/) { co_return; }

    auto wrapper_dispatcher_t::make_type() const noexcept -> const char* { return "wrapper_dispatcher"; }

    [[nodiscard]] std::pair<bool, actor_zeta::detail::enqueue_result>
    wrapper_dispatcher_t::enqueue_impl(actor_zeta::mailbox::message_ptr msg) {
        // Dead code in production — wrapper.address() is never a send target.
        // Exists only to satisfy has_enqueue_impl concept in actor-zeta 1.2.0.
        // Forward to manager_dispatcher's mailbox and ignore the schedule hint
        // (manager_dispatcher is actor_mixin, not a basic_actor — the
        // mailbox-drain loop is owned by its own resume() driver, not by
        // scheduler->enqueue(this)).
        auto [_, res] = manager_dispatcher_->enqueue_impl(std::move(msg));
        return {false, res};
    }

    void wrapper_dispatcher_t::wait_future_void(unique_future<void>& future) {
        while (!future.is_ready()) {
            std::unique_lock<std::mutex> lock(event_loop_mutex_);
            if (!future.is_ready()) {
                // 100µs (was 10ms) — see wait_future in the header for rationale.
                event_loop_cv_.wait_for(lock, std::chrono::microseconds(100));
            }
        }
        std::move(future).take_ready();
    }

    auto wrapper_dispatcher_t::create_collection(const session_id_t& session,
                                                 const database_name_t& database,
                                                 const collection_name_t& collection,
                                                 std::vector<components::table::column_definition_t> column_definitions,
                                                 std::vector<components::table::table_constraint_t> constraints)
        -> cursor_t_ptr {
        auto create = components::logical_plan::make_node_create_collection(resource(),
                                                                            core::relname_t{collection},
                                                                            std::move(column_definitions),
                                                                            std::move(constraints));
        components::logical_plan::node_ptr plan =
            components::sql::transform::maybe_wrap_with_catalog_resolve_namespace(resource(), database, create);
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::drop_collection(const components::session::session_id_t& session,
                                               const database_name_t& database,
                                               const collection_name_t& collection) -> cursor_t_ptr {
        // Drop nodes carry no names; the (db, rel) identity travels via
        // sibling catalog_resolve_namespace / catalog_resolve_table nodes wrapped
        // in a sequence_t. Pass 1 stamps namespace_oid + table_oid on the drop.
        auto drop = components::logical_plan::make_node_drop_collection(resource());
        components::logical_plan::node_ptr plan =
            components::sql::transform::maybe_wrap_with_catalog_resolve_table(resource(), database, collection, drop);
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::find(const session_id_t& session,
                                    components::logical_plan::node_aggregate_ptr condition,
                                    components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::find session: {}, database: {} collection: {} ",
              session.data(),
              static_cast<const std::string&>(condition->dbname()),
              static_cast<const std::string&>(condition->relname()));
        return send_plan(session, std::move(condition), std::move(params));
    }

    auto wrapper_dispatcher_t::find_one(const components::session::session_id_t& session,
                                        components::logical_plan::node_aggregate_ptr condition,
                                        components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::find_one session: {}, database: {} collection: {} ",
              session.data(),
              static_cast<const std::string&>(condition->dbname()),
              static_cast<const std::string&>(condition->relname()));
        return send_plan(session, condition, std::move(params));
    }

    auto wrapper_dispatcher_t::delete_one(const components::session::session_id_t& session,
                                          components::logical_plan::node_match_ptr condition,
                                          components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_one session: {}, database: {} collection: {} ",
              session.data(),
              condition->dbname(),
              condition->relname());
        // Identity travels via the catalog-resolve wrap; the DML node
        // itself carries only payload + table_oid() stamped at enrich time.
        const std::string db = condition->dbname();
        const std::string rel = condition->relname();
        components::logical_plan::node_ptr del = components::logical_plan::make_node_delete_one(resource(), condition);
        auto plan = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
            resource(),
            db,
            rel,
            std::move(del),
            components::sql::transform::constraint_resolve_kind::referencing);
        return send_plan(session, std::move(plan), std::move(params));
    }

    auto wrapper_dispatcher_t::delete_many(const components::session::session_id_t& session,
                                           components::logical_plan::node_match_ptr condition,
                                           components::logical_plan::parameter_node_ptr params) -> cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::delete_many session: {}, database: {} collection: {} ",
              session.data(),
              condition->dbname(),
              condition->relname());
        const std::string db = condition->dbname();
        const std::string rel = condition->relname();
        components::logical_plan::node_ptr del = components::logical_plan::make_node_delete_many(resource(), condition);
        auto plan = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
            resource(),
            db,
            rel,
            std::move(del),
            components::sql::transform::constraint_resolve_kind::referencing);
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
              condition->dbname(),
              condition->relname());
        const std::string db = condition->dbname();
        const std::string rel = condition->relname();
        components::logical_plan::node_ptr upd =
            components::logical_plan::make_node_update_one(resource(), condition, updates, upsert);
        auto plan = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
            resource(),
            db,
            rel,
            std::move(upd),
            components::sql::transform::constraint_resolve_kind::outgoing);
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
              condition->dbname(),
              condition->relname());
        const std::string db = condition->dbname();
        const std::string rel = condition->relname();
        components::logical_plan::node_ptr upd =
            components::logical_plan::make_node_update_many(resource(), condition, updates, upsert);
        auto plan = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
            resource(),
            db,
            rel,
            std::move(upd),
            components::sql::transform::constraint_resolve_kind::outgoing);
        return send_plan(session, std::move(plan), std::move(params));
    }

    auto wrapper_dispatcher_t::register_udf(const session_id_t& session, components::compute::function_ptr function)
        -> bool {
        trace(log_,
              "wrapper_dispatcher_t::register_udf session: {}, function name : {} ",
              session.data(),
              function->name());
        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &services::dispatcher::manager_dispatcher_t::register_udf,
                                                       session,
                                                       std::move(function));
        return wait_future(future);
    }

    auto wrapper_dispatcher_t::unregister_udf(const session_id_t& session,
                                              const std::string& function_name,
                                              const std::pmr::vector<components::types::complex_logical_type>& inputs)
        -> bool {
        trace(log_,
              "wrapper_dispatcher_t::unregister_udf session: {}, function name : {} ",
              session.data(),
              function_name);
        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &services::dispatcher::manager_dispatcher_t::unregister_udf,
                                                       session,
                                                       function_name,
                                                       inputs);
        return wait_future(future);
    }

    auto wrapper_dispatcher_t::create_index(const session_id_t& session,
                                            components::logical_plan::node_create_index_ptr node)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), node->name());
        return send_plan(session, node, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::create_index(const session_id_t& session,
                                            const std::string& dbname,
                                            const std::string& relname,
                                            components::logical_plan::node_create_index_ptr node)
        -> components::cursor::cursor_t_ptr {
        trace(log_, "wrapper_dispatcher_t::create_index session: {}, index: {}", session.data(), node->name());
        components::logical_plan::node_ptr plan =
            components::sql::transform::maybe_wrap_with_catalog_resolve_table(resource(), dbname, relname, node);
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    auto wrapper_dispatcher_t::drop_index(const session_id_t& session,
                                          components::logical_plan::node_drop_index_ptr node)
        -> components::cursor::cursor_t_ptr {
        trace(log_,
              "wrapper_dispatcher_t::drop_index session: {}, index_oid: {}",
              session.data(),
              static_cast<unsigned>(node->index_oid()));
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
        void* parse_result;
        try {
            parse_result = linitial(raw_parser(&parser_arena, query.c_str()));
        } catch (const std::exception& exception) {
            return make_cursor(
                resource(),
                core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{exception.what(), resource()}));
        }

        if (!parse_result) {
            return make_cursor(resource(),
                               core::error_t(core::error_code_t::sql_parse_error,
                                             std::pmr::string{"unknown parser error", resource()}));
        }
        transformer local_transformer(resource(), query.c_str());
        if (auto result = local_transformer.transform(pg_cell_to_node_cast(parse_result)).finalize();
            result.has_error()) {
            return make_cursor(resource(), result.error());
        } else {
            auto& view = std::move(result).value();
            return execute_plan(session, std::move(view.node), std::move(view.params));
        }
    }

    auto wrapper_dispatcher_t::set_timezone(const session_id_t& session, std::string timezone_name) -> cursor_t_ptr {
        std::transform(timezone_name.begin(), timezone_name.end(), timezone_name.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        auto plan = components::logical_plan::make_node_set_timezone(resource(), std::move(timezone_name));
        return send_plan(session, plan, components::logical_plan::make_parameter_node(resource()));
    }

    cursor_t_ptr wrapper_dispatcher_t::send_plan(const session_id_t& session,
                                                 components::logical_plan::node_ptr node,
                                                 components::logical_plan::parameter_node_ptr params) {
        trace(log_, "wrapper_dispatcher_t::send_plan session: {}, {} ", session.data(), node->to_string());
        assert(params);

        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                       session,
                                                       std::move(node),
                                                       std::move(params));

        return wait_future(future);
    }

} // namespace otterbrix
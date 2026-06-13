#include "wrapper_dispatcher.hpp"
#include <components/logical_plan/node_set_timezone.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/transformer/transform_result.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/types/logical_value.hpp>
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
        // Dead in production — wrapper.address() is never a send target. Exists
        // only to satisfy the has_enqueue_impl concept; do not delete. The
        // schedule hint is ignored because manager_dispatcher is an actor_mixin
        // whose drain loop runs from its own resume(), not scheduler->enqueue.
        auto [_, res] = manager_dispatcher_->enqueue_impl(std::move(msg));
        return {false, res};
    }

    void wrapper_dispatcher_t::wait_future_void(unique_future<void>& future) {
        while (!future.is_ready()) {
            std::unique_lock<std::mutex> lock(event_loop_mutex_);
            if (!future.is_ready()) {
                // See wait_future in the header for the 100µs poll rationale.
                event_loop_cv_.wait_for(lock, std::chrono::microseconds(100));
            }
        }
        std::move(future).take_ready();
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

    auto wrapper_dispatcher_t::execute_plan(const session_id_t& session,
                                            components::logical_plan::execution_plan_t plan) -> cursor_t_ptr {
        using namespace components::logical_plan;
        if (!plan.parameters) {
            plan.parameters = make_parameter_node(resource());
        }
        trace(log_, "wrapper_dispatcher_t::execute session: {}", session.data());
        return send_plan(session, std::move(plan));
    }

    cursor_t_ptr wrapper_dispatcher_t::execute_sql(const components::session::session_id_t& session,
                                                   const std::string& query) {
        using namespace components::sql::transform;

        trace(log_, "wrapper_dispatcher_t::execute sql session: {}", session.data());
        std::pmr::monotonic_buffer_resource parser_arena(resource());
        void* parse_result;
        try {
            parse_result = linitial(raw_parser(&parser_arena, query.c_str(), parser_extensions_));
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
        transformer local_transformer(resource(), query.c_str(), &parser_extensions_);
        if (auto result = local_transformer.transform(pg_cell_to_node_cast(parse_result)).finalize();
            result.has_error()) {
            return make_cursor(resource(), result.error());
        } else {
            return execute_plan(session, std::move(result.value()));
        }
    }

    cursor_t_ptr wrapper_dispatcher_t::execute_sql_with_params(
        const components::session::session_id_t& session,
        const std::string& query,
        const std::vector<std::pair<size_t, components::types::logical_value_t>>& params) {
        using namespace components::sql::transform;

        trace(log_, "wrapper_dispatcher_t::execute sql (params) session: {}", session.data());
        std::pmr::monotonic_buffer_resource parser_arena(resource());
        void* parse_result;
        try {
            parse_result = linitial(raw_parser(&parser_arena, query.c_str(), parser_extensions_));
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
        transformer local_transformer(resource(), query.c_str(), &parser_extensions_);
        auto binder = local_transformer.transform(pg_cell_to_node_cast(parse_result));
        try {
            for (const auto& [id, value] : params) {
                binder.bind(id, value);
            }
        } catch (const std::exception& exception) {
            return make_cursor(
                resource(),
                core::error_t(core::error_code_t::sql_parse_error, std::pmr::string{exception.what(), resource()}));
        }

        auto finalized = binder.finalize();
        if (finalized.has_error()) {
            return make_cursor(resource(), finalized.error());
        }
        auto& plan = std::move(finalized).value();
        return execute_plan(session, std::move(plan));
    }

    auto wrapper_dispatcher_t::add_parser_extension(components::sql::parser::parser_extension_t extension)
        -> core::result_wrapper_t<const components::sql::parser::parser_extension_t*> {
        return parser_extensions_.add(std::move(extension));
    }

    auto wrapper_dispatcher_t::set_timezone(const session_id_t& session, std::string timezone_name) -> cursor_t_ptr {
        std::transform(timezone_name.begin(), timezone_name.end(), timezone_name.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        auto node = components::logical_plan::make_node_set_timezone(resource(), std::move(timezone_name));
        return send_plan(
            session,
            components::logical_plan::execution_plan_t{resource(),
                                                       node,
                                                       components::logical_plan::make_parameter_node(resource())});
    }

    cursor_t_ptr wrapper_dispatcher_t::send_plan(const session_id_t& session,
                                                 components::logical_plan::execution_plan_t plan) {
        trace(log_,
              "wrapper_dispatcher_t::send_plan session: {}, {} ",
              session.data(),
              plan.sub_queries.back()->to_string());
        assert(plan.parameters);

        auto [_, future] = actor_zeta::otterbrix::send(manager_dispatcher_->address(),
                                                       &services::dispatcher::manager_dispatcher_t::execute_plan,
                                                       session,
                                                       std::move(plan));

        return wait_future(future);
    }

} // namespace otterbrix
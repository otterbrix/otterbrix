#include "context.hpp"

namespace components::pipeline {

    context_t::context_t(logical_plan::storage_parameters init_parameters)
        : parameters(std::move(init_parameters)) {}

    context_t::context_t(context_t&& context) noexcept
        : session(context.session)
        , current_message_sender(std::move(context.current_message_sender))
        , parameters(std::move(context.parameters))
        , address_(std::move(context.address_)) {}

    context_t::context_t(session::session_id_t session,
                         actor_zeta::address_t address,
                         actor_zeta::address_t sender,
                         const compute::function_registry_t* function_registry,
                         logical_plan::storage_parameters init_parameters)
        : session(session)
        , current_message_sender(std::move(sender))
        , function_registry(function_registry)
        , parameters(std::move(init_parameters))
        , address_(std::move(address)) {}

} // namespace components::pipeline

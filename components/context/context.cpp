#include "context.hpp"

namespace components::pipeline {

    context_t::context_t(logical_plan::storage_parameters init_parameters)
        : parameters(std::move(init_parameters)) {}

    context_t::context_t(context_t&& context)
        : session(context.session)
        , current_message_sender(std::move(context.current_message_sender))
        , parameters(std::move(context.parameters))
        , disk_address(std::move(context.disk_address))
        , index_address(std::move(context.index_address))
        , address_(std::move(context.address_)) {}

    context_t::context_t(session::session_id_t session,
                         actor_zeta::address_t address,
                         actor_zeta::address_t sender,
                         logical_plan::storage_parameters init_parameters)
        : session(session)
        , current_message_sender(std::move(sender))
        , parameters(std::move(init_parameters))
        , address_(std::move(address)) {}

} // namespace components::pipeline

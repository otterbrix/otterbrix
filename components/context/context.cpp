#include "context.hpp"

namespace components::pipeline {

    context_t::context_t(const logical_plan::storage_parameters& init_parameters,
                         actor_zeta::address_t disk,
                         actor_zeta::address_t index,
                         actor_zeta::address_t wal)
        : parameters(init_parameters.clone(init_parameters.resource()))
        , disk_address(std::move(disk))
        , index_address(std::move(index))
        , wal_address(std::move(wal)) {}

    context_t::context_t(session::session_id_t session,
                         actor_zeta::address_t address,
                         actor_zeta::address_t sender,
                         const compute::function_registry_t* function_registry,
                         const logical_plan::storage_parameters& init_parameters,
                         actor_zeta::address_t disk,
                         actor_zeta::address_t index,
                         actor_zeta::address_t wal)
        : session(session)
        , current_message_sender(std::move(sender))
        , function_registry(function_registry)
        , parameters(init_parameters.clone(init_parameters.resource()))
        , disk_address(std::move(disk))
        , index_address(std::move(index))
        , wal_address(std::move(wal))
        , address_(std::move(address)) {}

} // namespace components::pipeline

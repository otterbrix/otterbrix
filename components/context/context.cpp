#include "context.hpp"

namespace components::pipeline {

    namespace {

        // A plain copy would take the map's allocator from select_on_container_copy_construction
        // -- default-constructed for polymorphic_allocator, i.e. the process-global resource,
        // invisible to resource_tracer_t. Rebuild entry by entry onto the source's own resource
        // instead, copying each value through logical_value_t's resource-carrying copy ctor.
        logical_plan::storage_parameters parameters_on_their_own_arena(const logical_plan::storage_parameters& source) {
            logical_plan::storage_parameters copy{source.resource()};
            copy.parameters.reserve(source.parameters.size());
            for (const auto& entry : source.parameters) {
                copy.parameters.emplace(entry.first, types::logical_value_t(copy.resource(), entry.second));
            }
            return copy;
        }

    } // namespace

    context_t::context_t(const logical_plan::storage_parameters& init_parameters,
                         actor_zeta::address_t disk,
                         actor_zeta::address_t index,
                         actor_zeta::address_t wal)
        : parameters(parameters_on_their_own_arena(init_parameters))
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
        , parameters(parameters_on_their_own_arena(init_parameters))
        , disk_address(std::move(disk))
        , index_address(std::move(index))
        , wal_address(std::move(wal))
        , address_(std::move(address)) {}

} // namespace components::pipeline

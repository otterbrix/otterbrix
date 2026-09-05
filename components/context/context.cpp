#include "context.hpp"

namespace components::pipeline {

    namespace {

        // THE ONE PLACE THAT ANSWERS "WHICH ARENA DO THE COPIED PARAMETERS LIVE ON".
        //
        // storage_parameters is a plain struct around a std::pmr::unordered_map, so every
        // implicit copy of it — a by-value parameter, an `auto p = ...`, a defaulted member
        // copy — takes the map's allocator from select_on_container_copy_construction(), which
        // for polymorphic_allocator is a DEFAULT-constructed one: std::pmr::get_default_resource().
        // The map then lands on the process-global arena, invisible to resource_tracer_t, and a
        // move afterwards freezes it there. Rebuilding entry by entry is what keeps it on the
        // arena the source already stood on: the map is constructed with that resource, and each
        // value is copied through logical_value_t's resource-carrying copy constructor so the
        // value's own storage follows the map rather than trailing behind on the default.
        logical_plan::storage_parameters
        parameters_on_their_own_arena(const logical_plan::storage_parameters& source) {
            logical_plan::storage_parameters copy{source.resource()};
            copy.parameters.reserve(source.parameters.size());
            for (const auto& entry : source.parameters) {
                copy.parameters.emplace(entry.first, types::logical_value_t(copy.resource(), entry.second));
            }
            return copy;
        }

    } // namespace

    context_t::context_t(const logical_plan::storage_parameters& init_parameters)
        : parameters(parameters_on_their_own_arena(init_parameters)) {}

    context_t::context_t(session::session_id_t session,
                         actor_zeta::address_t address,
                         actor_zeta::address_t sender,
                         const compute::function_registry_t* function_registry,
                         const logical_plan::storage_parameters& init_parameters)
        : session(session)
        , current_message_sender(std::move(sender))
        , function_registry(function_registry)
        , parameters(parameters_on_their_own_arena(init_parameters))
        , address_(std::move(address)) {}

} // namespace components::pipeline

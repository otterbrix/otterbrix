#pragma once

#include <actor-zeta.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>

namespace components::pipeline {

    class context_t {
    public:
        session::session_id_t session;
        actor_zeta::address_t current_message_sender{actor_zeta::address_t::empty_address()};
        logical_plan::storage_parameters parameters;

        explicit context_t(logical_plan::storage_parameters init_parameters);
        context_t(context_t&& context);
        context_t(session::session_id_t session,
                  actor_zeta::address_t address,
                  actor_zeta::address_t sender,
                  logical_plan::storage_parameters init_parameters);

        const actor_zeta::address_t& address() const noexcept { return address_; }

    private:
        actor_zeta::address_t address_{actor_zeta::address_t::empty_address()};
    };

} // namespace components::pipeline

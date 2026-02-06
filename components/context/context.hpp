#pragma once

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <vector>

namespace components::pipeline {

    class context_t {
    public:
        using disk_future_t = actor_zeta::unique_future<void>;

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

        void add_pending_disk_future(disk_future_t&& future) {
            pending_disk_futures_.push_back(std::move(future));
        }

        std::vector<disk_future_t> take_pending_disk_futures() {
            return std::move(pending_disk_futures_);
        }

        bool has_pending_disk_futures() const noexcept {
            return !pending_disk_futures_.empty();
        }

    private:
        actor_zeta::address_t address_{actor_zeta::address_t::empty_address()};
        std::vector<disk_future_t> pending_disk_futures_;
    };

} // namespace components::pipeline

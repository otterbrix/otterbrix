#pragma once

#include "command.hpp"
#include "disk.hpp"
#include "result.hpp"

#include <actor-zeta/actor/actor_mixin.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/log/log.hpp>
#include <core/excutor.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace services::disk {

    class manager_disk_t;
    using name_t = std::string;
    using session_id_t = ::components::session::session_id_t;

    class base_manager_disk_t;

    class agent_disk_t final : public actor_zeta::basic_actor<agent_disk_t> {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        agent_disk_t(std::pmr::memory_resource* resource, manager_disk_t* manager, const path_t& path_db, log_t& log);
        ~agent_disk_t();

        // Coroutine methods - parameters by value (no const& allowed in coroutines)
        unique_future<result_load_t> load(session_id_t session);

        unique_future<void> append_database(command_t command);
        unique_future<void> remove_database(command_t command);

        unique_future<void> append_collection(command_t command);
        unique_future<void> remove_collection(command_t command);

        unique_future<void> write_documents(command_t command);
        unique_future<void> remove_documents(command_t command);

        unique_future<void> fix_wal_id(wal::id_t wal_id);

        // dispatch_traits must be defined AFTER all method declarations
        using dispatch_traits = actor_zeta::dispatch_traits<
            &agent_disk_t::load,
            &agent_disk_t::append_database,
            &agent_disk_t::remove_database,
            &agent_disk_t::append_collection,
            &agent_disk_t::remove_collection,
            &agent_disk_t::write_documents,
            &agent_disk_t::remove_documents,
            &agent_disk_t::fix_wal_id
        >;

        auto make_type() const noexcept -> const char*;

        void behavior(actor_zeta::mailbox::message* msg);

    private:
        const name_t name_;
        log_t log_;
        disk_t disk_;
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
#pragma once

#include "command.hpp"
#include "disk.hpp"

#include <components/configuration/configuration.hpp>
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
        agent_disk_t(manager_disk_t*, const path_t& path_db, log_t& log);
        ~agent_disk_t();

        auto load(const session_id_t& session, actor_zeta::address_t dispatcher) -> void;

        auto append_database(const command_t& command) -> void;
        auto remove_database(const command_t& command) -> void;

        auto append_collection(const command_t& command) -> void;
        auto remove_collection(const command_t& command) -> void;

        auto write_documents(const command_t& command) -> void;
        auto remove_documents(const command_t& command) -> void;

        auto fix_wal_id(wal::id_t wal_id) -> void;

        auto make_type() const noexcept -> const char*;

        actor_zeta::behavior_t behavior();

    private:
        // Behaviors
        actor_zeta::behavior_t load_;
        actor_zeta::behavior_t append_database_;
        actor_zeta::behavior_t remove_database_;
        actor_zeta::behavior_t append_collection_;
        actor_zeta::behavior_t remove_collection_;
        actor_zeta::behavior_t write_documents_;
        actor_zeta::behavior_t remove_documents_;
        actor_zeta::behavior_t fix_wal_id_;

        const name_t name_;
        log_t log_;
        disk_t disk_;
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk
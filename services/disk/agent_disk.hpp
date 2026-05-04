#pragma once

#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/detail/future.hpp>

#include <components/log/log.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <filesystem>
#include <services/wal/base.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace services::disk {

    using path_t = std::filesystem::path;
    using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;

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

        auto make_type() const noexcept -> const char*;

        unique_future<void> fix_wal_id(wal::id_t wal_id);

        using dispatch_traits = actor_zeta::dispatch_traits<&agent_disk_t::fix_wal_id>;

        actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg);

    private:
        const name_t name_;
        log_t log_;
        path_t path_;
        core::filesystem::local_file_system_t fs_;
        file_ptr file_wal_id_;
    };

    using agent_disk_ptr = std::unique_ptr<agent_disk_t, actor_zeta::pmr::deleter_t>;
} //namespace services::disk

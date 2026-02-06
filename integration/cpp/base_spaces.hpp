#pragma once

#include "wrapper_dispatcher.hpp"
#include <components/configuration/configuration.hpp>
#include <components/log/log.hpp>
#include <core/executor.hpp>
#include <actor-zeta/detail/memory.hpp>

#include <core/file/file_system.hpp>

#include <memory>

namespace services {

    namespace dispatcher {
        class manager_dispatcher_t;
        using manager_dispatcher_ptr = std::unique_ptr<manager_dispatcher_t, actor_zeta::pmr::deleter_t>;
    } // namespace dispatcher

    namespace disk {
        class manager_disk_t;
        using manager_disk_ptr = std::unique_ptr<manager_disk_t, actor_zeta::pmr::deleter_t>;
        class manager_disk_empty_t;
        using manager_disk_empty_ptr = std::unique_ptr<manager_disk_empty_t, actor_zeta::pmr::deleter_t>;
    } // namespace disk

    namespace wal {
        class manager_wal_replicate_t;
        class manager_wal_replicate_empty_t;
        using manager_wal_ptr = std::unique_ptr<manager_wal_replicate_t, actor_zeta::pmr::deleter_t>;
        using manager_wal_empty_ptr = std::unique_ptr<manager_wal_replicate_empty_t, actor_zeta::pmr::deleter_t>;
    } // namespace wal

} // namespace services

namespace otterbrix {

    class base_otterbrix_t {
    public:
        base_otterbrix_t(base_otterbrix_t& other) = delete;
        void operator=(const base_otterbrix_t&) = delete;

        log_t& get_log();
        otterbrix::wrapper_dispatcher_t* dispatcher();
        ~base_otterbrix_t();

    protected:
        explicit base_otterbrix_t(const configuration::config& config);
        std::filesystem::path main_path_;
        std::pmr::synchronized_pool_resource resource;
        log_t log_;
        actor_zeta::scheduler_ptr scheduler_;
        actor_zeta::scheduler_ptr scheduler_dispatcher_;
        services::dispatcher::manager_dispatcher_ptr manager_dispatcher_;
        std::variant<std::monostate, services::disk::manager_disk_empty_ptr, services::disk::manager_disk_ptr>
            manager_disk_;
        std::variant<std::monostate, services::wal::manager_wal_empty_ptr, services::wal::manager_wal_ptr> manager_wal_;
        std::unique_ptr<otterbrix::wrapper_dispatcher_t, actor_zeta::pmr::deleter_t> wrapper_dispatcher_;
        actor_zeta::scheduler_ptr scheduler_disk_;

    private:
        inline static std::unordered_set<std::filesystem::path, core::filesystem::path_hash> paths_ = {};
        inline static std::mutex m_;
    };

} // namespace otterbrix

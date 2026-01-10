#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <core/excutor.hpp>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/memory_storage/memory_storage.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace otterbrix {

    using services::dispatcher::manager_dispatcher_t;

    base_otterbrix_t::base_otterbrix_t(const configuration::config& config)
        : main_path_(config.main_path)
        , resource(std::pmr::synchronized_pool_resource())
        , scheduler_(new actor_zeta::shared_work(1, 1000))
        , scheduler_dispatcher_(new actor_zeta::shared_work(1, 1000))
        , manager_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_disk_()
        , manager_wal_()
        , wrapper_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , memory_storage_(nullptr, actor_zeta::pmr::deleter_t(&resource)) {
        log_ = initialization_logger("python", config.log.path.c_str());
        log_.set_level(config.log.level);
        trace(log_, "spaces::spaces()");
        {
            std::lock_guard lock(m_);
            if (paths_.find(main_path_) == paths_.end()) {
                paths_.insert(main_path_);
            } else {
                throw std::runtime_error("otterbrix instance has to have unique directory");
            }
        }

        ///scheduler_.reset(new actor_zeta::shared_work(1, 1000), actor_zeta::detail::thread_pool_deleter());

        trace(log_, "spaces::manager_wal start");
        auto manager_wal_address = actor_zeta::address_t::empty_address();
        services::wal::manager_wal_replicate_t* wal_ptr = nullptr;
        services::wal::manager_wal_replicate_empty_t* wal_empty_ptr = nullptr;
        if (config.wal.on) {
            auto manager = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource,
                                                                                                scheduler_.get(),
                                                                                                config.wal,
                                                                                                log_);
            manager_wal_address = manager->address();
            wal_ptr = manager.get();
            manager_wal_ = std::move(manager);
        } else {
            auto manager = actor_zeta::spawn<services::wal::manager_wal_replicate_empty_t>(&resource,
                                                                                                      scheduler_.get(),
                                                                                                      log_);
            manager_wal_address = manager->address();
            wal_empty_ptr = manager.get();
            manager_wal_ = std::move(manager);
        }
        trace(log_, "spaces::manager_wal finish");

        trace(log_, "spaces::manager_disk start");
        auto manager_disk_address = actor_zeta::address_t::empty_address();
        services::disk::manager_disk_t* disk_ptr = nullptr;
        services::disk::manager_disk_empty_t* disk_empty_ptr = nullptr;
        if (config.disk.on) {
            auto manager = actor_zeta::spawn<services::disk::manager_disk_t>(&resource,
                                                                                        scheduler_.get(),
                                                                                        config.disk,
                                                                                        log_);
            manager_disk_address = manager->address();
            disk_ptr = manager.get();
            manager_disk_ = std::move(manager);
        } else {
            auto manager =
                actor_zeta::spawn<services::disk::manager_disk_empty_t>(&resource, scheduler_.get());
            manager_disk_address = manager->address();
            disk_empty_ptr = manager.get();
            manager_disk_ = std::move(manager);
        }
        trace(log_, "spaces::manager_disk finish");

        trace(log_, "spaces::memory_storage start");
        memory_storage_ = actor_zeta::spawn<services::memory_storage_t>(&resource, scheduler_.get(), log_);
        trace(log_, "spaces::memory_storage finish");

        trace(log_, "spaces::manager_dispatcher start");
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource,
                                                                                     scheduler_dispatcher_.get(),
                                                                                     log_);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ =
            actor_zeta::spawn<wrapper_dispatcher_t>(&resource, manager_dispatcher_->address(), log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        // Call sync methods directly (not through message passing)
        // Pass addresses directly - polymorphic dispatch via interface contracts
        manager_dispatcher_->sync(std::make_tuple(memory_storage_->address(),
                                                   manager_wal_address,
                                                   manager_disk_address));

        if (wal_ptr) {
            wal_ptr->sync(std::make_tuple(actor_zeta::address_t(manager_disk_address), manager_dispatcher_->address()));
            wal_ptr->create_wal_worker();
        } else {
            wal_empty_ptr->sync(std::make_tuple(actor_zeta::address_t(manager_disk_address), manager_dispatcher_->address()));
            wal_empty_ptr->create_wal_worker();
        }

        if (disk_ptr) {
            disk_ptr->sync(std::make_tuple(manager_dispatcher_->address()));
            disk_ptr->create_agent();
        } else {
            disk_empty_ptr->sync(std::make_tuple(manager_dispatcher_->address()));
            disk_empty_ptr->create_agent();
        }

        memory_storage_->sync(std::make_tuple(manager_dispatcher_->address(), actor_zeta::address_t(manager_disk_address)));

        manager_dispatcher_->create_dispatcher();
        scheduler_dispatcher_->start();
        scheduler_->start();
        trace(log_, "spaces::spaces() final");
    }

    log_t& base_otterbrix_t::get_log() { return log_; }

    wrapper_dispatcher_t* base_otterbrix_t::dispatcher() { return wrapper_dispatcher_.get(); }

    base_otterbrix_t::~base_otterbrix_t() {
        trace(log_, "delete spaces");
        scheduler_->stop();
        scheduler_dispatcher_->stop();
        std::lock_guard lock(m_);
        paths_.erase(main_path_);
    }

} // namespace otterbrix
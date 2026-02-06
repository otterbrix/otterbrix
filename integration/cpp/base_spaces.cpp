#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog.hpp>
#include <core/executor.hpp>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/loader/loader.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace otterbrix {

    using services::dispatcher::manager_dispatcher_t;

    base_otterbrix_t::base_otterbrix_t(const configuration::config& config)
        : main_path_(config.main_path)
        , resource(std::pmr::synchronized_pool_resource())
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , scheduler_dispatcher_(new actor_zeta::shared_work(3, 1000))
        , manager_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_disk_()
        , manager_wal_()
        , wrapper_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , scheduler_disk_(new actor_zeta::shared_work(3, 1000)) {
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


        trace(log_, "spaces::PHASE 1 - Loading data from disk WITHOUT actors");
        services::loader::loader_t loader(config.disk, config.wal, &resource, log_);
        auto state = loader.load();
        auto index_definitions = std::move(state.index_definitions);
        auto wal_records = std::move(state.wal_records);
        trace(log_, "spaces::PHASE 1 complete - loaded {} databases, {} collections, {} indexes, {} WAL records",
              state.databases.size(), state.documents.size(), index_definitions.size(), wal_records.size());

        trace(log_, "spaces::PHASE 2 - Creating actors");

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
                                                                                        scheduler_disk_.get(),
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

        trace(log_, "spaces::manager_dispatcher start");
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource,
                                                                                     scheduler_dispatcher_.get(),
                                                                                     log_);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ = actor_zeta::spawn<wrapper_dispatcher_t>(&resource, manager_dispatcher_->address(), log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        manager_dispatcher_->sync(std::make_tuple(manager_wal_address,manager_disk_address));

        if (wal_ptr) {
            wal_ptr->sync(std::make_tuple(actor_zeta::address_t(manager_disk_address), manager_dispatcher_->address()));
        } else {
            wal_empty_ptr->sync(std::make_tuple(actor_zeta::address_t(manager_disk_address), manager_dispatcher_->address()));
        }

        if (disk_ptr) {
            disk_ptr->sync(std::make_tuple(manager_dispatcher_->address()));
        } else {
            disk_empty_ptr->sync(std::make_tuple(manager_dispatcher_->address()));
        }

        trace(log_, "spaces::PHASE 2.2 - Populating catalog");
        if (!state.databases.empty() || !state.documents.empty()) {
            auto& catalog = manager_dispatcher_->mutable_catalog();
            for (const auto& db_name : state.databases) {
                trace(log_, "spaces::creating namespace: {}", db_name);
                components::catalog::table_namespace_t ns(&resource);
                ns.push_back(std::pmr::string(db_name.c_str(), &resource));
                catalog.create_namespace(ns);
            }
            for (const auto& [coll_name, _] : state.documents) {
                trace(log_, "spaces::creating computing table: {}.{}", coll_name.database, coll_name.collection);
                components::catalog::table_id table_id(&resource, coll_name);
                auto err = catalog.create_computing_table(table_id);
                if (err) {
                    warn(log_, "spaces::failed to create computing table {}.{}: {}",
                         coll_name.database, coll_name.collection, err.what());
                }
            }
        }

        trace(log_, "spaces::PHASE 2.3 - Initializing manager_dispatcher from loaded state");
        manager_dispatcher_->init_from_state(
            std::move(state.databases),
            std::move(state.documents),
            std::move(state.schemas));

        trace(log_, "spaces::PHASE 2.4 - Starting schedulers");
        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        trace(log_, "spaces::PHASE 2.5 - Replaying {} WAL records", wal_records.size());
        if (!wal_records.empty()) {
            auto session = components::session::session_id_t();
            for (auto& record : wal_records) {
                if (record.data) {
                    trace(log_, "spaces::replaying WAL record id {} type {}",
                          record.id, record.data->to_string());
                    auto cursor = wrapper_dispatcher_->execute_plan(session, record.data, record.params);
                    if (cursor->is_error()) {
                        warn(log_, "spaces::failed to replay WAL record {}: {}",
                             record.id, cursor->get_error().what);
                    }
                }
            }
        }
        trace(log_, "spaces::PHASE 2.5 complete");

        trace(log_, "spaces::PHASE 3 - Creating {} indexes", index_definitions.size());
        if (!index_definitions.empty()) {
            auto session = components::session::session_id_t();

            for (auto& index_def : index_definitions) {
                trace(log_, "spaces::creating index: {} on {}",
                      index_def->name(), index_def->collection_full_name().to_string());
                auto cursor = wrapper_dispatcher_->execute_plan(session, index_def, nullptr);
                if (cursor->is_error()) {
                    warn(log_, "spaces::failed to create index {}: {}",
                         index_def->name(), cursor->get_error().what);
                } else {
                    trace(log_, "spaces::index {} created successfully", index_def->name());
                }
            }
        }

        trace(log_, "spaces::PHASE 3 complete");
        trace(log_, "spaces::spaces() final");
    }

    log_t& base_otterbrix_t::get_log() { return log_; }

    wrapper_dispatcher_t* base_otterbrix_t::dispatcher() { return wrapper_dispatcher_.get(); }

    base_otterbrix_t::~base_otterbrix_t() {
        trace(log_, "delete spaces");
        scheduler_->stop();
        scheduler_dispatcher_->stop();
        scheduler_disk_->stop();
        std::lock_guard lock(m_);
        paths_.erase(main_path_);
    }

}

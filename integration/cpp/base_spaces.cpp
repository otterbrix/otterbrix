#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_types.hpp>
#include <components/catalog/schema.hpp>
#include <components/catalog/table_metadata.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <components/serialization/deserializer.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_reader.hpp>
#include <thread>

namespace otterbrix {

    using services::dispatcher::manager_dispatcher_t;

    base_otterbrix_t::base_otterbrix_t(const configuration::config& config)
        : main_path_(config.main_path)
        , resource()
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , scheduler_dispatcher_(new actor_zeta::shared_work(3, 1000))
        , manager_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_disk_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_wal_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_index_(nullptr, actor_zeta::pmr::deleter_t(&resource))
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

        std::pmr::set<collection_full_name_t> collections(&resource);
        services::wal::id_t last_wal_id{0};

        if (!config.disk.path.empty()) {
            const auto legacy_catalog_otbx = config.disk.path / "catalog.otbx";
            if (std::filesystem::exists(legacy_catalog_otbx)) {
                throw std::runtime_error(
                    "Legacy catalog format detected at " + legacy_catalog_otbx.string() +
                    ". Remove the file and restart — pg_catalog is the source of truth.");
            }
        }

        auto index_definitions = std::pmr::vector<components::logical_plan::node_create_index_ptr>(&resource);

        // Read WAL records via wal_reader_t
        services::wal::wal_reader_t wal_reader(config.wal, log_);
        auto wal_records = wal_reader.read_committed_records(last_wal_id);

        trace(log_,
              "spaces::PHASE 1 complete - loaded {} collections, {} index definitions, {} WAL records",
              collections.size(),
              index_definitions.size(),
              wal_records.size());

        trace(log_, "spaces::manager_wal start");
        auto manager_wal_address = actor_zeta::address_t::empty_address();
        services::wal::manager_wal_replicate_t* wal_ptr = nullptr;
        {
            auto manager = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource,
                                                                                     scheduler_.get(),
                                                                                     config.wal,
                                                                                     log_);
            manager_wal_address = manager->address();
            wal_ptr = manager.get();
            manager_wal_ = std::move(manager);
        }
        trace(log_, "spaces::manager_wal finish");

        trace(log_, "spaces::manager_disk start");
        auto manager_disk_address = actor_zeta::address_t::empty_address();
        services::disk::manager_disk_t* disk_ptr = nullptr;
        {
            auto manager = actor_zeta::spawn<services::disk::manager_disk_t>(&resource,
                                                                             scheduler_.get(),
                                                                             scheduler_disk_.get(),
                                                                             config.disk,
                                                                             log_);
            manager_disk_address = manager->address();
            disk_ptr = manager.get();
            manager_disk_ = std::move(manager);
        }
        trace(log_, "spaces::manager_disk finish");

        trace(log_, "spaces::manager_index start");
        manager_index_ =
            actor_zeta::spawn<services::index::manager_index_t>(&resource, scheduler_.get(), log_, config.disk.path);
        auto manager_index_address = manager_index_->address();
        trace(log_, "spaces::manager_index finish");

        trace(log_, "spaces::manager_dispatcher start");
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource, scheduler_dispatcher_.get(), log_);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ = actor_zeta::spawn<wrapper_dispatcher_t>(&resource, manager_dispatcher_->address(), log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        manager_dispatcher_->sync(std::make_tuple(manager_wal_address, manager_disk_address, manager_index_address));

        wal_ptr->sync(std::make_tuple(actor_zeta::address_t(manager_disk_address), manager_dispatcher_->address()));

        if (disk_ptr) {
            // Bring up the pg_catalog system tables before any DDL/DML can flow through
            // the actor pipeline. Disk-on mode: on fresh install, bootstrap creates the 9
            // .otbx files; on restart, load_system_tables_sync picks them up. Disk-off mode:
            // bootstrap creates in-memory pg_catalog storages so DDL paths still resolve.
            const bool disk_persisted = config.disk.on && !config.disk.path.empty();
            const auto pg_class_otbx =
                config.disk.path / "pg_catalog" / "main" / "pg_class" / "table.otbx";
            if (disk_persisted && std::filesystem::exists(pg_class_otbx)) {
                disk_ptr->load_system_tables_sync();
                // User storages are NOT pre-loaded here. WAL replay calls
                // load_storage_for_wal_replay_sync() on demand for each disk-backed table it
                // encounters, and resolve_table() lazy-loads any table not yet in storages_.
                // Startup time is O(system-tables) rather than O(all-user-tables).
            } else {
                disk_ptr->bootstrap_system_tables_sync();
            }
        }
        if (disk_ptr) {
            // Pass WAL address: disk uses this to write pg_catalog WAL records inline from
            // append_pg_catalog_row. Previously this was dispatcher's address — only worked
            // because the runtime_txn check skipped WAL writes.
            disk_ptr->sync(std::make_tuple(manager_wal_address));
        }

        manager_index_->sync(std::make_tuple(manager_disk_address));

        manager_dispatcher_->init_from_state(std::move(collections));

        // Replay physical WAL records directly to storage (before schedulers start). Group
        // by collection: pg_catalog.* records are replayed first (sequential — small volume,
        // mutates the
        // catalog the rest of restore depends on); user-table records run in parallel.
        if (disk_ptr && !wal_records.empty()) {
            std::unordered_map<collection_full_name_t, std::vector<services::wal::record_t*>, collection_name_hash>
                pg_catalog_by_collection;
            std::unordered_map<collection_full_name_t, std::vector<services::wal::record_t*>, collection_name_hash>
                user_by_collection;
            for (auto& record : wal_records) {
                if (!record.is_physical())
                    continue;
                if (record.collection_name.database == "pg_catalog") {
                    pg_catalog_by_collection[record.collection_name].push_back(&record);
                } else {
                    // Peek at the .wal_id sidecar without loading the storage.
                    // Records at or before the checkpoint are already in the .otbx — skip.
                    auto cp_id = disk_ptr->peek_checkpoint_wal_id_from_disk(record.collection_name);
                    if (cp_id > services::wal::id_t{0} && record.id <= cp_id) {
                        continue;
                    }
                    user_by_collection[record.collection_name].push_back(&record);
                }
            }

            auto replay_one = [disk_ptr](const collection_full_name_t& name,
                                          std::vector<services::wal::record_t*>& records) {
                for (auto* r : records) {
                    switch (r->record_type) {
                        case services::wal::wal_record_type::PHYSICAL_INSERT:
                            if (r->physical_data) {
                                if (name.database != "pg_catalog" && !disk_ptr->has_storage(name)) {
                                    // Try to load disk-backed .otbx first.
                                    disk_ptr->load_storage_for_wal_replay_sync(name);
                                    // Still not loaded → in-memory table; create from WAL types.
                                    if (!disk_ptr->has_storage(name)) {
                                        auto types = r->physical_data->types();
                                        std::vector<components::table::column_definition_t> cols;
                                        cols.reserve(types.size());
                                        for (const auto& t : types) {
                                            cols.emplace_back(
                                                t.has_alias() ? t.alias() : std::string{},
                                                t);
                                        }
                                        disk_ptr->create_storage_with_columns_sync(name, std::move(cols));
                                    }
                                }
                                disk_ptr->direct_append_sync(name, *r->physical_data);
                            }
                            break;
                        case services::wal::wal_record_type::PHYSICAL_DELETE:
                            disk_ptr->direct_delete_sync(name, r->physical_row_ids, r->physical_row_count);
                            break;
                        case services::wal::wal_record_type::PHYSICAL_UPDATE:
                            if (r->physical_data) {
                                disk_ptr->direct_update_sync(name, r->physical_row_ids, *r->physical_data);
                            }
                            break;
                        default:
                            break;
                    }
                }
            };

            // Replay pg_catalog records first (sequential — mutates the catalog
            // that all user-table replays depend on).
            for (auto& [name, records] : pg_catalog_by_collection) {
                replay_one(name, records);
            }

            // Replay user collections in parallel.
            std::vector<std::thread> workers;
            workers.reserve(user_by_collection.size());
            for (auto& [name, records] : user_by_collection) {
                workers.emplace_back(
                    [&replay_one, &name, &records] { replay_one(name, records); });
            }
            for (auto& w : workers) {
                w.join();
            }
            // Re-aggregate counts for trace below.
            std::unordered_map<collection_full_name_t, std::vector<services::wal::record_t*>, collection_name_hash>
                by_collection;
            by_collection.reserve(pg_catalog_by_collection.size() + user_by_collection.size());
            for (auto& kv : pg_catalog_by_collection) by_collection.emplace(std::move(kv));
            for (auto& kv : user_by_collection) by_collection.emplace(std::move(kv));

            uint64_t physical_count = 0;
            for (auto& [name, records] : by_collection) {
                physical_count += records.size();
            }
            if (physical_count > 0) {
                trace(log_,
                      "spaces::replayed {} physical WAL records across {} collections in parallel",
                      physical_count,
                      by_collection.size());
            }
        }

        // Reseed after WAL replay so any OIDs minted in post-checkpoint WAL records
        // are included. Idempotent: seed() never lowers the counter.
        if (disk_ptr) {
            disk_ptr->restore_oid_generator_sync();
        }

        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        // NOT NULL overlays are recorded in pg_attribute (attnotnull) and applied
        // lazily by resolve_table when the storage is first loaded.
        (void)disk_ptr;

        if (!wal_records.empty()) {
            trace(log_, "spaces::PHASE 3 - Skipping {} indexes (WAL replay handled them)", index_definitions.size());
        } else if (!index_definitions.empty()) {
            auto session = components::session::session_id_t();

            for (auto& index_def : index_definitions) {
                trace(log_,
                      "spaces::creating index: {} on {}",
                      index_def->name(),
                      index_def->collection_full_name().to_string());
                auto cursor = wrapper_dispatcher_->execute_plan(session, index_def, nullptr);
                if (cursor->is_error()) {
                    warn(log_, "spaces::failed to create index {}: {}", index_def->name(), cursor->get_error().what);
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
        // Checkpoint all disk tables before shutdown
        if (wrapper_dispatcher_) {
            try {
                auto session = components::session::session_id_t();
                auto checkpoint_node = components::logical_plan::make_node_checkpoint(&resource);
                wrapper_dispatcher_->execute_plan(session, checkpoint_node, nullptr);
                trace(log_, "delete spaces: checkpoint complete");
            } catch (...) {
                // Best-effort: don't throw from destructor
            }
        }
        scheduler_->stop();
        scheduler_dispatcher_->stop();
        scheduler_disk_->stop();
        std::lock_guard lock(m_);
        paths_.erase(main_path_);
    }

} // namespace otterbrix

#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog.hpp>
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
#include <services/index/manager_index.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <thread>
#include <services/wal/wal_reader.hpp>

namespace otterbrix {

    using services::dispatcher::manager_dispatcher_t;

    namespace {

        struct collection_load_info_t {
            collection_full_name_t name;
            services::disk::table_storage_mode_t storage_mode{services::disk::table_storage_mode_t::IN_MEMORY};
            std::vector<services::disk::catalog_column_entry_t> columns;
        };

        bool is_index_valid(const std::filesystem::path& index_path) {
            if (!std::filesystem::exists(index_path) || !std::filesystem::is_directory(index_path)) {
                return false;
            }
            auto metadata_path = index_path / "metadata";
            if (!std::filesystem::exists(metadata_path)) {
                return false;
            }
            if (std::filesystem::file_size(metadata_path) == 0) {
                return false;
            }
            for (const auto& entry : std::filesystem::directory_iterator(index_path)) {
                if (entry.is_regular_file() && entry.path().filename() != "metadata") {
                    if (std::filesystem::file_size(entry.path()) == 0) {
                        return false;
                    }
                }
            }
            return true;
        }

        std::pmr::vector<components::logical_plan::node_create_index_ptr>
        read_index_definitions(const std::filesystem::path& disk_path,
                               std::pmr::memory_resource* resource,
                               log_t& log) {
            std::pmr::vector<components::logical_plan::node_create_index_ptr> defs(resource);
            auto indexes_path = disk_path / services::index::INDEXES_METADATA_FILENAME;
            if (!std::filesystem::exists(indexes_path)) {
                return defs;
            }
            core::filesystem::local_file_system_t fs;
            auto metafile = core::filesystem::open_file(fs, indexes_path,
                                                        core::filesystem::file_flags::READ,
                                                        core::filesystem::file_lock_type::NO_LOCK);

            constexpr auto count_byte_by_size = sizeof(size_t);
            size_t size;
            size_t offset = 0;
            std::unique_ptr<char[]> size_str(new char[count_byte_by_size]);

            while (true) {
                metafile->seek(offset);
                auto bytes_read = metafile->read(size_str.get(), count_byte_by_size);
                if (bytes_read == count_byte_by_size) {
                    offset += count_byte_by_size;
                    std::memcpy(&size, size_str.get(), count_byte_by_size);

                    std::pmr::string buf(resource);
                    buf.resize(size);
                    metafile->read(buf.data(), size, offset);
                    offset += size;

                    components::serializer::msgpack_deserializer_t deserializer(buf);
                    deserializer.advance_array(0);
                    auto index_ptr = components::logical_plan::node_create_index_t::deserialize(&deserializer);
                    deserializer.pop_array();

                    auto index_path = disk_path / index_ptr->collection_full_name().database /
                                      index_ptr->collection_full_name().collection / index_ptr->name();
                    if (is_index_valid(index_path)) {
                        debug(log, "read_index_definitions: found valid index: {} on {}",
                              index_ptr->name(), index_ptr->collection_full_name().to_string());
                        defs.push_back(std::move(index_ptr));
                    } else {
                        warn(log, "read_index_definitions: skipping corrupted index: {} on {}",
                             index_ptr->name(), index_ptr->collection_full_name().to_string());
                    }
                } else {
                    break;
                }
            }
            return defs;
        }

    } // anonymous namespace

    base_otterbrix_t::base_otterbrix_t(const configuration::config& config)
        : main_path_(config.main_path)
        , resource()
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , scheduler_dispatcher_(new actor_zeta::shared_work(3, 1000))
        , manager_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_disk_()
        , manager_wal_()
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

        // PHASE 1: Read catalog from disk (no actors needed)
        std::pmr::set<database_name_t> databases(&resource);
        std::pmr::set<collection_full_name_t> collections(&resource);
        std::vector<collection_load_info_t> collection_infos;
        std::vector<std::pair<database_name_t, services::disk::catalog_sequence_entry_t>> sequences;
        std::vector<std::pair<database_name_t, services::disk::catalog_view_entry_t>> views;
        std::vector<std::pair<database_name_t, services::disk::catalog_macro_entry_t>> macros;
        services::wal::id_t last_wal_id{0};

        std::unique_ptr<services::disk::disk_t> disk;
        if (!config.disk.path.empty() && std::filesystem::exists(config.disk.path)) {
            disk = std::make_unique<services::disk::disk_t>(config.disk.path, &resource);

            auto db_names = disk->databases();
            for (const auto& db_name : db_names) {
                databases.insert(db_name);
                auto table_entries = disk->table_entries(db_name);
                for (const auto& entry : table_entries) {
                    collection_full_name_t full_name(db_name, entry.name);
                    collections.insert(full_name);
                    collection_load_info_t info;
                    info.name = full_name;
                    info.storage_mode = entry.storage_mode;
                    info.columns = entry.columns;
                    collection_infos.push_back(std::move(info));
                }
                for (auto& seq : disk->catalog().sequences(db_name)) {
                    sequences.emplace_back(db_name, std::move(seq));
                }
                for (auto& view : disk->catalog().views(db_name)) {
                    views.emplace_back(db_name, std::move(view));
                }
                for (auto& macro : disk->catalog().macros(db_name)) {
                    macros.emplace_back(db_name, std::move(macro));
                }
            }
            last_wal_id = disk->wal_id();
        }

        auto index_definitions = config.disk.path.empty()
            ? std::pmr::vector<components::logical_plan::node_create_index_ptr>(&resource)
            : read_index_definitions(config.disk.path, &resource, log_);

        // Read WAL records via wal_reader_t
        services::wal::wal_reader_t wal_reader(config.wal, &resource, log_);
        auto wal_records = wal_reader.read_committed_records(last_wal_id);

        trace(log_, "spaces::PHASE 1 complete - loaded {} databases, {} collections, {} index definitions, {} WAL records",
              databases.size(), collections.size(), index_definitions.size(), wal_records.size());

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

        trace(log_, "spaces::manager_index start");
        manager_index_ = actor_zeta::spawn<services::index::manager_index_t>(
            &resource, scheduler_.get(), log_, config.disk.path);
        auto manager_index_address = manager_index_->address();
        trace(log_, "spaces::manager_index finish");

        trace(log_, "spaces::manager_dispatcher start");
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource,
                                                                                     scheduler_dispatcher_.get(),
                                                                                     log_);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ =
            actor_zeta::spawn<wrapper_dispatcher_t>(&resource, manager_dispatcher_->address(), log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        manager_dispatcher_->sync(std::make_tuple(manager_wal_address,
                                                   manager_disk_address,
                                                   manager_index_address));

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

        manager_index_->sync(std::make_tuple(manager_disk_address));

        if (!databases.empty() || !collections.empty()) {
            auto& catalog = manager_dispatcher_->mutable_catalog();
            for (const auto& db_name : databases) {
                trace(log_, "spaces::creating namespace: {}", db_name);
                components::catalog::table_namespace_t ns(&resource);
                ns.push_back(std::pmr::string(db_name.c_str(), &resource));
                catalog.create_namespace(ns);
            }

            // Use collection_infos for distinguishing in-memory vs disk tables
            for (const auto& info : collection_infos) {
                components::catalog::table_id table_id(&resource, info.name);

                if (info.storage_mode == services::disk::table_storage_mode_t::IN_MEMORY) {
                    trace(log_, "spaces::creating computing table: {}.{}", info.name.database, info.name.collection);
                    auto err = catalog.create_computing_table(table_id);
                    if (err) {
                        warn(log_, "spaces::failed to create computing table {}.{}: {}",
                             info.name.database, info.name.collection, err.what());
                    }
                } else {
                    trace(log_, "spaces::creating disk table: {}.{} ({} columns)",
                          info.name.database, info.name.collection, info.columns.size());
                    // Build schema from catalog columns
                    using namespace components::types;
                    using namespace components::catalog;
                    std::vector<complex_logical_type> schema_cols;
                    std::vector<field_description> descs;
                    schema_cols.reserve(info.columns.size());
                    descs.reserve(info.columns.size());
                    for (size_t i = 0; i < info.columns.size(); ++i) {
                        auto col_type = complex_logical_type(info.columns[i].type);
                        col_type.set_alias(info.columns[i].name);
                        schema_cols.push_back(std::move(col_type));
                        descs.push_back(field_description(static_cast<field_id_t>(i)));
                    }
                    auto sch = schema(&resource,
                                      create_struct("schema", schema_cols, std::move(descs)));
                    auto err = catalog.create_table(table_id,
                                                    table_metadata(&resource, std::move(sch)));
                    if (err) {
                        warn(log_, "spaces::failed to create disk table {}.{}: {}",
                             info.name.database, info.name.collection, err.what());
                    }
                }
            }
        }

        // Create storages in manager_disk_t for loaded collections
        if (disk_ptr) {
            for (const auto& info : collection_infos) {
                if (info.storage_mode == services::disk::table_storage_mode_t::IN_MEMORY) {
                    disk_ptr->create_storage_sync(info.name);
                } else {
                    auto otbx_path = config.disk.path / info.name.database / "main" / info.name.collection / "table.otbx";
                    disk_ptr->load_storage_disk_sync(info.name, otbx_path);
                }
            }
        }

        // Register loaded collections in manager_index_t
        for (const auto& full_name : collections) {
            auto session = components::session::session_id_t();
            manager_index_->register_collection_sync(session, full_name);
        }

        // Log loaded catalog DDL objects (sequences, views, macros)
        if (!sequences.empty()) {
            trace(log_, "spaces::loaded {} sequences from catalog", sequences.size());
        }
        if (!views.empty()) {
            trace(log_, "spaces::loaded {} views from catalog", views.size());
        }
        if (!macros.empty()) {
            trace(log_, "spaces::loaded {} macros from catalog", macros.size());
        }

        trace(log_, "spaces::PHASE 2.3 - Initializing manager_dispatcher from loaded state");
        manager_dispatcher_->init_from_state(
            std::move(databases),
            std::move(collections));

        // Replay physical WAL records directly to storage (before schedulers start)
        // Group by collection and replay per-collection in parallel for faster recovery
        if (disk_ptr && !wal_records.empty()) {
            std::unordered_map<collection_full_name_t,
                               std::vector<services::wal::record_t*>,
                               collection_name_hash> by_collection;
            for (auto& record : wal_records) {
                if (!record.is_physical()) continue;
                by_collection[record.collection_name].push_back(&record);
            }

            std::vector<std::thread> workers;
            workers.reserve(by_collection.size());
            for (auto& [name, records] : by_collection) {
                workers.emplace_back([disk_ptr, &name, &records] {
                    for (auto* r : records) {
                        switch (r->record_type) {
                            case services::wal::wal_record_type::PHYSICAL_INSERT:
                                if (r->physical_data) {
                                    disk_ptr->direct_append_sync(name, *r->physical_data);
                                }
                                break;
                            case services::wal::wal_record_type::PHYSICAL_DELETE:
                                disk_ptr->direct_delete_sync(name, r->physical_row_ids,
                                                             r->physical_row_count);
                                break;
                            case services::wal::wal_record_type::PHYSICAL_UPDATE:
                                if (r->physical_data) {
                                    disk_ptr->direct_update_sync(name, r->physical_row_ids,
                                                                 *r->physical_data);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                });
            }
            for (auto& w : workers) {
                w.join();
            }

            uint64_t physical_count = 0;
            for (auto& [name, records] : by_collection) {
                physical_count += records.size();
            }
            if (physical_count > 0) {
                trace(log_, "spaces::replayed {} physical WAL records across {} collections in parallel",
                      physical_count, by_collection.size());
            }
        }

        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        // Overlay NOT NULL constraints from catalog onto storage column definitions.
        if (disk_ptr) {
            for (const auto& info : collection_infos) {
                for (const auto& col : info.columns) {
                    if (col.not_null) {
                        disk_ptr->overlay_column_not_null_sync(info.name, col.name);
                    }
                }
            }
        }

        if (!wal_records.empty()) {
            trace(log_, "spaces::PHASE 3 - Skipping {} indexes (WAL replay handled them)", index_definitions.size());
        } else if (!index_definitions.empty()) {
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

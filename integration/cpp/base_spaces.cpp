#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/index_agent_disk.hpp>
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

        services::wal::id_t last_wal_id{0};

        if (!config.disk.path.empty()) {
            const auto legacy_catalog_otbx = config.disk.path / "catalog.otbx";
            if (std::filesystem::exists(legacy_catalog_otbx)) {
                throw std::runtime_error("Legacy catalog format detected at " + legacy_catalog_otbx.string() +
                                         ". Remove the file and restart — pg_catalog is the source of truth.");
            }
        }

        auto index_definitions = std::pmr::vector<components::logical_plan::node_create_index_ptr>(&resource);

        // Read WAL records via wal_reader_t
        services::wal::wal_reader_t wal_reader(config.wal, log_);
        auto wal_records = wal_reader.read_committed_records(last_wal_id);

        trace(log_,
              "spaces::PHASE 1 complete - loaded {} index definitions, {} WAL records",
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
        manager_index_ = actor_zeta::spawn<services::index::manager_index_t>(&resource,
                                                                             scheduler_.get(),
                                                                             log_,
                                                                             config.disk.path,
                                                                             config.disk.bitcask_flush_threshold,
                                                                             config.disk.bitcask_segment_record_limit,
                                                                             config.disk.btree_flush_threshold);
        auto manager_index_address = manager_index_->address();
        trace(log_, "spaces::manager_index finish");

        trace(log_, "spaces::manager_dispatcher start");
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource, scheduler_dispatcher_.get(), log_);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ = actor_zeta::spawn<wrapper_dispatcher_t>(&resource,
                                                                       manager_dispatcher_.get(),
                                                                       scheduler_dispatcher_.get(),
                                                                       log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        // When WAL is disabled, pass empty_address so all wal_address_ != empty()
        // guards in dispatcher and disk manager skip every WAL round-trip at no cost.
        auto effective_wal_address = config.wal.on ? manager_wal_address : actor_zeta::address_t::empty_address();

        manager_dispatcher_->sync(std::make_tuple(effective_wal_address, manager_disk_address, manager_index_address));

        wal_ptr->sync(std::make_tuple(actor_zeta::address_t(manager_disk_address), manager_dispatcher_->address()));

        // Publish the dispatcher address back into manager_disk / manager_index so the
        // GC-ack path (manager_disk → dispatcher → manager_wal truncate) has a
        // destination. Bootstrap-time sync direct call (pre-scheduler-start, single-threaded).
        if (disk_ptr) {
            disk_ptr->set_manager_dispatcher_sync(manager_dispatcher_->address());
        }
        manager_index_->set_manager_dispatcher_sync(manager_dispatcher_->address());

        if (disk_ptr) {
            // Bring up the pg_catalog system tables before any DDL/DML can flow through
            // the actor pipeline. bootstrap_system_tables_sync is idempotent per-table:
            // for each well_known system oid, load the existing .otbx if present, else
            // create a fresh storage. No external existence probe needed — the disk
            // actor owns the per-table decision.
            //
            // User storages are NOT pre-loaded. WAL replay calls
            // load_storage_for_wal_replay_sync on demand; resolve_table lazy-loads
            // anything still missing. Startup is O(system-tables).
            disk_ptr->bootstrap_system_tables_sync();
            // Walk config_.path for user-table .otbx files and load each.
            // Loaded storages bring their .otbx.wal_id sidecar into memory,
            // so the WAL-replay filter below can correctly skip
            // already-checkpointed records for user tables.
            disk_ptr->load_user_table_storages_sync();
        }
        if (disk_ptr) {
            // Pass WAL address: disk uses this to write pg_catalog WAL records inline from
            // append_pg_catalog_row.
            disk_ptr->sync(std::make_tuple(effective_wal_address));
        }

        manager_index_->sync(std::make_tuple(manager_disk_address));

        // Replay physical WAL records directly to storage (before schedulers start). Group
        // by oid: system-table (oid < FIRST_USER_OID) records are replayed first
        // (sequential — small volume, mutates the catalog the rest of restore depends on);
        // user-table records run in parallel.
        //
        // WAL records carry table_oid directly — no cfn-resolve roundtrip.
        if (disk_ptr && !wal_records.empty()) {
            std::unordered_map<components::catalog::oid_t, std::vector<services::wal::record_t*>> system_by_oid;
            std::unordered_map<components::catalog::oid_t, std::vector<services::wal::record_t*>> user_by_oid;
            constexpr components::catalog::oid_t main_db_oid = components::catalog::well_known_oid::main_database;
            // .otbx + sidecar are authoritative for *all* checkpointed
            // tables (system and user alike). Records at or before
            // sidecar.wal_id are already absorbed into the loaded storage;
            // replaying them would duplicate catalog rows. Tables without
            // a sidecar (cp_id == 0, never checkpointed) still replay
            // unconditionally. Cache the per-table sidecar wal_id to avoid
            // one fs read per record.
            std::unordered_map<components::catalog::oid_t, services::wal::id_t> cp_cache;
            auto cp_for = [&](components::catalog::oid_t oid) {
                auto [it, inserted] = cp_cache.try_emplace(oid);
                if (inserted)
                    it->second = disk_ptr->peek_checkpoint_wal_id_from_disk(oid, main_db_oid);
                return it->second;
            };
            for (auto& record : wal_records) {
                if (!record.is_physical())
                    continue;
                if (record.table_oid == components::catalog::INVALID_OID) {
                    continue;
                }
                auto cp_id = cp_for(record.table_oid);
                if (cp_id > services::wal::id_t{0} && record.id <= cp_id) {
                    continue;
                }
                if (record.table_oid < components::catalog::FIRST_USER_OID) {
                    system_by_oid[record.table_oid].push_back(&record);
                } else {
                    user_by_oid[record.table_oid].push_back(&record);
                }
            }

            auto replay_one = [disk_ptr](components::catalog::oid_t table_oid,
                                         std::vector<services::wal::record_t*>& records) {
                constexpr components::catalog::oid_t main_db_oid = components::catalog::well_known_oid::main_database;
                for (auto* r : records) {
                    switch (r->record_type) {
                        case services::wal::wal_record_type::PHYSICAL_INSERT:
                            if (r->physical_data) {
                                if (!disk_ptr->has_storage(table_oid)) {
                                    // Try lazy-load from .otbx; if that fails (table is
                                    // in-memory or .otbx absent), synthesise an in-memory
                                    // storage from the WAL chunk's column types.
                                    disk_ptr->load_storage_for_wal_replay_sync(table_oid, main_db_oid);
                                    if (!disk_ptr->has_storage(table_oid)) {
                                        auto types = r->physical_data->types();
                                        std::vector<components::table::column_definition_t> cols;
                                        cols.reserve(types.size());
                                        for (const auto& t : types) {
                                            cols.emplace_back(t.has_alias() ? t.alias() : std::string{}, t);
                                        }
                                        disk_ptr->create_storage_with_columns_sync(table_oid,
                                                                                   main_db_oid,
                                                                                   std::move(cols));
                                    }
                                }
                                // TODO: load timezone from settings?
                                disk_ptr->direct_append_sync(table_oid, *r->physical_data, {});
                            }
                            break;
                        case services::wal::wal_record_type::PHYSICAL_DELETE: {
                            disk_ptr->direct_delete_sync(table_oid, r->physical_row_ids, r->physical_row_count);
                            break;
                        }
                        case services::wal::wal_record_type::PHYSICAL_UPDATE:
                            if (r->physical_data) {
                                disk_ptr->direct_update_sync(table_oid, r->physical_row_ids, *r->physical_data);
                            }
                            break;
                        default:
                            break;
                    }
                }
            };

            // Replay system-table records first (sequential — mutates the catalog
            // that all user-table replays depend on).
            for (auto& [oid, records] : system_by_oid) {
                replay_one(oid, records);
            }

            // After system replay, pg_class reflects the final catalog
            // state. Drop user-table replay buckets whose oid is no longer
            // alive (table was DROPped — its pg_class row is gone and its
            // .otbx was physically removed by drop_storage). Without this
            // filter, surviving WAL INSERT records would resurrect a
            // phantom storage at the dropped oid; if the oid is later
            // recycled by re-CREATE TABLE, the new schema collides with
            // the phantom and queries return stale data.
            auto alive_user_oids = disk_ptr->alive_user_oids_sync();
            for (auto it = user_by_oid.begin(); it != user_by_oid.end();) {
                if (alive_user_oids.count(it->first) == 0) {
                    trace(log_,
                          "spaces::skipping {} WAL records for dropped user oid {}",
                          it->second.size(),
                          static_cast<unsigned>(it->first));
                    it = user_by_oid.erase(it);
                } else {
                    ++it;
                }
            }

            // Replay user tables sequentially. The parallel variant raced on
            // manager_disk_t::storages_ (unordered_map) — each worker called
            // create_storage_with_columns_sync() concurrently, and the hash
            // table is not thread-safe (TSan-confirmed). Bootstrap is a rare
            // path, so the perf hit is negligible.
            for (auto& [oid, records] : user_by_oid) {
                replay_one(oid, records);
            }

            uint64_t physical_count = 0;
            for (auto& [oid, records] : system_by_oid) physical_count += records.size();
            for (auto& [oid, records] : user_by_oid) physical_count += records.size();
            if (physical_count > 0) {
                trace(log_,
                      "spaces::replayed {} physical WAL records across {} tables",
                      physical_count,
                      system_by_oid.size() + user_by_oid.size());
            }
        }

        // Reseed after WAL replay so any OIDs minted in post-checkpoint WAL records
        // are included. Idempotent: seed() never lowers the counter.
        if (disk_ptr) {
            disk_ptr->restore_oid_generator_sync();
        }

        // Catalog scan rebuild — pg_class rows tombstoned by a pre-crash
        // DROP TABLE that didn't get to physically remove the .otbx are
        // recovered here, BEFORE scheduler.start. The scan returns
        // (oid, sentinel delete_id=1) pairs; we rebuild dropped_storages_ on
        // disk and dropped_table_agents_ on index, and flip the dispatcher's
        // per-subscriber broadcast flags so the first post-start horizon
        // advance fires on_horizon_advanced and finishes the deferred GC.
        // Bootstrap-only sync calls (schedulers haven't started, single-threaded).
        if (disk_ptr && manager_index_) {
            auto dropped_oids = disk_ptr->scan_dropped_oids_sync();
            if (!dropped_oids.empty()) {
                const auto db_root = disk_ptr->path_db();
                constexpr components::catalog::oid_t main_db_oid =
                    components::catalog::well_known_oid::main_database;
                for (auto& [oid, delete_id] : dropped_oids) {
                    // Mirrors create_storage_disk's layout:
                    //   ${db_root}/${db_oid}/${tbl_oid}/table.otbx
                    // with sidecars `table.otbx.wal_id` and `table.otbx.prev`
                    // — same files drop_storage removes on the live path.
                    auto base = db_root / std::to_string(static_cast<unsigned>(main_db_oid)) /
                                std::to_string(static_cast<unsigned>(oid));
                    auto otbx = base / "table.otbx";
                    std::pmr::vector<std::filesystem::path> sidecars{&resource};
                    {
                        auto wal_id_sidecar = otbx;
                        wal_id_sidecar += ".wal_id";
                        sidecars.push_back(std::move(wal_id_sidecar));
                    }
                    {
                        auto prev_sidecar = otbx;
                        prev_sidecar += ".prev";
                        sidecars.push_back(std::move(prev_sidecar));
                    }
                    disk_ptr->register_dropped_storage_sync(oid,
                                                            delete_id,
                                                            std::move(otbx),
                                                            std::move(sidecars));
                    manager_index_->mark_table_dropped_sync(oid, delete_id);
                }
                // Flip the dispatcher's selective-broadcast flags so the next
                // commit (the first one after scheduler.start) advances the
                // horizon past 1 and broadcasts on_horizon_advanced to disk &
                // index, draining the rebuilt dropped_storages_ / dropped_table_agents_
                // queues. We deliberately do NOT call on_horizon_advanced
                // inline: its body is a coroutine handler that uses the
                // actor's mailbox, and the mailbox isn't running yet.
                manager_dispatcher_->set_disk_has_dropped_sync(true);
                manager_dispatcher_->set_index_has_dropped_sync(true);
                trace(log_,
                      "spaces::PHASE 2c rebuilt {} dropped storage/index entries from pg_class",
                      dropped_oids.size());
            }
        }

        // Snapshot-aware WAL replay horizon.
        // Scan COMMIT records (already filtered by 2-pass committed-txn filter in
        // wal_reader_t) for the maximum commit_id and publish it once so the
        // post-recovery txn_manager_'s published_horizon_ matches the durable
        // MVCC frontier. A crash means in-flight commits were never published,
        // so we only restore the max-COMMIT horizon — never reconstruct
        // in_flight ids (crashed txns weren't visible to any snapshot anyway).
        if (!wal_records.empty()) {
            uint64_t max_commit_id = 0;
            for (const auto& r : wal_records) {
                if (r.is_commit_marker() && r.commit_id > max_commit_id) {
                    max_commit_id = r.commit_id;
                }
            }
            if (max_commit_id > 0) {
                manager_dispatcher_->set_replay_horizon_sync(max_commit_id);
                trace(log_, "spaces::WAL replay published_horizon advanced to {}", max_commit_id);
            }
        }

        // Catalog-driven index bootstrap. Walks pg_class + pg_index via
        // manager_disk_ sync helpers, then for each live table oid mints
        // an empty index_engine_t on manager_index_; for each alive
        // pg_index row spawns the persistence disk_agent and hands
        // ownership to manager_index_; for each dropped table restores
        // the tombstone so on_horizon_advanced finishes the deferred GC.
        // Single-threaded by construction (pre-scheduler-start).
        if (disk_ptr && manager_index_) {
            bootstrap_indexes_sync(config.disk);
        }

        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        // NOT NULL overlays are recorded in pg_attribute (attnotnull) and applied
        // lazily by resolve_table when the storage is first loaded.
        (void) disk_ptr;

        if (!wal_records.empty()) {
            trace(log_, "spaces::PHASE 3 - Skipping {} indexes (WAL replay handled them)", index_definitions.size());
        } else if (!index_definitions.empty()) {
            auto session = components::session::session_id_t();

            for (auto& index_def : index_definitions) {
                trace(log_, "spaces::creating index: {}", index_def->name());
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

    // Catalog-driven index bootstrap.
    //
    // Pre-scheduler-start, single-threaded by construction. pg_class /
    // pg_index / dropped-row scans are already settled by the catalog
    // rebuild and user-table load above, so the helpers below see the
    // steady-state catalog.
    //
    // Done in three passes:
    //   - mint an empty index_engine_t per live table oid so subsequent
    //     bootstrap_index_sync calls find an engine to attach to
    //     (bootstrap_index_sync does not mint engines on the fly);
    //   - walk pg_index, spawn one index_agent_disk_t per alive row
    //     (mirroring manager_index_t::create_index's spawn pattern), hand
    //     ownership to manager_index_ via bootstrap_index_sync;
    //   - replay tombstones from scan_dropped_table_oids_sync so
    //     on_horizon_advanced GC drains rebuilt entries after the first
    //     post-start commit.
    //
    // Errors propagate via log+return: scan helpers return empty pmr
    // vectors on internal failure, bootstrap_index_sync logs and skips a
    // malformed row, no throw escapes this method.
    void base_otterbrix_t::bootstrap_indexes_sync(const configuration::config_disk& disk_config) {
        // engines for every live table OID.
        auto live_tables = manager_disk_->scan_live_table_oids_sync();
        for (auto oid : live_tables) {
            manager_index_->bootstrap_engine_sync(oid);
        }

        // disk agents for every alive pg_index row.
        std::size_t indexes_wired = 0;
        std::size_t indexes_skipped_unfinished = 0;
        auto index_rows = manager_disk_->scan_alive_pg_index_sync();
        for (auto& row : index_rows) {
            if (row.ready_since == 0) {
                // pg_index row exists but the backfill never committed —
                // no fallback, the operator must re-issue CREATE INDEX.
                // Drop the half-built artefact silently here; the
                // post-bootstrap catalog scan picks it up by oid.
                ++indexes_skipped_unfinished;
                continue;
            }

            // Mirror manager_index_t::create_index's spawn pattern so the
            // resulting agent is byte-for-byte equivalent to one produced
            // by the runtime DDL path. index_agent_disk_t's ctor takes
            // index_name_t = std::string; pg_index_row_t carries a
            // std::pmr::string, so we materialise a non-pmr copy here
            // (cheap — short identifier).
            auto agent = actor_zeta::spawn<services::index::index_agent_disk_t>(
                &resource,
                disk_config.path,
                row.table_oid,
                std::string(row.name.data(), row.name.size()),
                row.type,
                disk_config.bitcask_flush_threshold,
                disk_config.bitcask_segment_record_limit,
                disk_config.btree_flush_threshold,
                log_);
            auto agent_addr = agent->address();

            manager_index_->bootstrap_index_sync(row.table_oid,
                                                  std::move(row.name),
                                                  row.type,
                                                  std::move(row.keys),
                                                  agent_addr,
                                                  std::move(agent));
            ++indexes_wired;
        }

        // restore dropped tombstones from pg_class.
        auto dropped = manager_disk_->scan_dropped_table_oids_sync();
        for (auto& [oid, delete_id] : dropped) {
            manager_index_->bootstrap_dropped_sync(oid, delete_id);
        }

        // Rebuild in-memory index against post-restart storage. CHECKPOINT
        // compacts storage and renumbers physical row_ids contiguously; the
        // on-disk index btree retains pre-compact row_ids, so the
        // bootstrap_index_sync btree-load step seeds the in-memory engine
        // with stale row_ids. Without this rebuild pass, post-restart
        // equality lookups via index_scan return row_ids that no longer map
        // to live storage rows and collection_t::fetch silently drops them
        // (visible as `SELECT WHERE indexed_col = X` returning 0 instead of
        // the expected row). We rescan storage for every indexed table and
        // re-populate the engine using the current row_ids. Sync: must run
        // before the scheduler starts (same window as the bootstrap_*_sync
        // calls above). live_tables is the same oid set bootstrap_engine_sync
        // walked, so we iterate it directly rather than re-scanning pg_class.
        for (auto oid : live_tables) {
            auto chunk = manager_disk_->scan_storage_for_rebuild_sync(oid, &resource);
            if (!chunk)
                continue;
            const auto row_count = chunk->size();
            if (row_count == 0)
                continue;
            manager_index_->bootstrap_repopulate_sync(oid, std::move(chunk), row_count);
        }

        trace(log_,
              "spaces::PHASE 4 bootstrap_indexes_sync: {} engines, {} indexes wired "
              "({} skipped as unfinished), {} dropped tombstones restored",
              live_tables.size(),
              indexes_wired,
              indexes_skipped_unfinished,
              dropped.size());
    }

} // namespace otterbrix

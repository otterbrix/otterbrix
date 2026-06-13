#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdint>
#include <memory>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/disk_hash_table.hpp>
#include <services/index/index_agent_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_reader.hpp>
#include <set>
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

        // Read WAL records via wal_reader_t. Capture the union of committed txn
        // ids alongside the records: the bitcask index txn-log recover gate
        // (M1.1) needs it to discard frames belonging to transactions whose WAL
        // commit marker never landed (index txn-log frames are durable BEFORE the
        // WAL commit marker, so an uncommitted txn's index entries could otherwise
        // survive a crash). Threaded by VALUE through the single-threaded
        // pre-scheduler bootstrap window down to each bitcask agent.
        std::set<std::uint64_t> committed_txn_ids;
        services::wal::wal_reader_t wal_reader(config.wal, log_);
        auto wal_records = wal_reader.read_committed_records(last_wal_id, &committed_txn_ids);

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

        manager_dispatcher_->sync(services::dispatcher::manager_dispatcher_t::sync_pack{effective_wal_address,
                                                                                        manager_disk_address,
                                                                                        manager_index_address});

        wal_ptr->sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(manager_disk_address),
                                                     manager_dispatcher_->address(),
                                                     manager_index_address});

        // Publish the dispatcher address into manager_disk / manager_index so the
        // GC-ack path (manager_disk → dispatcher → manager_wal truncate) has a
        // destination. Sync — pre-scheduler-start.
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
            disk_ptr->sync(services::disk::manager_disk_t::disk_sync_pack_t{effective_wal_address});
        }

        manager_index_->sync(services::index::index_sync_pack_t{manager_disk_address});

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

        // Recover pg_class rows tombstoned by a pre-crash DROP TABLE that never
        // physically removed the .otbx. The scan returns (oid, sentinel
        // delete_id=1) pairs; rebuild dropped_storages_ on disk and
        // dropped_table_agents_ on index so the first post-start horizon advance
        // finishes the deferred GC. Sync — schedulers not yet started.
        if (disk_ptr && manager_index_) {
            auto dropped_oids = disk_ptr->scan_dropped_oids_sync();
            if (!dropped_oids.empty()) {
                const auto db_root = disk_ptr->path_db();
                constexpr components::catalog::oid_t main_db_oid = components::catalog::well_known_oid::main_database;
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
                    disk_ptr->register_dropped_storage_sync(oid, delete_id, std::move(otbx), std::move(sidecars));
                    manager_index_->mark_table_dropped_sync(oid, delete_id);
                }
                // Arm the broadcast flags so the first post-start commit advances
                // the horizon and broadcasts on_horizon_advanced, draining the
                // rebuilt queues. Cannot call on_horizon_advanced inline: it is a
                // coroutine handler driven by the actor mailbox, not yet running.
                manager_dispatcher_->set_disk_has_dropped_sync(true);
                manager_dispatcher_->set_index_has_dropped_sync(true);
                trace(log_,
                      "spaces::PHASE 2c rebuilt {} dropped storage/index entries from pg_class",
                      dropped_oids.size());
            }
        }

        // Publish max COMMIT commit_id so the post-recovery txn_manager_'s
        // published_horizon_ matches the durable MVCC frontier. Only the
        // max-COMMIT horizon is restored — in_flight ids are never reconstructed,
        // since crashed in-flight txns were visible to no snapshot anyway.
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

        // Must run pre-scheduler-start while single-threaded. committed_txn_ids
        // travels by value into bootstrap_indexes_sync (and from there into each
        // spawned bitcask agent) — legal during this single-threaded bootstrap
        // window, no cross-actor sharing.
        if (disk_ptr && manager_index_) {
            bootstrap_indexes_sync(config.disk, committed_txn_ids);
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
                auto cursor = wrapper_dispatcher_->execute_plan(
                    session,
                    components::logical_plan::execution_plan_t{&resource, index_def, nullptr});
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
                wrapper_dispatcher_->execute_plan(
                    session,
                    components::logical_plan::execution_plan_t{&resource, checkpoint_node, nullptr});
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

    // Engine pass must precede the pg_index pass: bootstrap_index_sync attaches
    // to an existing index_engine_t and does not mint one on the fly.
    // Errors propagate via log+return — scan helpers return empty on internal
    // failure, bootstrap_index_sync skips malformed rows, no throw escapes.
    void base_otterbrix_t::bootstrap_indexes_sync(const configuration::config_disk& disk_config,
                                                  const std::set<std::uint64_t>& committed_txn_ids) {
        auto live_tables = manager_disk_->scan_live_table_oids_sync();
        for (auto oid : live_tables) {
            manager_index_->bootstrap_engine_sync(oid);
        }

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

            // Spawn args must match manager_index_t::create_index so the agent is
            // equivalent to one from the runtime DDL path. Ctor takes a non-pmr
            // index_name_t (std::string) but row.name is pmr::string, hence the copy.
            const auto index_name = std::string(row.name.data(), row.name.size());
            services::index::disk_hash_table_ptr shared_hash_storage;
            if (row.type == components::logical_plan::index_type::hashed && !disk_config.path.empty()) {
                const auto base = disk_config.path / std::to_string(static_cast<unsigned>(row.table_oid)) / index_name;
                std::filesystem::create_directories(base);
                try {
                    shared_hash_storage = boost::intrusive_ptr(new services::index::disk_hash_table_t(
                        base / "hash_index.bin",
                        services::index::disk_hash_table_t::default_bucket_count,
                        &resource));
                } catch (const std::exception& e) {
                    trace(log_,
                          "bootstrap_indexes_sync: disk hash storage init failed for {}: {}",
                          index_name,
                          e.what());
                }
            }

            // the WAL committed-txn set, used by the bitcask agent's
            // txn-log recover gate. Materialised here as a pmr::set on this
            // instance's resource (the resource the agent and its index store).
            // A copy of the committed ids per agent — legal value transfer during
            // the single-threaded bootstrap window.
            std::pmr::set<std::uint64_t> committed_for_agent(committed_txn_ids.begin(),
                                                             committed_txn_ids.end(),
                                                             &resource);

            auto agent = actor_zeta::spawn<services::index::index_agent_disk_t>(
                &resource,
                disk_config.path,
                row.table_oid,
                index_name,
                row.type,
                disk_config.bitcask_flush_threshold,
                disk_config.bitcask_segment_record_limit,
                disk_config.btree_flush_threshold,
                log_,
                std::move(committed_for_agent),
                shared_hash_storage);
            auto agent_addr = agent->address();

            manager_index_->bootstrap_index_sync(row.table_oid,
                                                  std::move(row.name),
                                                  row.type,
                                                  std::move(row.keys),
                                                  agent_addr,
                                                  std::move(agent),
                                                  std::move(shared_hash_storage));
            ++indexes_wired;
        }

        auto dropped = manager_disk_->scan_dropped_table_oids_sync();
        for (auto& [oid, delete_id] : dropped) {
            manager_index_->bootstrap_dropped_sync(oid, delete_id);
        }

        // Rebuild the in-memory index against post-restart storage. CHECKPOINT
        // renumbers physical row_ids contiguously, but the on-disk btree retains
        // pre-compact ids, so the bootstrap_index_sync load step seeds the engine
        // with stale ids. Without this rescan, post-restart equality lookups
        // return row_ids that no longer map to live rows and collection_t::fetch
        // silently drops them (SELECT WHERE indexed_col = X returns 0 rows).
        // Sync — same pre-scheduler-start window as the bootstrap_*_sync calls.
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

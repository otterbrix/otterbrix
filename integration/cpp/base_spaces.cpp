#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
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
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_reader.hpp>
#include <set>
#include <thread>

namespace otterbrix {

    using services::dispatcher::manager_dispatcher_t;

    base_otterbrix_t::base_otterbrix_t(const configuration::config& config,
                                       services::planner::create_plan_rule_t create_plan_rule,
                                       components::planner::optimizer_pass_t optimizer_pass)
        : main_path_(config.main_path)
        , resource()
        , scheduler_(new actor_zeta::shared_work(3, 1000))
        , scheduler_dispatcher_(new actor_zeta::shared_work(3, 1000))
        , scheduler_disk_(new actor_zeta::shared_work(3, 1000))
        , manager_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_disk_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_wal_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , manager_index_(nullptr, actor_zeta::pmr::deleter_t(&resource))
        , wrapper_dispatcher_(nullptr, actor_zeta::pmr::deleter_t(&resource)) {
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

        trace(log_, "spaces::PHASE 1 complete - {} WAL records", wal_records.size());

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
        manager_dispatcher_ = actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource,
                                                                                            scheduler_dispatcher_.get(),
                                                                                            log_,
                                                                                            create_plan_rule,
                                                                                            optimizer_pass);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ = actor_zeta::spawn<wrapper_dispatcher_t>(&resource,
                                                                      manager_dispatcher_.get(),
                                                                      scheduler_dispatcher_.get(),
                                                                      log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        // When WAL is disabled, pass empty_address so all wal_address_ != empty()
        // guards in dispatcher and disk manager skip every WAL round-trip at no cost.
        auto effective_wal_address = config.wal.on ? manager_wal_address : actor_zeta::address_t::empty_address();

        manager_dispatcher_->sync(
            services::dispatcher::manager_dispatcher_t::sync_pack{effective_wal_address,
                                                                  manager_disk_address,
                                                                  manager_index_address,
                                                                  config.execution.dml_flush_row_threshold});

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
            // B1a: every user table is disk-backed, so an alive pg_class row
            // whose .otbx is missing means the file was lost (the directory
            // entry of a freshly created .otbx is not fsynced, so a crash can
            // durably keep the catalog row while losing the file). Recreate the
            // missing .otbx from the pg_attribute columns so catalog and storage
            // agree again. Without this, a reopened session's CREATE TABLE IF
            // NOT EXISTS finds the table "exists" and skips creating storage,
            // resolve_table returns a schema, but every INSERT silently no-ops
            // (storage_append returns 0,0) and scans see nothing. Runs after
            // load_user_table_storages_sync so on-disk tables (already loaded)
            // are skipped, and before WAL replay so replayed INSERTs land in the
            // recreated storage.
            disk_ptr->rehydrate_missing_user_storages_sync();
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
            // B4: the namespace oid that names a table's on-disk directory. It is
            // `pg_class.relnamespace` — what create_storage_disk was given — and the catalog is
            // final here (system records replay first and sequentially). Every use below used to
            // substitute well_known_oid::main_database (4), which is a DATABASE oid, not a
            // namespace one, and which no user table can carry: CREATE DATABASE allocates its
            // namespace from FIRST_USER_OID upward. Cached per oid: each resolve is a pg_class
            // scan.
            std::unordered_map<components::catalog::oid_t, components::catalog::oid_t> ns_cache;
            auto ns_for = [&](components::catalog::oid_t oid) {
                auto [it, inserted] = ns_cache.try_emplace(oid);
                if (inserted) {
                    // A system table has no pg_class row of its own; its directory oid is the
                    // fixed bootstrap layout constant. A user table's comes from the catalog.
                    it->second = oid < components::catalog::FIRST_USER_OID
                                     ? services::disk::manager_disk_t::system_dir_oid()
                                     : disk_ptr->relnamespace_for_oid_sync(oid);
                }
                return it->second;
            };
            // The cache is CLEARED between the two replay phases below: this classification
            // pass runs BEFORE system records replay, so a crash image whose pg_class rows are
            // still only in the WAL answers "unknown" here and would poison the answer the user
            // phase needs. Answering "unknown" is harmless for the sidecar probe it feeds
            // (peek_checkpoint_wal_id_from_disk reads the loaded entry first, and a table with
            // no loaded entry has no sidecar to find either).
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
                    it->second = disk_ptr->peek_checkpoint_wal_id_from_disk(oid, ns_for(oid));
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

            auto replay_one = [disk_ptr, &log = log_](components::catalog::oid_t table_oid,
                                                      components::catalog::oid_t ns_oid,
                                                      std::vector<services::wal::record_t*>& records) {
                for (auto* r : records) {
                    switch (r->record_type) {
                        case services::wal::wal_record_type::PHYSICAL_INSERT:
                            if (!r->physical_data.empty()) {
                                if (!disk_ptr->has_storage(table_oid)) {
                                    // Try lazy-load from .otbx; if the file is absent
                                    // (lost with its unfsynced directory entry, or the
                                    // record predates this table's .otbx) synthesise a
                                    // DISK storage from the WAL chunk's column types at
                                    // the standard path — B1a: every table is
                                    // disk-backed, replay synthesis included.
                                    disk_ptr->load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                    if (!disk_ptr->has_storage(table_oid)) {
                                        if (ns_oid == components::catalog::INVALID_OID) {
                                            // Rule 6: the namespace names the directory the
                                            // file belongs in, and nothing in the record
                                            // implies it. Synthesising under a guessed one
                                            // writes a file the table's own resolve will
                                            // never open. Report and drop this table's
                                            // records rather than manufacture that.
                                            error(log,
                                                  "spaces::replay: table oid={} has no pg_class.relnamespace; "
                                                  "cannot place its .otbx and refusing to guess — records for "
                                                  "this table are NOT replayed",
                                                  static_cast<unsigned>(table_oid));
                                            return;
                                        }
                                        auto types = r->physical_data.front().types();
                                        std::vector<components::table::column_definition_t> cols;
                                        cols.reserve(types.size());
                                        for (const auto& t : types) {
                                            cols.emplace_back(t.has_alias() ? t.alias() : std::string{}, t);
                                        }
                                        auto otbx = disk_ptr->path_db() /
                                                    std::to_string(static_cast<unsigned>(ns_oid)) /
                                                    std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
                                        std::filesystem::create_directories(otbx.parent_path());
                                        // B1b: the synthesised storage must keep the computed
                                        // (relkind='g') flag — its columns come from the WAL
                                        // chunk, so they are NON-empty even for a computed
                                        // table, and the flag cannot be inferred from them.
                                        // pg_class is final here: system-table records replay
                                        // FIRST and sequentially, user replay is sequential
                                        // too, so the single-threaded relkind scan is safe.
                                        const bool synth_computed =
                                            disk_ptr->relkind_for_oid_sync(table_oid) ==
                                            components::catalog::relkind::computed;
                                        disk_ptr->create_storage_disk_sync(table_oid,
                                                                           ns_oid,
                                                                           std::move(cols),
                                                                           otbx,
                                                                           synth_computed);
                                    }
                                }
                                // TODO: load timezone from settings?
                                for (auto& chunk : r->physical_data) {
                                    disk_ptr->direct_append_sync(table_oid, chunk);
                                }
                            }
                            break;
                        case services::wal::wal_record_type::PHYSICAL_ADD_COLUMN:
                            // Schema-growth record: add the new columns before the
                            // dependent PHYSICAL_INSERT (higher wal_id, so replays after
                            // this). Storage must exist first — load .otbx or synthesise
                            // it from the schema chunk's column types.
                            if (!r->physical_data.empty()) {
                                if (!disk_ptr->has_storage(table_oid)) {
                                    disk_ptr->load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                    if (!disk_ptr->has_storage(table_oid)) {
                                        if (ns_oid == components::catalog::INVALID_OID) {
                                            // Same refusal as the PHYSICAL_INSERT branch: no
                                            // namespace, no directory, no guessing (rule 6).
                                            error(log,
                                                  "spaces::replay: table oid={} has no pg_class.relnamespace; "
                                                  "cannot place its .otbx and refusing to guess — records for "
                                                  "this table are NOT replayed",
                                                  static_cast<unsigned>(table_oid));
                                            return;
                                        }
                                        auto types = r->physical_data.front().types();
                                        std::vector<components::table::column_definition_t> cols;
                                        cols.reserve(types.size());
                                        for (const auto& t : types) {
                                            cols.emplace_back(t.has_alias() ? t.alias() : std::string{}, t);
                                        }
                                        // B1a: synthesise DISK storage (standard path),
                                        // mirroring the PHYSICAL_INSERT branch above —
                                        // including the B1b relkind-derived computed flag.
                                        auto otbx = disk_ptr->path_db() /
                                                    std::to_string(static_cast<unsigned>(ns_oid)) /
                                                    std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
                                        std::filesystem::create_directories(otbx.parent_path());
                                        const bool synth_computed =
                                            disk_ptr->relkind_for_oid_sync(table_oid) ==
                                            components::catalog::relkind::computed;
                                        disk_ptr->create_storage_disk_sync(table_oid,
                                                                           ns_oid,
                                                                           std::move(cols),
                                                                           otbx,
                                                                           synth_computed);
                                        // create_* already seeded these columns; nothing
                                        // more to add for a freshly-synthesised storage.
                                        break;
                                    }
                                }
                                disk_ptr->direct_add_column_sync(table_oid, r->physical_data.front());
                            }
                            break;
                        case services::wal::wal_record_type::PHYSICAL_DELETE: {
                            disk_ptr->direct_delete_sync(table_oid, r->physical_row_ids, r->physical_row_count);
                            break;
                        }
                        case services::wal::wal_record_type::PHYSICAL_UPDATE:
                            if (!r->physical_data.empty()) {
                                // physical_row_ids is flat across the batch; slice it per
                                // chunk in vector order to match each chunk's rows.
                                std::size_t id_base = 0;
                                for (auto& chunk : r->physical_data) {
                                    const std::size_t n = chunk.size();
                                    std::pmr::vector<int64_t> ids(r->physical_row_ids.get_allocator().resource());
                                    ids.reserve(n);
                                    for (std::size_t i = 0; i < n && id_base + i < r->physical_row_ids.size(); ++i) {
                                        ids.push_back(r->physical_row_ids[id_base + i]);
                                    }
                                    id_base += n;
                                    disk_ptr->direct_update_sync(table_oid, ids, chunk);
                                }
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
                replay_one(oid, ns_for(oid), records);
            }

            // pg_class is final only now (that is exactly what the system phase above just
            // established), so every namespace answered before this point was answered against
            // a partial catalog. Drop them and re-resolve for the user phase.
            ns_cache.clear();

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
            // the synchronous storage-create path concurrently, and the hash
            // table is not thread-safe (TSan-confirmed). Bootstrap is a rare
            // path, so the perf hit is negligible.
            for (auto& [oid, records] : user_by_oid) {
                replay_one(oid, ns_for(oid), records);
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

        // A7.6 post-replay walk: the pre-replay walk DEFERS any never-checkpointed .otbx
        // whose catalog rows still sat in the WAL (a table created, never checkpointed,
        // crashed — its schema exists only as replayed pg_attribute rows). Now that replay
        // has repopulated the catalog, walk the user-table directories again: already-loaded
        // oids are skipped (has_storage), deferred young files open as legitimately empty
        // DISK tables with their catalog schema. Without this second walk such a table would
        // answer queries through the storage-less record branch — empty by accident, and any
        // later CHECKPOINT would never reach its .otbx.
        if (disk_ptr) {
            disk_ptr->load_user_table_storages_sync();
        }

        // B3c2: re-derive any column drop whose physical release a crash discarded. B3c1's
        // commit path names the dropped column's blocks in memory and B3c's checkpoint releases
        // them; a crash in between loses that set while the disk keeps BOTH durable facts — the
        // pg_attribute tombstone and the still-present column — so the table reloads with the
        // column back and nothing else can ever re-derive the drop.
        //
        // Placement is the argument, and both halves of the comparison land exactly here:
        //   * STORAGE — every user .otbx is open: the pre-replay walk plus the post-replay one
        //     immediately above, which is what picks up the deferred young files;
        //   * CATALOG — pg_attribute is final only now. The tombstone reaches the .otbx only at
        //     a catalog checkpoint, and in the crash this exists for it is typically still
        //     WAL-only, so it becomes visible in the system-table replay above.
        // Earlier would INVERT the comparison, not merely weaken it: an ALTER ADD COLUMN whose
        // pg_attribute row is still unreplayed would look like a drop of a surviving column,
        // and the replayed PHYSICAL_INSERT chunks still carry the pre-drop column count and
        // need a table that still has it. Later would be after bootstrap_indexes_sync, whose
        // rebuild scan would then feed the index a layout no post-start scan ever sees.
        // Single-threaded, pre-scheduler-start; the release itself happens at the next
        // checkpoint, exactly as on the live path.
        if (disk_ptr) {
            disk_ptr->rearm_dropped_column_blocks_sync();
        }

        // Reseed after WAL replay so any OIDs minted in post-checkpoint WAL records
        // are included. Idempotent: seed() never lowers the counter.
        if (disk_ptr) {
            disk_ptr->restore_oid_generator_sync();
        }

        // Re-seed the MVCC commit clock on reopen from a SINGLE combined durable
        // frontier so its two halves (current_timestamp_ and published_horizon_)
        // can never disagree. The frontier is the max of two durable sources:
        //   * the persisted pg_attribute commit-ids (added_at/dropped_at) — the
        //     checkpointed catalog frontier (covers ALTER-touched schemas), and
        //   * the max WAL COMMIT-marker commit_id replayed this boot — the durable
        //     MVCC frontier for plain CREATE TABLE / data loads (the SSB case,
        //     where pg_attribute carries no commit-id so the persisted scan is 0).
        // restore_commit_clock raises current_timestamp_ to frontier+1 AND
        // published_horizon_ to frontier together: persisted columns stay visible,
        // post-recovery snapshots see persisted commits as published, AND fresh
        // post-reopen INSERTs draw commit-ids strictly above the durable band so
        // they are never mis-judged invisible. Mirror restore_oid_generator_sync:
        // single-threaded bootstrap (schedulers not started), a one-time direct
        // call, not ongoing cross-actor sharing.
        if (disk_ptr) {
            uint64_t reopen_frontier = disk_ptr->max_persisted_commit_id_sync();
            for (const auto& r : wal_records) {
                if (r.is_commit_marker() && r.commit_id > reopen_frontier) {
                    reopen_frontier = r.commit_id;
                }
            }
            if (reopen_frontier > 0) {
                manager_dispatcher_->seed_commit_clock_sync(reopen_frontier);
                trace(log_, "spaces::restored MVCC commit clock from durable frontier {}", reopen_frontier);
            }
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
                for (const auto& row : dropped_oids) {
                    // Mirrors create_storage_disk's layout:
                    //   ${db_root}/${relnamespace}/${tbl_oid}/table.otbx
                    // with sidecar `table.otbx.wal_id`
                    // — same files drop_storage removes on the live path. B4: the namespace
                    // oid comes off the tombstoned pg_class row (scan_dropped_oids_sync reads
                    // it there because an ordinary catalog read omits deleted rows); this used
                    // to substitute well_known_oid::main_database and swept a directory no
                    // user table is ever in, so the .otbx of a crash-interrupted DROP was
                    // never actually removed.
                    auto base = db_root / std::to_string(static_cast<unsigned>(row.namespace_oid)) /
                                std::to_string(static_cast<unsigned>(row.oid));
                    auto otbx = base / "table.otbx";
                    std::pmr::vector<std::filesystem::path> sidecars{&resource};
                    {
                        auto wal_id_sidecar = otbx;
                        wal_id_sidecar += ".wal_id";
                        sidecars.push_back(std::move(wal_id_sidecar));
                    }
                    disk_ptr->register_dropped_storage_sync(row.oid,
                                                            row.delete_id,
                                                            std::move(otbx),
                                                            std::move(sidecars));
                    manager_index_->mark_table_dropped_sync(row.oid, row.delete_id);
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

        // NOTE: the post-recovery MVCC commit clock (both current_timestamp_ and
        // published_horizon_) is restored ABOVE from the combined durable frontier
        // (max of persisted pg_attribute commit-ids and the max WAL COMMIT marker).
        // in_flight ids are never reconstructed — crashed in-flight txns were
        // visible to no snapshot anyway.

        // Must run pre-scheduler-start while single-threaded. committed_txn_ids
        // travels by value into bootstrap_indexes_sync (and from there into each
        // bitcask agent the index manager raises) — legal during this single-threaded
        // bootstrap window, no cross-actor sharing.
        if (disk_ptr && manager_index_) {
            bootstrap_indexes_sync(committed_txn_ids);
        }

        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        // NOT NULL overlays are recorded in pg_attribute (attnotnull) and applied
        // lazily by resolve_table when the storage is first loaded.
        //
        // No index re-creation here: on-disk indexes were re-attached from their
        // pg_index rows by bootstrap_indexes_sync above (the retired
        // metadata-file replay used to run in this spot).

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

    // The table pass must precede the pg_index pass: bootstrap_index_sync attaches to a
    // table the index manager already knows about and does not register one on the fly.
    // Errors propagate as VALUES — scan helpers return empty on internal failure,
    // bootstrap_index_sync returns the reason a row could not be brought up and this loop
    // logs it and moves on; no throw escapes.
    void base_otterbrix_t::bootstrap_indexes_sync(const std::set<std::uint64_t>& committed_txn_ids) {
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

            // NOTHING IS SPAWNED HERE ANY MORE. There are two agent classes now, one per
            // storage family, so "pick a class from pg_index.indtype" became real code —
            // and a second copy of it at this call site would be a second place to keep in
            // step with the catalog. The index manager owns that decision
            // (manager_index_t::spawn_disk_agent) and raises the agent inside
            // bootstrap_index_sync, from ITS OWN configured thresholds, which is what makes
            // a bootstrapped index and a runtime-created one the same object.
            //
            // What survives at this site is the DECISION taken from the failure: it comes
            // back as the returned core::error_t, so this loop cannot proceed without
            // having looked at it.
            //
            // committed_txn_ids: the WAL committed-txn set, used by the hashed family's
            // txn-log recover gate. Materialised here as a pmr::set on this instance's
            // resource (the resource the agent and its index store use). A copy per index —
            // legal value transfer during the single-threaded bootstrap window.
            std::pmr::set<std::uint64_t> committed_for_agent(committed_txn_ids.begin(),
                                                             committed_txn_ids.end(),
                                                             &resource);

            auto wire_error = manager_index_->bootstrap_index_sync(row.table_oid,
                                                                   row.oid,
                                                                   row.type,
                                                                   std::move(row.keys),
                                                                   std::move(committed_for_agent));
            if (wire_error.contains_error()) {
                // Skip the WHOLE index: an index whose storage will not open costs a full
                // scan, whereas aborting costs the whole engine its start. Nothing was
                // registered, no address published, nothing scheduled — and the table stays
                // readable.
                error(log_,
                      "bootstrap_indexes_sync: index_oid={} left unregistered: {}",
                      static_cast<unsigned>(row.oid),
                      wire_error.what);
                ++indexes_skipped_unfinished;
                continue;
            }
            ++indexes_wired;
        }

        auto dropped = manager_disk_->scan_dropped_table_oids_sync();
        for (const auto& row : dropped) {
            // Index bookkeeping is keyed by table oid alone; the row's namespace oid names the
            // .otbx directory and is only of interest to the storage sweep.
            manager_index_->bootstrap_dropped_sync(row.oid, row.delete_id);
        }

        // THE POST-RESTART INDEX REBUILD PASS IS GONE FROM HERE, and what it removes is
        // WORK, not behaviour. It scanned EVERY live table in full and handed the rows to
        // manager_index_t::bootstrap_repopulate_sync, which — once the last in-memory
        // index went away — cleared the facades' pending buckets (empty at bootstrap),
        // re-staged every row into bucket 0, and then committed bucket 0, which for a
        // disk-backed facade only ERASED it. A provable no-op, paid for with a full scan
        // of every table on every start.
        //
        // WHAT IT WAS SUPPOSED TO FIX IS STILL BROKEN AND IS NOT FIXED HERE: CHECKPOINT
        // compaction renumbers physical row_ids from 0 while the on-disk index keeps the
        // pre-compact ones, so a post-restart lookup can name a row id that no longer maps
        // to a live row (collection_t::fetch drops it). Repairing that means clearing and
        // rebuilding the AGENT's store, which is a mailbox round trip, and this window
        // runs before the scheduler starts. It belongs to the runtime repopulate path
        // (manager_index_t::repopulate_table), not to bootstrap.

        trace(log_,
              "spaces::PHASE 4 bootstrap_indexes_sync: {} engines, {} indexes wired "
              "({} skipped: unfinished build or unopenable storage), {} dropped tombstones restored",
              live_tables.size(),
              indexes_wired,
              indexes_skipped_unfinished,
              dropped.size());
    }

} // namespace otterbrix

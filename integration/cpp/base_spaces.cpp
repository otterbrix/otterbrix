#include "base_spaces.hpp"
#include <actor-zeta.hpp>
#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <core/executor.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/pipeline_bypass.hpp>
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

        // Every refusal below throws out of the constructor, so ~base_otterbrix_t never runs to
        // erase the path — without this guard a leaked entry makes the directory permanently
        // unopenable. Disarmed at the end once construction succeeds.
        struct path_registration_guard_t {
            std::filesystem::path path;
            bool armed{true};
            ~path_registration_guard_t() {
                if (armed) {
                    std::lock_guard lock(m_);
                    paths_.erase(path);
                }
            }
        } path_guard{main_path_};

        services::wal::id_t last_wal_id{0};

        if (!config.disk.path.empty()) {
            const auto legacy_catalog_otbx = config.disk.path / "catalog.otbx";
            if (std::filesystem::exists(legacy_catalog_otbx)) {
                throw std::runtime_error("Legacy catalog format detected at " + legacy_catalog_otbx.string() +
                                         ". Remove the file and restart — pg_catalog is the source of truth.");
            }
        }

        // committed_txn_ids feeds the bitcask index recover gate: index txn-log frames are
        // durable BEFORE the WAL commit marker, so an uncommitted txn's index entries must be
        // discarded using this set.
        std::set<std::uint64_t> committed_txn_ids;
        services::wal::wal_reader_t wal_reader(&resource, config.wal, log_);
        auto wal_records_result = wal_reader.read_committed_records(last_wal_id, &committed_txn_ids);

        // Refuse to start rather than come up with a gap: the id allocator (global_id_,
        // last_crc_) is derived from this same read, so records it could not see would leave
        // it BELOW ids already on disk and the first post-start write would reuse them. Refusal
        // writes/deletes nothing (truncate_before also refuses on this segment rather than
        // unlinking it), so the next start can retry the replay in full.
        if (wal_records_result.has_error()) {
            error(log_,
                  "spaces::startup REFUSED , the WAL could not be replayed in full: {}",
                  wal_records_result.error().what);
            throw std::runtime_error("WAL replay could not read a segment, refusing to start: " +
                                     std::string(wal_records_result.error().what.c_str()));
        }
        auto wal_records = std::move(wal_records_result.value());

        trace(log_, "spaces::PHASE 1 complete - {} WAL records", wal_records.size());

        trace(log_, "spaces::manager_wal start");
        manager_wal_ = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource,
                                                                                 scheduler_.get(),
                                                                                 config.wal,
                                                                                 log_);
        auto& wal = *manager_wal_;
        const auto manager_wal_address = wal.address();
        trace(log_, "spaces::manager_wal finish");

        trace(log_, "spaces::manager_disk start");
        manager_disk_ = actor_zeta::spawn<services::disk::manager_disk_t>(&resource,
                                                                          scheduler_.get(),
                                                                          scheduler_disk_.get(),
                                                                          config.disk,
                                                                          log_);
        // References, not nullable pointers: both managers are spawned unconditionally, so no
        // null state exists to guard.
        auto& disk = *manager_disk_;
        const auto manager_disk_address = disk.address();
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
        // The WAL mailbox is deliberately absent when the WAL is off, so every wal-address
        // guard in the dispatcher and the disk manager skips the WAL round-trip at no cost.
        const auto effective_wal_address =
            config.wal.on ? manager_wal_address : components::pipeline::no_mailbox();
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource,
                                                                          scheduler_dispatcher_.get(),
                                                                          log_,
                                                                          effective_wal_address,
                                                                          manager_disk_address,
                                                                          manager_index_address,
                                                                          config.execution.dml_flush_row_threshold,
                                                                          create_plan_rule,
                                                                          optimizer_pass);
        trace(log_, "spaces::manager_dispatcher finish");

        wrapper_dispatcher_ = actor_zeta::spawn<wrapper_dispatcher_t>(&resource,
                                                                      manager_dispatcher_.get(),
                                                                      scheduler_dispatcher_.get(),
                                                                      log_);
        trace(log_, "spaces::manager_dispatcher create dispatcher");

        wal.sync(services::wal::wal_sync_pack_t{actor_zeta::address_t(manager_disk_address),
                                                manager_dispatcher_->address(),
                                                manager_index_address});

        // Publish the dispatcher address into manager_disk / manager_index so the
        // GC-ack path (manager_disk → dispatcher → manager_wal truncate) has a
        // destination. Sync — pre-scheduler-start.
        disk.set_manager_dispatcher_sync(manager_dispatcher_->address());
        manager_index_->set_manager_dispatcher_sync(manager_dispatcher_->address());

        // Bring up the pg_catalog system tables before any DDL/DML can flow through
        // the actor pipeline. bootstrap_system_tables_sync is idempotent per-table:
        // for each well_known system oid, load the existing .otbx if present, else
        // create a fresh storage. No external existence probe needed — the disk
        // actor owns the per-table decision.
        // User storages are NOT pre-loaded. WAL replay calls
        // load_storage_for_wal_replay_sync on demand; resolve_table lazy-loads
        // anything still missing. Startup is O(system-tables).
        disk.bootstrap_system_tables_sync();
        // Walk config_.path for user-table .otbx files and load each.
        // Loaded storages bring their .otbx.wal_id sidecar into memory,
        // so the WAL-replay filter below can correctly skip
        // already-checkpointed records for user tables.
        disk.load_user_table_storages_sync();
        // A .otbx can be lost after crash even though its pg_class row survived (the
        // directory entry of a freshly created .otbx is not fsynced). Recreate the missing
        // storage from pg_attribute so catalog and storage agree; without this a reopened
        // CREATE TABLE IF NOT EXISTS finds the table "exists" and every INSERT silently
        // no-ops. Must run after load_user_table_storages_sync and before WAL replay.
        auto rehydrated = disk.rehydrate_missing_user_storages_sync();
        if (rehydrated.has_error()) {
            // Reported separately from the count below: folded in, a walk that never ran
            // would read as 0 divergences, indistinguishable from nothing wrong.
            error(log_,
                  "spaces::open: the rehydrate walk did not run, so no catalog/storage divergence was "
                  "examined: {}",
                  rehydrated.error().what);
        } else if (rehydrated.value() > 0) {
            // Non-fatal: none of these can be repaired here, and refusing would repeat every
            // boot over the same catalog. Each table is already named by the walk.
            error(log_,
                  "spaces::open: {} alive catalog table(s) came up with no storage behind them",
                  rehydrated.value());
        }

        // Pass WAL address: disk uses this to write pg_catalog WAL records inline from
        // append_pg_catalog_row.
        disk.sync(services::disk::manager_disk_t::disk_sync_pack_t{effective_wal_address});

        manager_index_->sync(services::index::index_sync_pack_t{manager_disk_address});

        // Replay physical WAL records directly to storage (before schedulers start). Group
        // by oid: system-table (oid < FIRST_USER_OID) records are replayed first
        // (sequential — small volume, mutates the catalog the rest of restore depends on);
        // user-table records run in parallel.
        // WAL records carry table_oid directly — no cfn-resolve roundtrip.
        if (!wal_records.empty()) {
            std::unordered_map<components::catalog::oid_t, std::vector<services::wal::record_t*>> system_by_oid;
            std::unordered_map<components::catalog::oid_t, std::vector<services::wal::record_t*>> user_by_oid;
            // Namespace oid = pg_class.relnamespace, NOT well_known_oid::main_database (that is
            // a DATABASE oid, never a namespace one). Cached per oid: each resolve is a pg_class
            // scan.
            std::unordered_map<components::catalog::oid_t, components::catalog::oid_t> ns_cache;
            auto ns_for = [&](components::catalog::oid_t oid) {
                auto [it, inserted] = ns_cache.try_emplace(oid);
                if (inserted) {
                    // A system table has no pg_class row of its own; its directory oid is the
                    // fixed bootstrap layout constant. A user table's comes from the catalog.
                    it->second = oid < components::catalog::FIRST_USER_OID
                                     ? services::disk::manager_disk_t::system_dir_oid()
                                     : disk.relnamespace_for_oid_sync(oid);
                }
                return it->second;
            };
            // cp_id==0 means never checkpointed (replay all); >0 skips records already absorbed
            // by the checkpointed .otbx. Cache is cleared between the system/user replay phases
            // (below) because this classification pass runs before system replay, when pg_class
            // is still partial. A checkpoint floor that fails to read is NOT treated as 0 — 0
            // would mean "replay everything" and re-apply rows already in the checkpointed file
            // — its records are dropped instead.
            std::unordered_map<components::catalog::oid_t, services::wal::id_t> cp_cache;
            std::unordered_set<components::catalog::oid_t> cp_unreadable;
            auto cp_for = [&](components::catalog::oid_t oid) -> services::wal::id_t {
                if (cp_unreadable.count(oid) != 0) {
                    return services::wal::id_t{0};
                }
                auto [it, inserted] = cp_cache.try_emplace(oid);
                if (inserted) {
                    // No namespace + no storage means the table was created since the last
                    // checkpoint (pg_class row still WAL-only) — checkpoint_all writes pg_class
                    // and the table together, so there is no checkpoint and 0 is correct here.
                    const auto ns_oid = ns_for(oid);
                    if (ns_oid == components::catalog::INVALID_OID && !disk.has_storage(oid)) {
                        it->second = services::wal::id_t{0};
                        return it->second;
                    }
                    auto probed = disk.peek_checkpoint_wal_id_from_disk(oid, ns_oid);
                    if (probed.has_error()) {
                        error(log_,
                              "spaces::replay: table oid={} has no readable checkpoint floor ({}) — its records are "
                              "NOT replayed, because replaying them could re-apply rows the checkpointed file "
                              "already holds",
                              static_cast<unsigned>(oid),
                              probed.error().what);
                        cp_unreadable.insert(oid);
                        cp_cache.erase(oid);
                        return services::wal::id_t{0};
                    }
                    it->second = probed.value();
                }
                return it->second;
            };
            for (auto& record : wal_records) {
                if (!record.is_physical())
                    continue;
                if (record.table_oid == components::catalog::INVALID_OID) {
                    continue;
                }
                auto cp_id = cp_for(record.table_oid);
                if (cp_unreadable.count(record.table_oid) != 0) {
                    continue;
                }
                if (cp_id > services::wal::id_t{0} && record.id <= cp_id) {
                    continue;
                }
                if (record.table_oid < components::catalog::FIRST_USER_OID) {
                    system_by_oid[record.table_oid].push_back(&record);
                } else {
                    user_by_oid[record.table_oid].push_back(&record);
                }
            }

            // Declared bypass — see core/pipeline_bypass.hpp. Legal only here: runs inside
            // base_otterbrix_t's constructor before any scheduler starts, so there is no
            // planner/executor/transaction pipeline to bypass yet. Writes are stamped
            // transaction_data{0,0} (committed-for-everyone) and journal nothing, so calling this
            // from a running engine would corrupt snapshots and lose data on crash. Storage
            // synthesis also mutates manager_disk_t::storages_ with no lock but the
            // single-threadedness of this window — the parallel variant of the replay below was
            // TSan-confirmed racing on it.
            auto replay_one = core::maintenance::pipeline_bypass<
                core::maintenance::bypass_site::wal_replay_storage_synthesis>(
                [&disk, &log = log_](components::catalog::oid_t table_oid,
                                        components::catalog::oid_t ns_oid,
                                        std::vector<services::wal::record_t*>& records) {
                    for (auto* r : records) {
                        switch (r->record_type) {
                            case services::wal::wal_record_type::PHYSICAL_INSERT:
                                if (!r->physical_data.empty()) {
                                    if (!disk.has_storage(table_oid)) {
                                        // A file that failed to load is not a file that is
                                        // absent: creating a storage at the same path would
                                        // overwrite an .otbx that holds committed rows, so the
                                        // loader's error is checked before synthesising.
                                        if (auto load_err =
                                                disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                            load_err.contains_error()) {
                                            error(log,
                                                  "spaces::replay: table oid={} has a file that did not load ({}) — "
                                                  "records for this table are NOT replayed, and no storage is "
                                                  "created over it",
                                                  static_cast<unsigned>(table_oid),
                                                  load_err.what);
                                            return;
                                        }
                                        if (!disk.has_storage(table_oid)) {
                                            if (ns_oid == components::catalog::INVALID_OID) {
                                                // No namespace, no directory: a guessed one would
                                                // write a file the table's own resolve never opens.
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
                                            auto otbx = disk.path_db() /
                                                        std::to_string(static_cast<unsigned>(ns_oid)) /
                                                        std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
                                            std::filesystem::create_directories(otbx.parent_path());
                                            // The synthesised storage must keep the computed
                                            // (relkind='g') flag — WAL chunk columns are non-empty
                                            // even for a computed table, so the flag can't be
                                            // inferred from them. An unreadable relkind is refused
                                            // rather than defaulted to 'r'.
                                            auto relkind_r = disk.relkind_for_oid_sync(table_oid);
                                            if (relkind_r.has_error()) {
                                                error(log,
                                                      "spaces::replay: table oid={} has no readable relkind ({}) — "
                                                      "refusing to synthesise a storage whose kind is a guess; "
                                                      "records for this table are NOT replayed",
                                                      static_cast<unsigned>(table_oid),
                                                      relkind_r.error().what);
                                                return;
                                            }
                                            const bool synth_computed =
                                                relkind_r.value() == components::catalog::relkind::computed;
                                            if (auto synth_err = disk.create_storage_disk_sync(table_oid,
                                                                                                    ns_oid,
                                                                                                    std::move(cols),
                                                                                                    otbx,
                                                                                                    synth_computed);
                                                synth_err.contains_error()) {
                                                error(log,
                                                      "spaces::replay: table oid={} could not be synthesised ({}) — "
                                                      "records for this table are NOT replayed",
                                                      static_cast<unsigned>(table_oid),
                                                      synth_err.what);
                                                return;
                                            }
                                        }
                                    }
                                    for (auto& chunk : r->physical_data) {
                                        // The appended row's start index is 0 both on a refusal
                                        // and for a fresh table's first row; only the error
                                        // channel tells them apart.
                                        if (auto append_r = disk.direct_append_sync(table_oid, chunk);
                                            append_r.has_error()) {
                                            error(log,
                                                  "spaces::replay: {} committed row(s) for table oid={} were not "
                                                  "restored: {}",
                                                  chunk.size(),
                                                  static_cast<unsigned>(table_oid),
                                                  append_r.error().what);
                                        }
                                    }
                                }
                                break;
                            case services::wal::wal_record_type::PHYSICAL_ADD_COLUMN:
                                // Schema-growth record: add the new columns before the
                                // dependent PHYSICAL_INSERT (higher wal_id, so replays after
                                // this). Storage must exist first — load .otbx or synthesise
                                // it from the schema chunk's column types.
                                if (!r->physical_data.empty()) {
                                    if (!disk.has_storage(table_oid)) {
                                        // A file that failed to load is not absent; don't
                                        // overwrite an .otbx holding committed rows.
                                        if (auto load_err =
                                                disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                            load_err.contains_error()) {
                                            error(log,
                                                  "spaces::replay: table oid={} has a file that did not load ({}) — "
                                                  "records for this table are NOT replayed, and no storage is "
                                                  "created over it",
                                                  static_cast<unsigned>(table_oid),
                                                  load_err.what);
                                            return;
                                        }
                                        if (!disk.has_storage(table_oid)) {
                                            if (ns_oid == components::catalog::INVALID_OID) {
                                                // Same refusal as the PHYSICAL_INSERT branch above.
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
                                            // Mirrors the PHYSICAL_INSERT branch above.
                                            auto otbx = disk.path_db() /
                                                        std::to_string(static_cast<unsigned>(ns_oid)) /
                                                        std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
                                            std::filesystem::create_directories(otbx.parent_path());
                                            // Same two refusals as the PHYSICAL_INSERT branch.
                                            auto relkind_r = disk.relkind_for_oid_sync(table_oid);
                                            if (relkind_r.has_error()) {
                                                error(log,
                                                      "spaces::replay: table oid={} has no readable relkind ({}) — "
                                                      "refusing to synthesise a storage whose kind is a guess; "
                                                      "records for this table are NOT replayed",
                                                      static_cast<unsigned>(table_oid),
                                                      relkind_r.error().what);
                                                return;
                                            }
                                            const bool synth_computed =
                                                relkind_r.value() == components::catalog::relkind::computed;
                                            if (auto synth_err = disk.create_storage_disk_sync(table_oid,
                                                                                                    ns_oid,
                                                                                                    std::move(cols),
                                                                                                    otbx,
                                                                                                    synth_computed);
                                                synth_err.contains_error()) {
                                                error(log,
                                                      "spaces::replay: table oid={} could not be synthesised ({}) — "
                                                      "records for this table are NOT replayed",
                                                      static_cast<unsigned>(table_oid),
                                                      synth_err.what);
                                                return;
                                            }
                                            // create_* already seeded these columns; nothing
                                            // more to add for a freshly-synthesised storage.
                                            break;
                                        }
                                    }
                                    if (auto add_err =
                                            disk.direct_add_column_sync(table_oid, r->physical_data.front());
                                        add_err.contains_error()) {
                                        error(log, "spaces::replay: {}", add_err.what);
                                    }
                                }
                                break;
                            case services::wal::wal_record_type::PHYSICAL_DELETE: {
                                // A DELETE record carries only row ids, so there is no chunk to
                                // synthesise a table from — the most this can do is load an
                                // existing .otbx. If that still leaves no storage the delete
                                // cannot be applied, and that must be logged: silence here would
                                // leave rows the WAL says are deleted alive after recovery.
                                if (!disk.has_storage(table_oid)) {
                                    if (auto load_err =
                                            disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                        load_err.contains_error()) {
                                        error(log, "spaces::replay: {}", load_err.what);
                                    }
                                }
                                if (auto del_err = disk.direct_delete_sync(table_oid,
                                                                                r->physical_row_ids,
                                                                                r->physical_row_count);
                                    del_err.contains_error()) {
                                    error(log, "spaces::replay: {}", del_err.what);
                                }
                                break;
                            }
                            case services::wal::wal_record_type::PHYSICAL_UPDATE:
                                if (!r->physical_data.empty()) {
                                    // Same load-first rule as the DELETE branch above.
                                    if (!disk.has_storage(table_oid)) {
                                        if (auto load_err =
                                                disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                            load_err.contains_error()) {
                                            error(log, "spaces::replay: {}", load_err.what);
                                        }
                                    }
                                    // physical_row_ids is flat across the batch; slice it per
                                    // chunk in vector order. A torn/damaged record can name fewer
                                    // ids than rows — data_table_t::update reads ids by the
                                    // chunk's row count, so a short id list is truncated onto the
                                    // chunk (rows with ids restored, the rest reported as lost)
                                    // rather than silently under- or over-reading.
                                    std::size_t id_base = 0;
                                    for (auto& chunk : r->physical_data) {
                                        const std::size_t n = chunk.size();
                                        const std::size_t have = r->physical_row_ids.size() > id_base
                                                                     ? r->physical_row_ids.size() - id_base
                                                                     : 0;
                                        const std::size_t take = std::min(n, have);
                                        if (take < n) {
                                            error(log,
                                                  "spaces::replay: PHYSICAL_UPDATE for table oid={} carries {} "
                                                  "row(s) in a chunk but only {} row id(s) for them; {} committed "
                                                  "row update(s) are NOT replayed",
                                                  static_cast<unsigned>(table_oid),
                                                  n,
                                                  take,
                                                  n - take);
                                        }
                                        id_base += n;
                                        if (take == 0) {
                                            continue;
                                        }
                                        std::pmr::vector<int64_t> ids(r->physical_row_ids.get_allocator().resource());
                                        ids.reserve(take);
                                        for (std::size_t i = 0; i < take; ++i) {
                                            ids.push_back(r->physical_row_ids[id_base - n + i]);
                                        }
                                        if (take < n) {
                                            chunk.set_cardinality(take);
                                        }
                                        if (auto upd_err = disk.direct_update_sync(table_oid, ids, chunk);
                                            upd_err.contains_error()) {
                                            error(log, "spaces::replay: {}", upd_err.what);
                                        }
                                    }
                                }
                                break;
                            default:
                                break;
                        }
                    }
                });

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
            auto alive_user_oids = disk.alive_user_oids_sync();
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

        // Second walk: the pre-replay walk deferred any never-checkpointed table whose catalog
        // row still sat only in the WAL. Now that replay has repopulated pg_class, re-walk user
        // directories to open those files against their catalog schema (already-loaded oids are
        // skipped via has_storage).
        disk.load_user_table_storages_sync();

        // Re-derive any column drop whose physical release a crash discarded: the commit path
        // marks blocks for release and the checkpoint frees them, so a crash in between leaves
        // both the pg_attribute tombstone AND the still-present column on disk, and nothing else
        // re-derives that. Must run here: every user .otbx is now open (storage side final) and
        // pg_attribute has just been replayed (catalog side final) — earlier would see an
        // ALTER ADD COLUMN whose row isn't replayed yet as a false drop; later (after
        // bootstrap_indexes_sync) opens index stores against a schema this pass would still change.
        disk.rearm_dropped_column_blocks_sync();

        // Reseed after WAL replay so any OIDs minted in post-checkpoint WAL records
        // are included. Idempotent: seed() never lowers the counter.
        disk.restore_oid_generator_sync();

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
        // Unlike the wal-id allocator, this bound is taken only over REPLAYED commit markers,
        // not re-derived from files past a CRC break: a commit id past a break is observable in
        // no reopened state (replayed rows carry no commit id, checkpointed rows carry no
        // version info), so raising the clock over it would be decoding data no reader ever
        // sees. A later repair of the segment replays those markers and raises the clock then.
        uint64_t reopen_frontier = disk.max_persisted_commit_id_sync();
        for (const auto& r : wal_records) {
            if (r.is_commit_marker() && r.commit_id > reopen_frontier) {
                reopen_frontier = r.commit_id;
            }
        }
        if (reopen_frontier > 0) {
            manager_dispatcher_->seed_commit_clock_sync(reopen_frontier);
            trace(log_, "spaces::restored MVCC commit clock from durable frontier {}", reopen_frontier);
        }

        // Recover pg_class rows tombstoned by a pre-crash DROP TABLE that never
        // physically removed the .otbx. The scan returns (oid, sentinel
        // delete_id=1) pairs; rebuild dropped_storages_ on disk and
        // dropped_table_agents_ on index so the first post-start horizon advance
        // finishes the deferred GC. Sync — schedulers not yet started.
        auto dropped_oids = disk.scan_dropped_oids_sync();
        if (!dropped_oids.empty()) {
            const auto db_root = disk.path_db();
            for (const auto& row : dropped_oids) {
                // Mirrors create_storage_disk's layout: ${db_root}/${relnamespace}/${tbl_oid}
                // /table.otbx + table.otbx.wal_id. namespace_oid comes off the tombstoned
                // pg_class row (an ordinary catalog read omits deleted rows).
                auto base = db_root / std::to_string(static_cast<unsigned>(row.namespace_oid)) /
                            std::to_string(static_cast<unsigned>(row.oid));
                auto otbx = base / "table.otbx";
                std::pmr::vector<std::filesystem::path> sidecars{&resource};
                {
                    auto wal_id_sidecar = otbx;
                    wal_id_sidecar += ".wal_id";
                    sidecars.push_back(std::move(wal_id_sidecar));
                }
                disk.register_dropped_storage_sync(row.oid, row.delete_id, std::move(otbx), std::move(sidecars));
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

        // NOTE: the post-recovery MVCC commit clock (both current_timestamp_ and
        // published_horizon_) is restored ABOVE from the combined durable frontier
        // (max of persisted pg_attribute commit-ids and the max WAL COMMIT marker).
        // in_flight ids are never reconstructed — crashed in-flight txns were
        // visible to no snapshot anyway.

        // Must run pre-scheduler-start while single-threaded. committed_txn_ids
        // travels by value into bootstrap_indexes_sync (and from there into each
        // bitcask agent the index manager raises) — legal during this single-threaded
        // bootstrap window, no cross-actor sharing.
        bootstrap_indexes_sync(committed_txn_ids);

        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        // NOT NULL overlays are recorded in pg_attribute (attnotnull) and applied
        // lazily by resolve_table when the storage is first loaded.
        // No index re-creation here: on-disk indexes were re-attached from their
        // pg_index rows by bootstrap_indexes_sync above.

        trace(log_, "spaces::PHASE 3 complete");
        trace(log_, "spaces::spaces() final");
        // Construction succeeded: the destructor owns the registration from here on.
        path_guard.armed = false;
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
                // A failed final checkpoint means the next start replays the journal instead of
                // nothing; a destructor has no caller to answer, so log it instead.
                auto cursor = wrapper_dispatcher_->execute_plan(
                    session,
                    components::logical_plan::execution_plan_t{&resource, checkpoint_node, nullptr});
                if (!cursor) {
                    error(log_,
                          "delete spaces , the shutdown checkpoint answered NO cursor , whether the journal "
                          "was folded into storage is unknown");
                } else if (cursor->is_error()) {
                    error(log_,
                          "delete spaces , the shutdown checkpoint FAILED , the journal is NOT folded into "
                          "storage and the next start replays it: {}",
                          cursor->get_error().what);
                } else {
                    trace(log_, "delete spaces: checkpoint complete");
                }
            } catch (...) {
                // A destructor must not throw — but it must not be silent either.
                error(log_,
                      "delete spaces , the shutdown checkpoint THREW , whether the journal was folded into "
                      "storage is unknown");
            }
        }
        scheduler_->stop();
        scheduler_dispatcher_->stop();
        scheduler_disk_->stop();
        std::lock_guard lock(m_);
        paths_.erase(main_path_);
    }

    // The table pass must precede the pg_index pass: bootstrap_index_sync attaches to a table
    // the index manager already knows about, it does not register one on the fly.
    void base_otterbrix_t::bootstrap_indexes_sync(const std::set<std::uint64_t>& committed_txn_ids) {
        auto live_tables = manager_disk_->scan_live_table_oids_sync();
        for (auto oid : live_tables) {
            manager_index_->bootstrap_engine_sync(oid);
        }

        std::size_t indexes_wired = 0;
        std::size_t indexes_skipped_unfinished = 0;
        std::size_t indexes_skipped_unopenable = 0;
        std::size_t indexes_skipped_unrebuilt = 0;

        // A compacting round arms this marker before renumbering and clears it per table only
        // once that table's rebuild has force_flushed; anything left in it names an index whose
        // store still holds pre-compact row ids. See manager_index_t::rebuild_marker_path_.
        const auto pending_rebuilds = manager_index_->pending_index_rebuilds_sync();
        const auto rebuild_is_owed = [&pending_rebuilds](components::catalog::oid_t table_oid,
                                                         components::catalog::oid_t index_oid) {
            for (const auto& entry : pending_rebuilds) {
                if (entry.table_oid == table_oid && entry.index_oid == index_oid) {
                    return true;
                }
            }
            return false;
        };

        auto index_rows = manager_disk_->scan_alive_pg_index_sync();
        for (auto& row : index_rows) {
            if (rebuild_is_owed(row.table_oid, row.oid)) {
                // Not wired: this store names PRE-COMPACT physical row ids over a table that was
                // renumbered — wiring it would silently answer with whatever row slid into the
                // stale id. No rebuild is attempted here (this runs before schedulers start); a
                // fresh CREATE INDEX is the fix.
                error(log_,
                      "bootstrap_indexes_sync: pg_index row (indexrelid={}, indrelid={}) was left naming "
                      "PRE-COMPACT row ids by a checkpoint that did not finish its index rebuild — the index is "
                      "NOT wired, queries on the table fall back to full scans; DROP INDEX and re-issue CREATE "
                      "INDEX to rebuild it",
                      static_cast<unsigned>(row.oid),
                      static_cast<unsigned>(row.table_oid));
                ++indexes_skipped_unrebuilt;
                continue;
            }
            if (row.ready_since == 0) {
                // pg_index row exists but the backfill never committed — no fallback, the
                // operator must re-issue CREATE INDEX.
                error(log_,
                      "bootstrap_indexes_sync: pg_index row (indexrelid={}, indrelid={}) has an uncommitted "
                      "backfill (indisvalid=false) — the index is NOT wired, queries on the table fall back "
                      "to full scans; re-issue CREATE INDEX (or DROP INDEX the leftover)",
                      static_cast<unsigned>(row.oid),
                      static_cast<unsigned>(row.table_oid));
                ++indexes_skipped_unfinished;
                continue;
            }

            // No agent is spawned here: manager_index_t::spawn_disk_agent owns picking the agent
            // class from pg_index.indtype, so a bootstrapped index and a runtime-created one stay
            // the same object.
            // committed_txn_ids: the WAL committed-txn set for the hashed family's txn-log
            // recover gate, copied per index (legal value transfer, single-threaded bootstrap).
            std::pmr::set<std::uint64_t> committed_for_agent(committed_txn_ids.begin(),
                                                             committed_txn_ids.end(),
                                                             &resource);

            auto wire_error = manager_index_->bootstrap_index_sync(row.table_oid,
                                                                   row.oid,
                                                                   row.type,
                                                                   std::move(row.keys),
                                                                   std::move(committed_for_agent));
            if (wire_error.contains_error()) {
                // Skip the whole index rather than abort: a full scan costs less than the
                // engine failing to start, and the table stays readable.
                error(log_,
                      "bootstrap_indexes_sync: index_oid={} left unregistered: {}",
                      static_cast<unsigned>(row.oid),
                      wire_error.what);
                ++indexes_skipped_unopenable;
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

        // No index is rebuilt on restart: a stale store is declined above rather than repaired,
        // since repair needs a mailbox round trip the schedulers aren't running for yet.
        trace(log_,
              "spaces::PHASE 4 bootstrap_indexes_sync: {} engines, {} indexes wired "
              "({} skipped: unfinished build; {} skipped: unopenable storage; {} skipped: rebuild owed after a "
              "compaction), {} dropped tombstones restored",
              live_tables.size(),
              indexes_wired,
              indexes_skipped_unfinished,
              indexes_skipped_unopenable,
              indexes_skipped_unrebuilt,
              dropped.size());
    }

} // namespace otterbrix

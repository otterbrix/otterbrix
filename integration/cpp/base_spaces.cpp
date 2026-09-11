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

        // A refusal below throws before ~base_otterbrix_t can erase the path, leaking it forever.
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

        // An empty wal.path is not "no WAL", it is a WAL nobody can find: manager_wal_replicate_t
        // skips its recovery scan on it and total_wal_bytes() answers 0, so the auto-checkpoint
        // threshold never fires. Refused here rather than left as a second, quieter off switch.
        if (config.wal.path.empty()) {
            throw std::runtime_error("spaces::startup REFUSED , config.wal.path is empty: the WAL is the "
                                     "table's only redo record between checkpoints and has nowhere to write");
        }

        if (!config.disk.path.empty()) {
            const auto legacy_catalog_otbx = config.disk.path / "catalog.otbx";
            if (std::filesystem::exists(legacy_catalog_otbx)) {
                throw std::runtime_error("Legacy catalog format detected at " + legacy_catalog_otbx.string() +
                                         ". Remove the file and restart — pg_catalog is the source of truth.");
            }
        }

        // Index txn-log frames are durable before the WAL commit marker, so uncommitted entries need this set.
        std::set<std::uint64_t> committed_txn_ids;
        services::wal::wal_reader_t wal_reader(&resource, config.wal, log_);
        auto wal_records_result = wal_reader.read_committed_records(last_wal_id, &committed_txn_ids);

        // Refuses rather than allocating IDs below what's already on disk; writes/deletes nothing.
        if (wal_records_result.has_error()) {
            error(log_,
                  "spaces::startup REFUSED , the WAL could not be replayed in full: {}",
                  wal_records_result.error().what);
            throw std::runtime_error("WAL replay could not read a segment, refusing to start: " +
                                     std::string(wal_records_result.error().what.c_str()));
        }
        auto wal_records = std::move(wal_records_result.value());

        trace(log_, "spaces::PHASE 1 complete - {} WAL records", wal_records.size());

        // The dispatcher's own address — born last — is wired back into each manager below, post-construction.
        trace(log_, "spaces::manager_disk start");
        manager_disk_ = actor_zeta::spawn<services::disk::manager_disk_t>(&resource,
                                                                          scheduler_.get(),
                                                                          scheduler_disk_.get(),
                                                                          config.disk,
                                                                          log_);
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

        trace(log_, "spaces::manager_wal start");
        manager_wal_ = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(&resource,
                                                                                 scheduler_.get(),
                                                                                 config.wal,
                                                                                 log_,
                                                                                 manager_disk_address,
                                                                                 manager_index_address);
        auto& wal = *manager_wal_;
        const auto manager_wal_address = wal.address();
        trace(log_, "spaces::manager_wal finish");

        trace(log_, "spaces::manager_dispatcher start");
        manager_dispatcher_ =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(&resource,
                                                                          scheduler_dispatcher_.get(),
                                                                          log_,
                                                                          manager_wal_address,
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

        // Dispatcher address published into every manager: disk/index for the GC-ack path
        // (disk -> dispatcher -> wal truncate), wal for the auto-checkpoint watermark.
        disk.set_manager_dispatcher_sync(manager_dispatcher_->address());
        manager_index_->set_manager_dispatcher_sync(manager_dispatcher_->address());
        wal.set_manager_dispatcher_sync(manager_dispatcher_->address());

        disk.bootstrap_system_tables_sync();
        disk.load_user_table_storages_sync();
        // A .otbx can be lost after crash even though its pg_class row survived (its directory entry
        // isn't fsynced); unrehydrated, a reopened CREATE TABLE IF NOT EXISTS finds the table
        // "exists" and every INSERT silently no-ops.
        auto rehydrated = disk.rehydrate_missing_user_storages_sync();
        if (rehydrated.has_error()) {
            error(log_,
                  "spaces::open: the rehydrate walk did not run, so no catalog/storage divergence was "
                  "examined: {}",
                  rehydrated.error().what);
        } else if (rehydrated.value() > 0) {
            error(log_,
                  "spaces::open: {} alive catalog table(s) came up with no storage behind them",
                  rehydrated.value());
        }

        disk.set_manager_wal_sync(manager_wal_address);

        // System-table records mutate the catalog user-table restore depends on.
        if (!wal_records.empty()) {
            std::unordered_map<components::catalog::oid_t, std::vector<services::wal::record_t*>> system_by_oid;
            std::unordered_map<components::catalog::oid_t, std::vector<services::wal::record_t*>> user_by_oid;
            std::unordered_map<components::catalog::oid_t, components::catalog::oid_t> ns_cache;
            auto ns_for = [&](components::catalog::oid_t oid) {
                auto [it, inserted] = ns_cache.try_emplace(oid);
                if (inserted) {
                    it->second = oid < components::catalog::FIRST_USER_OID
                                     ? services::disk::manager_disk_t::system_dir_oid()
                                     : disk.relnamespace_for_oid_sync(oid);
                }
                return it->second;
            };
            // An unreadable checkpoint floor is not treated as 0, or records would re-apply checkpointed rows.
            std::unordered_map<components::catalog::oid_t, services::wal::id_t> cp_cache;
            std::unordered_set<components::catalog::oid_t> cp_unreadable;
            auto cp_for = [&](components::catalog::oid_t oid) -> services::wal::id_t {
                if (cp_unreadable.count(oid) != 0) {
                    return services::wal::id_t{0};
                }
                auto [it, inserted] = cp_cache.try_emplace(oid);
                if (inserted) {
                    // No namespace + no storage: table created since the last checkpoint, so 0 is correct.
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

            // Legal only pre-scheduler-start (a running engine would corrupt snapshots); synthesis
            // mutates manager_disk_t::storages_ with no lock, and a parallel variant TSan-confirmed raced on it.
            auto replay_one = [&disk, &log = log_](components::catalog::oid_t table_oid,
                                                   components::catalog::oid_t ns_oid,
                                                   std::vector<services::wal::record_t*>& records) {
                for (auto* r : records) {
                    switch (r->record_type) {
                        case services::wal::wal_record_type::PHYSICAL_INSERT:
                            if (!r->physical_data.empty()) {
                                if (!disk.has_storage(table_oid)) {
                                    // A failed load isn't an absent file: don't overwrite committed rows.
                                    if (auto load_err = disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
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
                                        auto otbx = disk.path_db() / std::to_string(static_cast<unsigned>(ns_oid)) /
                                                    std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
                                        std::filesystem::create_directories(otbx.parent_path());
                                        // An unreadable relkind is refused, never defaulted to 'r'.
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
                            // Applies before the dependent PHYSICAL_INSERT (higher wal_id, replays after).
                            if (!r->physical_data.empty()) {
                                if (!disk.has_storage(table_oid)) {
                                    if (auto load_err = disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
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
                                        auto otbx = disk.path_db() / std::to_string(static_cast<unsigned>(ns_oid)) /
                                                    std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
                                        std::filesystem::create_directories(otbx.parent_path());
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
                                        break;
                                    }
                                }
                                if (auto add_err = disk.direct_add_column_sync(table_oid, r->physical_data.front());
                                    add_err.contains_error()) {
                                    error(log, "spaces::replay: {}", add_err.what);
                                }
                            }
                            break;
                        case services::wal::wal_record_type::PHYSICAL_DELETE: {
                            // No row chunk to synthesise from; a still-missing storage must be logged.
                            if (!disk.has_storage(table_oid)) {
                                if (auto load_err = disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                    load_err.contains_error()) {
                                    error(log, "spaces::replay: {}", load_err.what);
                                }
                            }
                            if (auto del_err =
                                    disk.direct_delete_sync(table_oid, r->physical_row_ids, r->physical_row_count);
                                del_err.contains_error()) {
                                error(log, "spaces::replay: {}", del_err.what);
                            }
                            break;
                        }
                        case services::wal::wal_record_type::PHYSICAL_UPDATE:
                            if (!r->physical_data.empty()) {
                                if (!disk.has_storage(table_oid)) {
                                    if (auto load_err = disk.load_storage_for_wal_replay_sync(table_oid, ns_oid);
                                        load_err.contains_error()) {
                                        error(log, "spaces::replay: {}", load_err.what);
                                    }
                                }
                                // A torn record can name fewer ids than rows; truncate rather than misread.
                                std::size_t id_base = 0;
                                for (auto& chunk : r->physical_data) {
                                    const std::size_t n = chunk.size();
                                    const std::size_t have =
                                        r->physical_row_ids.size() > id_base ? r->physical_row_ids.size() - id_base : 0;
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
            };

            for (auto& [oid, records] : system_by_oid) {
                replay_one(oid, ns_for(oid), records);
            }

            // pg_class is final only now; earlier namespace answers were against a partial catalog.
            ns_cache.clear();

            // Dropped buckets: a surviving INSERT would resurrect a phantom storage at a recycled oid.
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

            // Sequential: a parallel variant TSan-confirmed raced on manager_disk_t::storages_.
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

        disk.load_user_table_storages_sync();

        // Re-derives a column drop a crash discarded; must run before bootstrap_indexes_sync opens
        // index stores against this schema.
        disk.rearm_dropped_column_blocks_sync();

        disk.restore_oid_generator_sync();

        // Both commit-clock halves are raised together, from one frontier, so they never disagree.
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

        // Recovers pg_class rows tombstoned by a pre-crash DROP TABLE that never removed the .otbx.
        auto dropped_oids = disk.scan_dropped_oids_sync();
        if (!dropped_oids.empty()) {
            const auto db_root = disk.path_db();
            for (const auto& row : dropped_oids) {
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
            // on_horizon_advanced can't be called inline (not yet running); arm flags for the first commit.
            manager_dispatcher_->set_disk_has_dropped_sync(true);
            manager_dispatcher_->set_index_has_dropped_sync(true);
            trace(log_, "spaces::PHASE 2c rebuilt {} dropped storage/index entries from pg_class", dropped_oids.size());
        }

        // Travels by value — legal only during this single-threaded bootstrap window.
        bootstrap_indexes_sync(committed_txn_ids);

        scheduler_dispatcher_->start();
        scheduler_->start();
        scheduler_disk_->start();

        trace(log_, "spaces::PHASE 3 complete");
        trace(log_, "spaces::spaces() final");
        // Construction succeeded: the destructor owns the registration from here on.
        path_guard.armed = false;
    }

    log_t& base_otterbrix_t::get_log() { return log_; }

    wrapper_dispatcher_t* base_otterbrix_t::dispatcher() { return wrapper_dispatcher_.get(); }

    base_otterbrix_t::~base_otterbrix_t() {
        trace(log_, "delete spaces");
        if (wrapper_dispatcher_) {
            try {
                auto session = components::session::session_id_t();
                auto checkpoint_node = components::logical_plan::make_node_checkpoint(&resource);
                // A destructor has no caller to answer a failed checkpoint, so log it instead.
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

    // The table pass must precede the pg_index pass: bootstrap_index_sync attaches to a table the
    // index manager already knows about.
    void base_otterbrix_t::bootstrap_indexes_sync(const std::set<std::uint64_t>& committed_txn_ids) {
        auto live_tables = manager_disk_->scan_live_table_oids_sync();
        for (auto oid : live_tables) {
            manager_index_->bootstrap_engine_sync(oid);
        }

        std::size_t indexes_wired = 0;
        std::size_t indexes_skipped_unfinished = 0;
        std::size_t indexes_skipped_unopenable = 0;
        std::size_t indexes_skipped_unrebuilt = 0;

        // Anything left in this marker names an index whose store still holds pre-compact row ids.
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
                // Wiring it would silently answer with whatever row slid into the stale id.
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
                error(log_,
                      "bootstrap_indexes_sync: pg_index row (indexrelid={}, indrelid={}) has an uncommitted "
                      "backfill (indisvalid=false) — the index is NOT wired, queries on the table fall back "
                      "to full scans; re-issue CREATE INDEX (or DROP INDEX the leftover)",
                      static_cast<unsigned>(row.oid),
                      static_cast<unsigned>(row.table_oid));
                ++indexes_skipped_unfinished;
                continue;
            }

            std::pmr::set<std::uint64_t> committed_for_agent(committed_txn_ids.begin(),
                                                             committed_txn_ids.end(),
                                                             &resource);

            auto wire_error = manager_index_->bootstrap_index_sync(row.table_oid,
                                                                   row.oid,
                                                                   row.type,
                                                                   std::move(row.keys),
                                                                   std::move(committed_for_agent));
            if (wire_error.contains_error()) {
                // Skipped rather than aborting: a full scan costs less than the engine failing to start.
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
            manager_index_->bootstrap_dropped_sync(row.oid, row.delete_id);
        }

        // No index is rebuilt here: repair needs a mailbox round trip the schedulers aren't running for yet.
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

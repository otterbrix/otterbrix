#include "engine.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/detail/memory.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/dispatcher/dispatcher.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <services/wal/wal_reader.hpp>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <set>
#include <sys/file.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace services::engine {

    namespace detail {

        struct engine_parts_t final {
            engine_parts_t(std::pmr::memory_resource* resource,
                           log_t& log,
                           const configuration::config& config,
                           directory_lock_t lock)
                : resource(resource)
                , log(log.clone())
                , config(config)
                , lock(std::move(lock))
                , dispatcher(nullptr, actor_zeta::pmr::deleter_t(resource))
                , disk(nullptr, actor_zeta::pmr::deleter_t(resource))
                , wal(nullptr, actor_zeta::pmr::deleter_t(resource))
                , index(nullptr, actor_zeta::pmr::deleter_t(resource)) {}

            // Every loop stops before any manager goes: a loop still running would resume a
            // coroutine whose reply was broken by a destroyed neighbour, or send into it.
            ~engine_parts_t() { stop_loops(); }

            void stop_loops() noexcept {
                if (dispatcher) {
                    dispatcher->stop_loop();
                }
                if (wal) {
                    wal->stop_loop();
                }
                if (disk) {
                    disk->stop_loop();
                }
                if (index) {
                    index->stop_loop();
                }
            }

            std::pmr::memory_resource* resource;
            log_t log;
            configuration::config config;
            // Declared before the managers: released only after every one of them is gone.
            directory_lock_t lock;
            std::vector<wal::record_t> wal_records;
            // Index txn-log frames are durable before the WAL commit marker, so uncommitted entries need this set.
            std::set<std::uint64_t> commit_ids;
            schedulers_t schedulers{nullptr, nullptr, nullptr};
            std::unique_ptr<dispatcher::manager_dispatcher_t, actor_zeta::pmr::deleter_t> dispatcher;
            std::unique_ptr<disk::manager_disk_t, actor_zeta::pmr::deleter_t> disk;
            std::unique_ptr<wal::manager_wal_replicate_t, actor_zeta::pmr::deleter_t> wal;
            std::unique_ptr<index::manager_index_t, actor_zeta::pmr::deleter_t> index;
        };

        void engine_parts_deleter_t::operator()(engine_parts_t* parts) const noexcept { delete parts; }

    } // namespace detail

    using detail::engine_parts_t;

    namespace {
        core::error_t startup_error(std::pmr::memory_resource* resource, core::error_code_t code, const std::string& what) {
            return core::error_t(code, std::pmr::string{what.data(), what.size(), resource});
        }

        template<typename T>
        T wait_ready(actor_zeta::unique_future<T>& future) {
            while (!future.is_ready()) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            return std::move(future).take_ready();
        }
    } // namespace

    directory_lock_t::directory_lock_t(int fd) noexcept
        : fd_(fd) {}

    directory_lock_t::directory_lock_t(directory_lock_t&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)) {}

    directory_lock_t& directory_lock_t::operator=(directory_lock_t&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                ::close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    directory_lock_t::~directory_lock_t() {
        if (fd_ >= 0) {
            ::close(fd_);
        }
    }

    core::result_wrapper_t<directory_lock_t> directory_lock_t::acquire(std::pmr::memory_resource* resource,
                                                                       const std::filesystem::path& directory) {
        if (directory.empty()) {
            return startup_error(resource,
                                 core::error_code_t::invalid_parameter,
                                 "engine startup REFUSED , config.main_path is empty: there is no directory to own");
        }
        std::error_code ec;
        std::filesystem::create_directories(directory, ec);
        if (ec) {
            return startup_error(resource,
                                 core::error_code_t::io_error,
                                 "engine startup REFUSED , the directory " + directory.string() +
                                     " could not be created: " + ec.message());
        }
        const auto lock_path = directory / ".lock";
        const int fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fd < 0) {
            return startup_error(resource,
                                 core::error_code_t::io_error,
                                 "engine startup REFUSED , the lock file " + lock_path.string() +
                                     " could not be opened: " + std::strerror(errno));
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            const int flock_errno = errno;
            ::close(fd);
            if (flock_errno == EWOULDBLOCK) {
                return startup_error(resource,
                                     core::error_code_t::already_exists,
                                     "engine startup REFUSED , another engine already owns " + directory.string() +
                                         ": otterbrix instance has to have unique directory");
            }
            return startup_error(resource,
                                 core::error_code_t::io_error,
                                 "engine startup REFUSED , the lock file " + lock_path.string() +
                                     " could not be locked: " + std::strerror(flock_errno));
        }
        return directory_lock_t{fd};
    }

    prepared_engine_t::prepared_engine_t(detail::engine_parts_ptr parts) noexcept
        : parts_(std::move(parts)) {}

    spawned_engine_t::spawned_engine_t(detail::engine_parts_ptr parts) noexcept
        : parts_(std::move(parts)) {}

    bootstrapped_engine_t::bootstrapped_engine_t(detail::engine_parts_ptr parts) noexcept
        : parts_(std::move(parts)) {}

    engine_t::engine_t(detail::engine_parts_ptr parts) noexcept
        : parts_(std::move(parts)) {}

    core::result_wrapper_t<prepared_engine_t>
    prepare_engine(std::pmr::memory_resource* resource, const configuration::config& config, log_t& log) {
        trace(log, "engine::prepare_engine");
        if (config.execution.executor_pool_size == 0) {
            return startup_error(resource,
                                 core::error_code_t::invalid_parameter,
                                 "engine startup REFUSED , config.execution.executor_pool_size is 0: the dispatcher "
                                 "would have no executor to route a statement to");
        }
        const auto& pump = config.execution.pump;
        if (pump.in_flight.count() <= 0 || pump.idle <= pump.in_flight) {
            return startup_error(resource,
                                 core::error_code_t::invalid_parameter,
                                 "engine startup REFUSED , config.execution.pump must satisfy 0 < in_flight < idle "
                                 "(in_flight " +
                                     std::to_string(pump.in_flight.count()) + " us, idle " +
                                     std::to_string(pump.idle.count()) +
                                     " us): in_flight is the per-hop latency while replies are awaited");
        }
        // An empty wal.path is not "no WAL", it is a WAL nobody can find: manager_wal_replicate_t
        // skips its recovery scan on it and total_wal_bytes() answers 0, so the auto-checkpoint
        // threshold never fires. Refused here rather than left as a second, quieter off switch.
        if (config.wal.path.empty()) {
            return startup_error(resource,
                                 core::error_code_t::invalid_parameter,
                                 "engine startup REFUSED , config.wal.path is empty: the WAL is the table's only redo "
                                 "record between checkpoints and has nowhere to write");
        }

        VALUE_OR_RETURN(auto lock, directory_lock_t::acquire(resource, config.main_path));

        if (!config.disk.path.empty()) {
            const auto legacy_catalog_otbx = config.disk.path / "catalog.otbx";
            std::error_code exists_ec;
            if (std::filesystem::exists(legacy_catalog_otbx, exists_ec)) {
                return startup_error(resource,
                                     core::error_code_t::invalid_parameter,
                                     "Legacy catalog format detected at " + legacy_catalog_otbx.string() +
                                         ". Remove the file and restart — pg_catalog is the source of truth.");
            }
        }

        detail::engine_parts_ptr parts{new engine_parts_t(resource, log, config, std::move(lock))};

        services::wal::id_t last_wal_id{0};
        services::wal::wal_reader_t wal_reader(resource, config.wal, parts->log);
        auto wal_records_result = wal_reader.read_committed_records(last_wal_id, &parts->commit_ids);
        // Refuses rather than allocating IDs below what's already on disk; writes/deletes nothing.
        if (wal_records_result.has_error()) {
            error(parts->log,
                  "engine startup REFUSED , the WAL could not be replayed in full: {}",
                  wal_records_result.error().what);
            return startup_error(resource,
                                 core::error_code_t::io_error,
                                 "WAL replay could not read a segment, refusing to start: " +
                                     std::string(wal_records_result.error().what.c_str()));
        }
        parts->wal_records = std::move(wal_records_result.value());
        trace(parts->log, "engine::prepare_engine - {} WAL records", parts->wal_records.size());
        return prepared_engine_t{std::move(parts)};
    }

    spawned_engine_t spawn_engine(prepared_engine_t prepared,
                                  std::pmr::memory_resource* resource,
                                  schedulers_t schedulers,
                                  const configuration::config& config,
                                  log_t& log,
                                  primitives_t primitives) {
        auto parts = std::move(prepared.parts_);
        assert(parts != nullptr && parts->resource == resource);
        parts->schedulers = schedulers;
        parts->log = log.clone();
        auto& own_log = parts->log;

        // The dispatcher's own address — born last — is wired back into each manager below, post-construction.
        trace(own_log, "engine::spawn manager_disk");
        parts->disk = actor_zeta::spawn<services::disk::manager_disk_t>(resource,
                                                                         schedulers.general,
                                                                         schedulers.disk,
                                                                         config.disk,
                                                                         own_log,
                                                                         config.execution.pump);
        trace(own_log, "engine::spawn manager_index");
        parts->index = actor_zeta::spawn<services::index::manager_index_t>(resource,
                                                                           schedulers.general,
                                                                           own_log,
                                                                           config.disk.path,
                                                                           config.disk.bitcask_flush_threshold,
                                                                           config.disk.bitcask_segment_record_limit,
                                                                           config.disk.btree_flush_threshold,
                                                                           config.execution.pump);
        trace(own_log, "engine::spawn manager_wal");
        parts->wal = actor_zeta::spawn<services::wal::manager_wal_replicate_t>(resource,
                                                                               schedulers.general,
                                                                               config.wal,
                                                                               own_log,
                                                                               parts->disk->address(),
                                                                               parts->index->address(),
                                                                               config.execution.pump);
        trace(own_log, "engine::spawn manager_dispatcher");
        parts->dispatcher =
            actor_zeta::spawn<services::dispatcher::manager_dispatcher_t>(resource,
                                                                          schedulers.exec,
                                                                          own_log,
                                                                          parts->wal->address(),
                                                                          parts->disk->address(),
                                                                          parts->index->address(),
                                                                          config.execution.dml_flush_row_threshold,
                                                                          primitives.optimizer_rules,
                                                                          primitives.name_resolution,
                                                                          config.execution.executor_pool_size,
                                                                          config.execution.pump);

        // Dispatcher address published into every manager: disk/index for the GC-ack path
        // (disk -> dispatcher -> wal truncate), wal for the auto-checkpoint watermark.
        parts->disk->set_manager_dispatcher_sync(parts->dispatcher->address());
        parts->index->set_manager_dispatcher_sync(parts->dispatcher->address());
        parts->wal->set_manager_dispatcher_sync(parts->dispatcher->address());
        return spawned_engine_t{std::move(parts)};
    }

    namespace {

    core::error_t bootstrap_indexes(engine_parts_t& parts) {
        auto& log = parts.log;
        auto live_tables = parts.disk->scan_live_table_oids_sync();
        for (auto oid : live_tables) {
            parts.index->bootstrap_engine_sync(oid);
        }

        std::size_t indexes_wired = 0;
        std::size_t indexes_skipped_unfinished = 0;
        std::size_t indexes_skipped_unopenable = 0;
        std::size_t indexes_skipped_unrebuilt = 0;

        // Anything left in this marker names an index whose store still holds pre-compact row ids.
        const auto pending_rebuilds = parts.index->pending_index_rebuilds_sync();
        const auto rebuild_is_owed = [&pending_rebuilds](components::catalog::oid_t table_oid,
                                                         components::catalog::oid_t index_oid) {
            for (const auto& entry : pending_rebuilds) {
                if (entry.table_oid == table_oid && entry.index_oid == index_oid) {
                    return true;
                }
            }
            return false;
        };

        VALUE_OR_RETURN(auto index_rows, parts.disk->scan_alive_pg_index_sync());
        for (auto& row : index_rows) {
            if (rebuild_is_owed(row.table_oid, row.oid)) {
                // Wiring it would silently answer with whatever row slid into the stale id.
                error(log,
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
                error(log,
                      "bootstrap_indexes_sync: pg_index row (indexrelid={}, indrelid={}) has an uncommitted "
                      "backfill (indisvalid=false) — the index is NOT wired, queries on the table fall back "
                      "to full scans; re-issue CREATE INDEX (or DROP INDEX the leftover)",
                      static_cast<unsigned>(row.oid),
                      static_cast<unsigned>(row.table_oid));
                ++indexes_skipped_unfinished;
                continue;
            }

            std::pmr::set<std::uint64_t> committed_for_agent(parts.commit_ids.begin(), parts.commit_ids.end(), parts.resource);

            auto wire_error = parts.index->bootstrap_index_sync(row.table_oid,
                                                                   row.oid,
                                                                   row.type,
                                                                   std::move(row.keys),
                                                                   std::move(committed_for_agent));
            if (wire_error.contains_error()) {
                // Skipped rather than aborting: a full scan costs less than the engine failing to start.
                error(log,
                      "bootstrap_indexes_sync: index_oid={} left unregistered: {}",
                      static_cast<unsigned>(row.oid),
                      wire_error.what);
                ++indexes_skipped_unopenable;
                continue;
            }
            ++indexes_wired;
        }

        auto dropped = parts.disk->scan_dropped_table_oids_sync();
        for (const auto& row : dropped) {
            parts.index->bootstrap_dropped_sync(row.oid, row.delete_id);
        }

        // No index is rebuilt here: repair needs a mailbox round trip the schedulers aren't running for yet.
        trace(log,
              "spaces::PHASE 4 bootstrap_indexes_sync: {} engines, {} indexes wired "
              "({} skipped: unfinished build; {} skipped: unopenable storage; {} skipped: rebuild owed after a "
              "compaction), {} dropped tombstones restored",
              live_tables.size(),
              indexes_wired,
              indexes_skipped_unfinished,
              indexes_skipped_unopenable,
              indexes_skipped_unrebuilt,
              dropped.size());
        return core::error_t::no_error();
    }

    core::error_t bootstrap_parts(engine_parts_t& parts) {
        auto& disk = *parts.disk;
        auto& log = parts.log;
        RETURN_IF_ERROR(disk.bootstrap_system_tables_sync());
        RETURN_IF_ERROR(disk.load_user_table_storages_sync());
        // A .otbx can be lost after crash even though its pg_class row survived (its directory entry
        // isn't fsynced); unrehydrated, a reopened CREATE TABLE IF NOT EXISTS finds the table
        // "exists" and every INSERT silently no-ops.
        auto rehydrated = disk.rehydrate_missing_user_storages_sync();
        if (rehydrated.has_error()) {
            error(log,
                  "spaces::open: the rehydrate walk did not run, so no catalog/storage divergence was "
                  "examined: {}",
                  rehydrated.error().what);
        } else if (rehydrated.value() > 0) {
            error(log,
                  "spaces::open: {} alive catalog table(s) came up with no storage behind them",
                  rehydrated.value());
        }

        disk.set_manager_wal_sync(parts.wal->address());

        // System-table records mutate the catalog user-table restore depends on.
        if (!parts.wal_records.empty()) {
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
                        error(log,
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
            for (auto& record : parts.wal_records) {
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
            auto replay_one = [&disk, &log](components::catalog::oid_t table_oid,
                                                   components::catalog::oid_t ns_oid,
                                                   std::vector<services::wal::record_t*>& records) {
                std::map<std::uint64_t, std::uint64_t> pending_delete_commits;
                auto commit_delete_group =
                    [&disk, &log](components::catalog::oid_t oid, std::uint64_t txn_id, std::uint64_t commit_id) {
                        if (auto err = disk.commit_all_deletes_sync(oid, txn_id, commit_id); err.contains_error()) {
                            error(log, "spaces::replay: {}", err.what);
                        }
                    };
                auto flush_delete_commit =
                    [&](components::catalog::oid_t oid, std::uint64_t txn_id, std::uint64_t commit_id) {
                        if (txn_id == 0) {
                            return;
                        }
                        auto it = pending_delete_commits.find(txn_id);
                        if (it != pending_delete_commits.end() && it->second != commit_id) {
                            commit_delete_group(oid, txn_id, it->second);
                            pending_delete_commits.erase(it);
                        }
                    };
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
                                        std::error_code dir_ec;
                                        std::filesystem::create_directories(otbx.parent_path(), dir_ec);
                                        if (dir_ec) {
                                            error(log,
                                                  "spaces::replay: table oid={} has no directory for its .otbx ({}) "
                                                  "— records for this table are NOT replayed",
                                                  static_cast<unsigned>(table_oid),
                                                  dir_ec.message());
                                            return;
                                        }
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
                                    const auto chunk_count = static_cast<uint64_t>(chunk.size());
                                    auto append_r =
                                        disk.append_sync(table_oid,
                                                         chunk,
                                                         components::table::transaction_data{r->transaction_id, 0});
                                    if (append_r.has_error()) {
                                        error(log,
                                              "spaces::replay: {} committed row(s) for table oid={} were not "
                                              "restored: {}",
                                              chunk.size(),
                                              static_cast<unsigned>(table_oid),
                                              append_r.error().what);
                                        continue;
                                    }
                                    if (r->transaction_id == 0) {
                                        continue;
                                    }
                                    if (auto committed = disk.commit_append_sync(table_oid,
                                                                                 r->commit_id,
                                                                                 static_cast<int64_t>(append_r.value()),
                                                                                 chunk_count);
                                        committed.contains_error()) {
                                        error(log,
                                              "spaces::replay: {} row(s) for table oid={} were restored but "
                                              "their commit stamp was not applied: {}",
                                              chunk_count,
                                              static_cast<unsigned>(table_oid),
                                              committed.what);
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
                                        std::error_code dir_ec;
                                        std::filesystem::create_directories(otbx.parent_path(), dir_ec);
                                        if (dir_ec) {
                                            error(log,
                                                  "spaces::replay: table oid={} has no directory for its .otbx ({}) "
                                                  "— records for this table are NOT replayed",
                                                  static_cast<unsigned>(table_oid),
                                                  dir_ec.message());
                                            return;
                                        }
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
                            flush_delete_commit(table_oid, r->transaction_id, r->commit_id);
                            if (auto del_err =
                                    disk.delete_sync(table_oid,
                                                     r->physical_row_ids,
                                                     r->physical_row_count,
                                                     components::table::transaction_data{r->transaction_id, 0});
                                del_err.contains_error()) {
                                error(log, "spaces::replay: {}", del_err.what);
                                break;
                            }
                            if (r->transaction_id != 0) {
                                pending_delete_commits[r->transaction_id] = r->commit_id;
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
                                    flush_delete_commit(table_oid, r->transaction_id, r->commit_id);
                                    auto upd_r =
                                        disk.update_sync(table_oid,
                                                         ids,
                                                         chunk,
                                                         components::table::transaction_data{r->transaction_id, 0});
                                    if (upd_r.has_error()) {
                                        error(log, "spaces::replay: {}", upd_r.error().what);
                                        continue;
                                    }
                                    if (r->transaction_id == 0) {
                                        continue;
                                    }
                                    pending_delete_commits[r->transaction_id] = r->commit_id;
                                    const auto upd = upd_r.value();
                                    if (auto committed =
                                            disk.commit_append_sync(table_oid, r->commit_id, upd.start_row, upd.count);
                                        committed.contains_error()) {
                                        error(log, "spaces::replay: {}", committed.what);
                                    }
                                }
                            }
                            break;
                        default:
                            break;
                    }
                }
                for (const auto& [txn_id, commit_id] : pending_delete_commits) {
                    commit_delete_group(table_oid, txn_id, commit_id);
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
                    trace(log,
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
                trace(log,
                      "spaces::replayed {} physical WAL records across {} tables",
                      physical_count,
                      system_by_oid.size() + user_by_oid.size());
            }
        }

        RETURN_IF_ERROR(disk.load_user_table_storages_sync());

        // Re-derives a column drop a crash discarded; must run before bootstrap_indexes_sync opens
        // index stores against this schema.
        disk.reconcile_storage_with_catalog_sync();

        disk.restore_oid_generator_sync();

        parts.dispatcher->cache_settings_sync(disk.stored_settings_sync());

        // Both commit-clock halves are raised together, from one frontier, so they never disagree.
        uint64_t reopen_frontier = disk.max_persisted_commit_id_sync();
        uint64_t txn_high_water = 0;
        for (const auto& r : parts.wal_records) {
            if (r.is_commit_marker() && r.commit_id > reopen_frontier) {
                reopen_frontier = r.commit_id;
            }
            if (r.transaction_id > txn_high_water) {
                txn_high_water = r.transaction_id;
            }
        }
        parts.dispatcher->seed_clocks_sync(reopen_frontier, txn_high_water);

        // Recovers pg_class rows tombstoned by a pre-crash DROP TABLE that never removed the .otbx.
        auto dropped_oids = disk.scan_dropped_oids_sync();
        if (!dropped_oids.empty()) {
            const auto db_root = disk.path_db();
            for (const auto& row : dropped_oids) {
                auto base = db_root / std::to_string(static_cast<unsigned>(row.namespace_oid)) /
                            std::to_string(static_cast<unsigned>(row.oid));
                auto otbx = base / "table.otbx";
                std::pmr::vector<std::filesystem::path> sidecars{parts.resource};
                {
                    auto wal_id_sidecar = otbx;
                    wal_id_sidecar += ".wal_id";
                    sidecars.push_back(std::move(wal_id_sidecar));
                }
                disk.register_dropped_storage_sync(row.oid, row.delete_id, std::move(otbx), std::move(sidecars));
                parts.index->mark_table_dropped_sync(row.oid, row.delete_id);
            }
            // on_horizon_advanced can't be called inline (not yet running); arm flags for the first commit.
            parts.dispatcher->set_disk_has_dropped_sync(true);
            parts.dispatcher->set_index_has_dropped_sync(true);
            trace(log, "spaces::PHASE 2c rebuilt {} dropped storage/index entries from pg_class", dropped_oids.size());
        }

        // Travels by value — legal only during this single-threaded bootstrap window.
        return bootstrap_indexes(parts);
    }

    } // namespace

    core::result_wrapper_t<bootstrapped_engine_t> bootstrap(spawned_engine_t spawned) {
        auto parts = std::move(spawned.parts_);
        assert(parts != nullptr);
        if (auto err = bootstrap_parts(*parts); err.contains_error()) {
            error(parts->log, "engine::bootstrap REFUSED: {}", err.what);
            return err;
        }
        // Only replay needs them; the running engine never reads the journal back.
        parts->wal_records.clear();
        parts->commit_ids.clear();
        return bootstrapped_engine_t{std::move(parts)};
    }

    engine_t start(bootstrapped_engine_t bootstrapped) {
        auto parts = std::move(bootstrapped.parts_);
        assert(parts != nullptr);
        parts->schedulers.exec->start();
        parts->schedulers.general->start();
        parts->schedulers.disk->start();
        trace(parts->log, "engine::start complete");
        return engine_t{std::move(parts)};
    }

    engine_t::~engine_t() { shutdown(); }

    actor_zeta::actor::address_t engine_t::dispatcher_address() const noexcept { return parts_->dispatcher->address(); }
    actor_zeta::actor::address_t engine_t::disk_address() const noexcept { return parts_->disk->address(); }
    actor_zeta::actor::address_t engine_t::index_address() const noexcept { return parts_->index->address(); }
    actor_zeta::actor::address_t engine_t::wal_address() const noexcept { return parts_->wal->address(); }

    void engine_t::shutdown() noexcept {
        if (parts_ == nullptr) {
            return;
        }
        auto& log = parts_->log;
        trace(log, "engine::shutdown");
        auto* resource = parts_->resource;
        const components::session::session_id_t session;

        auto [_close, closed] =
            actor_zeta::otterbrix::send(parts_->dispatcher->address(),
                                        &services::dispatcher::manager_dispatcher_t::begin_shutdown);
        wait_ready(closed);

        auto [_quiet, quiet] = actor_zeta::otterbrix::send(parts_->wal->address(),
                                                           &services::wal::manager_wal_replicate_t::stop_auto_checkpoint,
                                                           session);
        wait_ready(quiet);

        auto checkpoint_node = components::logical_plan::make_node_checkpoint(resource);
        auto [_cp, checkpointed] = actor_zeta::otterbrix::send(
            parts_->dispatcher->address(),
            &services::dispatcher::manager_dispatcher_t::execute_plan,
            session,
            components::logical_plan::execution_plan_t{resource,
                                                       checkpoint_node,
                                                       components::logical_plan::make_parameter_node(resource)});
        // No caller can be answered from here, so a failed checkpoint is logged.
        auto cursor = wait_ready(checkpointed);
        if (!cursor) {
            error(log,
                  "engine::shutdown , the shutdown checkpoint answered NO cursor , whether the journal was folded "
                  "into storage is unknown");
        } else if (cursor->is_error()) {
            error(log,
                  "engine::shutdown , the shutdown checkpoint FAILED , the journal is NOT folded into storage and "
                  "the next start replays it: {}",
                  cursor->get_error().what);
        } else {
            trace(log, "engine::shutdown: checkpoint complete");
        }

        parts_->stop_loops();
        parts_->schedulers.general->stop();
        parts_->schedulers.exec->stop();
        parts_->schedulers.disk->stop();
        parts_.reset();
    }

} // namespace services::engine

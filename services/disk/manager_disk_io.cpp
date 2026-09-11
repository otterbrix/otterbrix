#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    void manager_disk_t::set_manager_wal_sync(actor_zeta::address_t address) {
        // Fanned into every agent, bootstrap-only; the manager itself keeps no copy.
        for (auto& agent : agents_) {
            if (agent != nullptr) {
                agent->set_manager_wal_sync(address);
            }
        }
    }

    void manager_disk_t::create_agent(int count_agents) {
        // Roles align with pool_idx_for_oid: slot 0 = CATALOG, slots 1..N-1 = USER_POOL.
        for (int i = 0; i < count_agents; i++) {
            const std::size_t slot = agents_.size();
            auto name_agent = "agent_disk_" + std::to_string(slot + 1);
            trace(log_, "manager_disk create_agent : {}", name_agent);
            const agent_role_t role = (slot == 0) ? agent_role_t::CATALOG : agent_role_t::USER_POOL;
            auto agent = actor_zeta::spawn<agent_disk_t>(resource(), config_.path, log_, role, slot);
            agents_.emplace_back(std::move(agent));
        }
    }

    manager_disk_t::unique_future<wal::id_t>
    manager_disk_t::checkpoint_all(session_id_t session, wal::id_t current_wal_id, uint64_t compact_watermark) {
        trace(log_,
              "manager_disk_t::checkpoint_all , session : {} , wal_id : {} , compact_watermark : {}",
              session.data(),
              current_wal_id,
              compact_watermark);

        // Each agent returns min(prev_checkpoint_wal_id_) over its entries (max() sentinel if it owns none).
        std::pmr::vector<unique_future<checkpoint_result_t>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::checkpoint_inner,
                                                                  session,
                                                                  current_wal_id,
                                                                  uint64_t{compact_watermark});
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }

        wal::id_t min_prev_id = std::numeric_limits<wal::id_t>::max();
        uint64_t deferred = 0;
        uint64_t rewritten = 0;
        uint64_t advanced = 0;
        for (auto& f : agent_futures) {
            auto agent_result = co_await std::move(f);
            min_prev_id = std::min(min_prev_id, agent_result.min_prev_checkpoint_wal_id);
            deferred += agent_result.deferred;
            rewritten += agent_result.rewritten;
            advanced += agent_result.advanced;
        }

        if (deferred > 0) {
            // Observed as boundaries 31/55/55/135 with one truncation deleting nothing: a deferred entry's prev doesn't
            // move, pinning the WAL floor -- structurally safe, but must not be silent or the WAL grows every round
            // while every health line reports success. Two log levels since a round that rewrote nothing means the WAL
            // only grows if it repeats, while one that rewrote something with an entry waiting is the ordinary steady
            // state of a busy database.
            const auto floor_reported = static_cast<std::uint64_t>(min_prev_id);
            if (rewritten == 0) {
                warn(log_,
                     "manager_disk_t::checkpoint_all , session : {} , the round rewrote NOTHING and deferred {} "
                     "entr{} ({} unchanged advanced) — the WAL floor stays at {} and cannot move past the "
                     "deferred tables this round",
                     session.data(),
                     deferred,
                     deferred == 1 ? "y" : "ies",
                     advanced,
                     floor_reported);
            } else {
                info(log_,
                     "manager_disk_t::checkpoint_all , session : {} , the round deferred {} entr{} and rewrote {} "
                     "({} unchanged advanced) — the WAL floor is held at {} by the deferred tables",
                     session.data(),
                     deferred,
                     deferred == 1 ? "y" : "ies",
                     rewritten,
                     advanced,
                     floor_reported);
            }
        }

        if (!agents_.empty()) {
            // The sentinel means "no entry reported a floor", not "no entry was checkpointed"; it survives as max()
            // only when the agents own nothing checkpointable, so report 0 instead of handing truncate_before max().
            const bool wal_floor_reported = (min_prev_id != std::numeric_limits<wal::id_t>::max());

            trace(log_,
                  "manager_disk_t::checkpoint_all complete , rewritten : {} , advanced : {} , deferred : {}",
                  rewritten,
                  advanced,
                  deferred);
            if (!wal_floor_reported) {
                co_return wal::id_t{0};
            }
            co_return min_prev_id;
        }

        trace(log_, "manager_disk_t::checkpoint_all complete (no agents)");
        co_return wal::id_t{0};
    }

    manager_disk_t::unique_future<void> manager_disk_t::vacuum_all(session_id_t session,
                                                                   uint64_t lowest_active_start_time) {
        trace(log_, "manager_disk_t::vacuum_all , session : {}", session.data());

        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(agents_.size());
        for (auto& agent_ptr : agents_) {
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent_ptr->address(),
                                                                  &agent_disk_t::vacuum_inner,
                                                                  session,
                                                                  lowest_active_start_time);
            if (needs_sched) {
                scheduler_disk_->enqueue(agent_ptr.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }

        for (auto& f : agent_futures) {
            co_await std::move(f);
        }

        trace(log_, "manager_disk_t::vacuum_all complete");
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::maybe_cleanup_many(execution_context_t /*ctx*/,
                                       std::pmr::vector<components::catalog::oid_t> table_oids,
                                       uint64_t compact_watermark) {
        // Each table_oid routes to its owning agent so the threshold check + compact stays mailbox-serialized per oid.
        std::pmr::vector<unique_future<void>> agent_futures{resource()};
        agent_futures.reserve(table_oids.size());
        for (const auto table_oid : table_oids) {
            if (table_oid == components::catalog::INVALID_OID) {
                continue;
            }
            if (agents_.empty()) {
                break;
            }
            const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
            auto& agent = agents_[pool_idx];
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::maybe_cleanup_inner,
                                                                  table_oid,
                                                                  uint64_t{compact_watermark});
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            agent_futures.emplace_back(std::move(fut));
        }
        for (auto& f : agent_futures) {
            co_await std::move(f);
        }

        co_return;
    }

    core::error_t manager_disk_t::create_storage_disk_sync(components::catalog::oid_t table_oid,
                                                           components::catalog::oid_t /*database_oid*/,
                                                           std::vector<components::table::column_definition_t> columns,
                                                           const std::filesystem::path& otbx_path,
                                                           bool is_computed) {
        trace(log_,
              "manager_disk_t::create_storage_disk_sync , oid : {} , path : {}",
              static_cast<unsigned>(table_oid),
              otbx_path.string());
        // The manager never opens .otbx itself (would race the WRITE_LOCK); construction happens on the agent thread.
        if (agents_.empty()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"create_storage_disk_sync: no disk agents to own oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)),
                                                  resource()});
        }

        // WAL-replay synthesis leg of "every column carries its attoid": replayed columns are name-only, so this binds
        // by name (safe once the catalog is final).
        {
            bool needs_identity = false;
            for (const auto& col : columns) {
                if (col.attoid() == 0) {
                    needs_identity = true;
                    break;
                }
            }
            if (needs_identity) {
                std::unordered_set<components::catalog::oid_t> wanted;
                wanted.insert(table_oid);
                auto cols_by_relid = collect_catalog_columns_sync(wanted);
                auto found = cols_by_relid.find(table_oid);
                if (found != cols_by_relid.end()) {
                    for (auto& col : columns) {
                        if (col.attoid() != 0) {
                            continue;
                        }
                        for (const auto& def : found->second) {
                            if (def.name() == col.name() && def.attoid() != 0) {
                                col.set_attoid(def.attoid());
                                break;
                            }
                        }
                    }
                }
            }
        }
        const std::size_t pool_idx_c = pool_idx_for_oid(table_oid, agents_.size());
        trace(log_,
              "manager_disk_t::create_storage_disk_sync: create oid={} pool_idx={} path={}",
              static_cast<unsigned>(table_oid),
              pool_idx_c,
              otbx_path.string());
        // Decided before the call can change it, separating a stump this create just made from a file already on disk.
        std::error_code pre_ec;
        const bool existed_before = std::filesystem::exists(otbx_path, pre_ec) && !pre_ec;

        auto& agent = agents_[pool_idx_c];
        const bool ok = agent->bootstrap_create_disk_inner_sync(table_oid, std::move(columns), otbx_path, is_computed);
        if (ok) {
            return core::error_t::no_error();
        }
        // One `false`, two unrelated causes; the real question is whether the agent holds this oid now.
        if (agent->has_storage_sync(table_oid)) {
            trace(log_,
                  "manager_disk_t::create_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                  pool_idx_c,
                  static_cast<unsigned>(table_oid),
                  otbx_path.string());
            return core::error_t::no_error();
        }
        // A refusal must not leave the zero-byte stump FILE_CREATE_NEW makes on a failed write.
        if (!existed_before) {
            std::error_code stump_ec;
            if (std::filesystem::exists(otbx_path, stump_ec) && !stump_ec &&
                std::filesystem::file_size(otbx_path, stump_ec) == 0 && !stump_ec) {
                std::filesystem::remove(otbx_path, stump_ec);
            }
        }
        error(log_,
              "manager_disk_t::create_storage_disk_sync: agent[{}] could not create oid {} at {} — no storage "
              "came up for it",
              pool_idx_c,
              static_cast<unsigned>(table_oid),
              otbx_path.string());
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{"create_storage_disk_sync: could not create the .otbx for oid " +
                                                  std::to_string(static_cast<unsigned>(table_oid)) + " at " +
                                                  otbx_path.string(),
                                              resource()});
    }

    core::error_t
    manager_disk_t::load_storage_disk_sync(components::catalog::oid_t table_oid,
                                           components::catalog::oid_t /*database_oid*/,
                                           const std::filesystem::path& otbx_path,
                                           std::vector<components::table::column_definition_t> catalog_columns) {
        trace(log_,
              "manager_disk_t::load_storage_disk_sync , oid : {} , path : {}",
              static_cast<unsigned>(table_oid),
              otbx_path.string());

        // The SFBM holds an exclusive posix WRITE_LOCK, so only the agent thread opens it.
        const std::size_t pool_idx = agents_.empty() ? 0 : pool_idx_for_oid(table_oid, agents_.size());
        trace(log_,
              "manager_disk_t::load_storage_disk_sync: load oid={} pool_idx={} path={}",
              static_cast<unsigned>(table_oid),
              pool_idx,
              otbx_path.string());

        // Pre-read before the SFBM exists; absent and unreadable must answer differently.
        auto read_sidecar_wal_id = [&](const std::filesystem::path& base) -> core::result_wrapper_t<wal::id_t> {
            auto sidecar = base;
            sidecar += ".wal_id";
            std::error_code exists_ec;
            const bool present = std::filesystem::exists(sidecar, exists_ec);
            if (exists_ec) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"load_storage_disk_sync: cannot stat the checkpoint sidecar " +
                                                          sidecar.string() + ": " + exists_ec.message(),
                                                      resource()});
            }
            if (!present) {
                return wal::id_t{0};
            }
            std::ifstream f(sidecar, std::ios::binary);
            uint64_t v = 0;
            if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
                f.gcount() == static_cast<std::streamsize>(sizeof(v))) {
                return wal::id_t{v};
            }
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"load_storage_disk_sync: the checkpoint sidecar " + sidecar.string() +
                                     " exists but does not hold a wal id, so this table's checkpoint floor is "
                                     "unknown — it is NOT never-checkpointed. The table opens with the floor marked "
                                     "unreadable and its WAL records are not replayed; both files are left "
                                     "byte-identical, and the next committed checkpoint rewrites the sidecar.",
                                 resource()});
        };

        // Resolved before any open: system tables pass the builtin schema; user tables' columns come from pg_attribute.
        bool is_computed = false;
        if (catalog_columns.empty() && table_oid >= components::catalog::FIRST_USER_OID) {
            std::unordered_set<components::catalog::oid_t> wanted{table_oid};
            auto resolved = collect_catalog_columns_sync(wanted);
            if (auto it = resolved.find(table_oid); it != resolved.end()) {
                catalog_columns = std::move(it->second);
            }
            // Computed (relkind='g') tables have a legitimately empty schema; resolving relkind opens a young one
            // schema-less.
            if (catalog_columns.empty()) {
                // Assuming "regular" on a failed relkind read would silently drop a checkpointed table's dynamic-schema
                // semantics.
                auto relkind_r = relkind_for_oid_sync(table_oid);
                if (relkind_r.has_error()) {
                    error(log_,
                          "manager_disk_t::load_storage_disk_sync: could not read the relkind of oid {}: {}",
                          static_cast<unsigned>(table_oid),
                          relkind_r.error().what.c_str());
                    return core::error_t(
                        core::error_code_t::data_corruption,
                        std::pmr::string{"load_storage_disk_sync: the relkind of oid " +
                                             std::to_string(static_cast<unsigned>(table_oid)) +
                                             " could not be read, so " + otbx_path.string() +
                                             " cannot be opened without guessing whether it is a document table; "
                                             "refusing. Nothing was modified: " +
                                             relkind_r.error().what.c_str(),
                                         resource()});
                }
                is_computed = relkind_r.value() == components::catalog::relkind::computed;
            }
        }

        // The agent seeds its checkpoint floor from this; an unreadable one only flags the entry, not the open.
        auto sidecar_r = read_sidecar_wal_id(otbx_path);
        const bool sidecar_readable = !sidecar_r.has_error();
        if (!sidecar_readable) {
            error(log_,
                  "manager_disk_t::load_storage_disk_sync: oid {} comes up with an UNKNOWN checkpoint floor: {}",
                  static_cast<unsigned>(table_oid),
                  sidecar_r.error().what.c_str());
        }
        const auto sidecar_id = sidecar_readable ? sidecar_r.value() : wal::id_t{0};

        auto transfer_to_agent = [&](const std::filesystem::path& path) -> core::error_t {
            if (agents_.empty()) {
                return core::error_t(core::error_code_t::io_error,
                                     std::pmr::string{"load_storage_disk_sync: no disk agents to own oid " +
                                                          std::to_string(static_cast<unsigned>(table_oid)),
                                                      resource()});
            }
            auto& agent = agents_[pool_idx];
            if (agent->bootstrap_disk_inner_sync(table_oid,
                                                 path,
                                                 sidecar_id,
                                                 sidecar_readable,
                                                 catalog_columns,
                                                 is_computed)) {
                return core::error_t::no_error();
            }
            if (agent->has_storage_sync(table_oid)) {
                // The pre-construction probe drops the incoming SFBM on a duplicate key, so no WRITE_LOCK race.
                trace(log_,
                      "manager_disk_t::load_storage_disk_sync: agent[{}] already owns oid {} (path={})",
                      pool_idx,
                      static_cast<unsigned>(table_oid),
                      path.string());
                return core::error_t::no_error();
            }
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"load_storage_disk_sync: agent could not take ownership of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) + " from " +
                                                      path.string(),
                                                  resource()});
        };

        // Crash recovery is the two-slot shadow-paged root inside the .otbx; every refusal leaves it byte-identical.
        if (!std::filesystem::exists(otbx_path)) {
            // Callers guard existence, so arriving here means the file vanished between their check and this load.
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() +
                                                      " does not exist (a DISK table's file was expected here; "
                                                      "refusing to create an empty one)",
                                                  resource()});
        }

        // A stray sidecar in the engine-owned namespace makes the on-disk state ambiguous, so it's refused.
        if (auto sidecar_err = verify_otbx_sidecars(otbx_path, resource()); sidecar_err.contains_error()) {
            warn(log_, "load_storage_disk_sync: {}", sidecar_err.what.c_str());
            return sidecar_err;
        }

        // A file of exactly BLOCK_START bytes is the never-checkpointed signature; a `.wal_id` sidecar recording a real
        // checkpoint would contradict that, so refuse rather than discard it. A young file with no resolved schema just
        // means this walk ran before the catalog knew the table; defer, since the post-replay walk revisits it.
        {
            std::error_code size_ec;
            const auto file_bytes = std::filesystem::file_size(otbx_path, size_ec);
            if (!size_ec && file_bytes == components::table::storage::BLOCK_START) {
                if (sidecar_id > wal::id_t{0}) {
                    return core::error_t(
                        core::error_code_t::data_corruption,
                        std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() +
                                             " carries no checkpointed content (never-checkpointed signature, " +
                                             std::to_string(file_bytes) +
                                             " bytes), but its .wal_id sidecar records a committed checkpoint at "
                                             "wal id " +
                                             std::to_string(static_cast<uint64_t>(sidecar_id)) +
                                             ". The two cannot both be true; refusing to open the table as empty. "
                                             "Both files are left byte-identical.",
                                         resource()});
                }
                if (catalog_columns.empty() && !is_computed) {
                    trace(log_,
                          "manager_disk_t::load_storage_disk_sync: {} is never-checkpointed (size == BLOCK_START) "
                          "and the catalog does not know oid {} yet — deferring the load until after WAL replay",
                          otbx_path.string(),
                          static_cast<unsigned>(table_oid));
                    return core::error_t::no_error();
                }
            }
        }

        // Probed on the manager thread, then destroyed to free the WRITE_LOCK before the agent reopens.
        bool probe_failed = false;
        std::string probe_error;
        {
            auto probe =
                std::make_unique<collection_storage_entry_t>(resource(), otbx_path, catalog_columns, is_computed);
            if (probe->table_storage.construction_failed()) {
                probe_failed = true;
                probe_error = probe->table_storage.construction_error().what.c_str();
            }
            probe.reset(); // release WRITE_LOCK before agent reopens on agent thread
        }
        if (probe_failed) {
            warn(log_, "load_storage_disk_sync: failed to load {} : {}", otbx_path.string(), probe_error);
            return core::error_t(
                core::error_code_t::data_corruption,
                std::pmr::string{"load_storage_disk_sync: " + otbx_path.string() + " : " + probe_error, resource()});
        }
        return transfer_to_agent(otbx_path);
    }

    core::error_t verify_otbx_sidecars(const std::filesystem::path& otbx_path, std::pmr::memory_resource* resource) {
        // The engine owns every name extending the table file's own; this build writes only `.wal_id`/`.tmp`.
        const auto dir = otbx_path.parent_path();
        const auto base = otbx_path.filename().string();
        const std::string wal_id_name = base + ".wal_id";
        const std::string wal_id_tmp_name = wal_id_name + ".tmp";
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            const auto name = entry.path().filename().string();
            if (name == base || name == wal_id_name || name == wal_id_tmp_name) {
                continue;
            }
            if (name.rfind(base + ".", 0) == 0) {
                return core::error_t(
                    core::error_code_t::data_corruption,
                    std::pmr::string{"refusing to open " + otbx_path.string() + ": unexpected sidecar '" + name +
                                         "' sits next to the table file. This build (A7.5) recovers solely from "
                                         "the two-slot root inside the .otbx and writes only the .wal_id sidecar; "
                                         "a leftover backup or quarantine file from an earlier build makes the "
                                         "on-disk state ambiguous. Nothing was modified — remove or archive '" +
                                         entry.path().string() + "' and reopen.",
                                     resource});
            }
        }
        if (ec) {
            return core::error_t(
                core::error_code_t::io_error,
                std::pmr::string{"verify_otbx_sidecars: cannot list " + dir.string() + ": " + ec.message(), resource});
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<wal::id_t>
    manager_disk_t::peek_checkpoint_wal_id_from_disk(components::catalog::oid_t table_oid,
                                                     components::catalog::oid_t database_oid) const {
        // A loaded entry carries the floor its own load read; an unreadable sidecar means an unknown floor, not 0.
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (idx < agents_.size() && agents_[idx] != nullptr) {
                if (const auto* entry = agents_[idx]->storage_entry_sync(table_oid); entry != nullptr) {
                    if (!entry->table_storage.checkpoint_wal_id_known()) {
                        return core::error_t(
                            core::error_code_t::data_corruption,
                            std::pmr::string{"peek_checkpoint_wal_id_from_disk: oid " +
                                                 std::to_string(static_cast<unsigned>(table_oid)) +
                                                 " was loaded over a checkpoint sidecar that holds no wal id; its "
                                                 "floor is unknown and replaying this table would re-apply records "
                                                 "the checkpointed file already holds",
                                             resource()});
                    }
                    return entry->table_storage.checkpoint_wal_id();
                }
            }
        }
        // Not a fallback: answering 0 on every read failure would tell the replay filter "never checkpointed" wrongly.
        if (table_oid == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: asked for the checkpoint floor "
                                                  "of INVALID_OID; there is no table to answer about",
                                                  resource()});
        }
        if (config_.path.empty()) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: config_disk::path is empty, so "
                                                  "there is no directory to look for the sidecar of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) + " in",
                                                  resource()});
        }
        if (database_oid == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: the catalog does not name the "
                                                  "namespace of oid " +
                                                      std::to_string(static_cast<unsigned>(table_oid)) +
                                                      ", so its sidecar cannot be located — which is not the same as "
                                                      "no sidecar existing",
                                                  resource()});
        }
        auto sidecar = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                       std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx.wal_id";
        std::error_code exists_ec;
        const bool present = std::filesystem::exists(sidecar, exists_ec);
        if (exists_ec) {
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string{"peek_checkpoint_wal_id_from_disk: cannot stat " + sidecar.string() +
                                                      ": " + exists_ec.message(),
                                                  resource()});
        }
        if (!present) {
            return wal::id_t{0};
        }
        std::ifstream f(sidecar, std::ios::binary);
        uint64_t v = 0;
        if (f && f.read(reinterpret_cast<char*>(&v), sizeof(v)) &&
            static_cast<std::streamsize>(sizeof(v)) == f.gcount()) {
            return wal::id_t{v};
        }
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string{"peek_checkpoint_wal_id_from_disk: the checkpoint sidecar " +
                                                  sidecar.string() +
                                                  " exists but does not hold a wal id; its checkpoint floor is "
                                                  "unknown and replaying this table would re-apply records the "
                                                  "checkpointed file already holds",
                                              resource()});
    }

    core::error_t manager_disk_t::load_storage_for_wal_replay_sync(components::catalog::oid_t table_oid,
                                                                   components::catalog::oid_t database_oid) {
        if (has_storage(table_oid) || config_.path.empty() || table_oid == components::catalog::INVALID_OID ||
            database_oid == components::catalog::INVALID_OID) {
            return core::error_t::no_error();
        }
        auto otbx_path = config_.path / std::to_string(static_cast<unsigned>(database_oid)) /
                         std::to_string(static_cast<unsigned>(table_oid)) / "table.otbx";
        if (!std::filesystem::exists(otbx_path)) {
            // Legal here: replay can run ahead of a checkpoint, synthesising the storage from the chunk instead.
            return core::error_t::no_error();
        }
        // Must not be swallowed into a warn, or replay would synthesise over unreachable committed rows.
        if (auto err = load_storage_disk_sync(table_oid, database_oid, otbx_path, {}); err.contains_error()) {
            error(log_,
                  "load_storage_for_wal_replay_sync: failed to load {}: {}",
                  otbx_path.string(),
                  err.what.c_str());
            return err;
        }
        return core::error_t::no_error();
    }

} // namespace services::disk

#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Crash-safe pg_catalog row append: WAL is written first so a crash before the
    // storage update can be replayed on restart, then storage is updated.
    manager_disk_t::unique_future<components::pg_catalog_append_range_t>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                          components::catalog::oid_t table_oid,
                                          components::vector::data_chunk_t row) {
        record_session(ctx.session);
        const bool wal_available = (manager_wal_ != actor_zeta::address_t::empty_address());
        if (wal_available) {
            auto wal_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), row.types(), row.size());
            wal_chunk->set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk->data[col].set_value(r, row.data[col].value(r));
                }
            }
            // pg_catalog operations route to main_database (ctx.database_oid
            // is currently always INVALID_OID for catalog writes).
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                             &wal::manager_wal_replicate_t::write_physical_insert,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_chunk),
                                             std::uint64_t{0},
                                             static_cast<std::uint64_t>(row.size()),
                                             ctx.txn.transaction_id,
                                             db_oid);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "pg_catalog insert WAL write returned zero id for oid={}",
                      static_cast<unsigned>(table_oid));
            }
        }
        const auto count = static_cast<std::uint64_t>(row.size());
        const auto start_row = direct_append_sync(table_oid, row, ctx.session_tz, ctx.txn);
        if (ctx.txn.transaction_id == 0 || count == 0) {
            co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), 0};
        }
        co_return components::pg_catalog_append_range_t{table_oid, static_cast<int64_t>(start_row), count};
    }

    manager_disk_t::unique_future<void> manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                                                               components::catalog::oid_t table_oid,
                                                                               std::int64_t oid_col_idx,
                                                                               components::catalog::oid_t target_oid) {
        record_session(ctx.session);
        // Catalog OIDs only. Every caller passes a pg_catalog table_oid
        // (pg_class, pg_attribute, pg_depend, pg_type, pg_namespace,
        // pg_index) — all catalog OIDs route to agents_[0] via
        // pool_idx_for_oid. Null storage_entry_sync is a terminal "no entry".
        // Mutation half (direct_delete_sync) routes to the agent.
        const collection_storage_entry_t* entry = nullptr;
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
            if (agents_[idx] != nullptr) {
                entry = agents_[idx]->storage_entry_sync(table_oid);
            }
        }
        if (entry == nullptr) {
            co_return;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        inline_scan(entry->table_storage.table(),
                    {oid_col_idx},
                    &scan_resource,
                    [&, oid_col_idx](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(static_cast<uint64_t>(oid_col_idx), i);
                        if (v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return true;
                    });
        if (row_ids.empty()) {
            co_return;
        }
        if (manager_wal_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<std::int64_t> wal_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                             &wal::manager_wal_replicate_t::write_physical_delete,
                                             ctx.session,
                                             table_oid,
                                             std::move(wal_ids),
                                             static_cast<std::uint64_t>(row_ids.size()),
                                             ctx.txn.transaction_id,
                                             components::catalog::well_known_oid::main_database);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "pg_catalog delete WAL write returned zero id for oid={}",
                      static_cast<unsigned>(table_oid));
            }
        }
        direct_delete_sync(table_oid, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
        co_return;
    }

    // Locates the pg_attribute row by `attoid` (column index 0) and writes
    // `commit_id` into column index 10 (added_at_commit_id) or 11
    // (dropped_at_commit_id) per `kind`. Called from operator_commit_
    // transaction_t after tm->commit() returns the freshly-allocated
    // commit_id but BEFORE storage_publish_commits flips MVCC visibility on
    // the just-appended row — the row still carries insert_id == txn_id and
    // is invisible to every concurrent snapshot, so this metadata patch
    // races with no observer.
    //
    // Storage layer note: data_table_t::update() rewrites every column in
    // the target row (see components/table/data_table.cpp:401 — it builds
    // column_ids = [0..column_count) unconditionally). A naive "patch one
    // column" chunk would NULL out the other ten. We therefore read the
    // full row first, mutate the target field in the read-back chunk, and
    // write the whole chunk back — a metadata round-trip but correct.
    //
    // WAL pairing: emits a physical_update record so replay re-applies the
    // backfill after the matching physical_insert (txn-local ordering).
    manager_disk_t::unique_future<void> manager_disk_t::update_pg_attribute_commit_id_field(
        execution_context_t ctx,
        components::catalog::oid_t attoid,
        components::pg_attribute_commit_id_backfill_t::kind_t kind,
        std::uint64_t commit_id) {
        record_session(ctx.session);
        constexpr auto pg_attr_oid = components::catalog::well_known_oid::pg_attribute_table;
        // pg_attribute is a catalog OID — routes to agents_[0] (CATALOG
        // agent). Null storage_entry_sync is a terminal "no entry".
        // Mutation half (direct_update_sync) routes to the agent.
        const collection_storage_entry_t* entry = nullptr;
        if (!agents_.empty()) {
            const std::size_t idx = pool_idx_for_oid(pg_attr_oid, agents_.size());
            if (agents_[idx] != nullptr) {
                entry = agents_[idx]->storage_entry_sync(pg_attr_oid);
            }
        }
        if (entry == nullptr) {
            co_return;
        }

        // scan all columns for the row with attoid == target. Build
        // both the row_id list AND a per-row snapshot of every column value.
        // attoid is the unique identity of pg_attribute (assigned by oid_gen
        // and never reused), so at most one row matches.
        auto& tbl = entry->table_storage.table();
        const std::size_t col_count = tbl.column_count();
        std::vector<std::int64_t> all_col_indices;
        all_col_indices.reserve(col_count);
        for (std::size_t i = 0; i < col_count; ++i) {
            all_col_indices.push_back(static_cast<std::int64_t>(i));
        }

        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        std::pmr::vector<components::types::logical_value_t> row_values(resource());
        row_values.reserve(col_count);

        inline_scan(tbl,
                    all_col_indices,
                    &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v0 = chunk.value(0, i);
                        if (v0.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(v0.value<std::uint32_t>()) != attoid)
                            return true;
                        row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        for (std::size_t c = 0; c < col_count; ++c) {
                            row_values.push_back(chunk.value(static_cast<uint64_t>(c), i));
                        }
                        return false; // single-row identity — short-circuit
                    });
        if (row_ids.empty()) {
            trace(log_,
                  "update_pg_attribute_commit_id_field: attoid={} not found (skipping backfill)",
                  static_cast<unsigned>(attoid));
            co_return;
        }

        // patch the target column in the read-back snapshot. Column
        // 10 = added_at_commit_id, column 11 = dropped_at_commit_id (see
        // components/catalog/system_table_schemas.cpp pg_attribute_columns()).
        const std::size_t patch_col_idx =
            (kind == components::pg_attribute_commit_id_backfill_t::kind_t::added_at) ? 10u : 11u;
        if (patch_col_idx >= row_values.size()) {
            trace(log_,
                  "update_pg_attribute_commit_id_field: patch_col_idx={} out of range (col_count={})",
                  patch_col_idx,
                  col_count);
            co_return;
        }
        row_values[patch_col_idx] =
            components::types::logical_value_t(resource(), static_cast<std::int64_t>(commit_id));

        // build a full-width update chunk. Every column carries its
        // pre-existing value (taken from the scan); only column patch_col_idx
        // carries the new commit_id. Aliases mirror the table's column names
        // so direct_update_sync's name-match routing maps each vector to the
        // correct storage column.
        const auto& table_columns = entry->table_storage.table().columns();
        std::pmr::vector<components::types::complex_logical_type> chunk_types(resource());
        chunk_types.reserve(table_columns.size());
        for (const auto& col_def : table_columns) {
            auto t = col_def.type();
            t.set_alias(col_def.name());
            chunk_types.push_back(std::move(t));
        }
        components::vector::data_chunk_t patch(resource(), chunk_types, 1);
        patch.set_cardinality(1);
        for (std::size_t c = 0; c < table_columns.size() && c < row_values.size(); ++c) {
            if (row_values[c].is_null()) {
                patch.data[c].validity().set_invalid(0);
            } else {
                patch.data[c].set_value(0, row_values[c]);
            }
        }

        // WAL physical_update emission. Paired with the matching
        // physical_insert already written by operator_alter_column_{add,drop,
        // rename}; replay applies them in WAL order so the column commit_id
        // materializes alongside the row. Note: the WAL chunk mirrors the
        // patch chunk full-width — replay's direct_update_sync uses the same
        // alias-matching path.
        if (manager_wal_ != actor_zeta::address_t::empty_address()) {
            auto wal_chunk = std::make_unique<components::vector::data_chunk_t>(resource(), chunk_types, 1);
            wal_chunk->set_cardinality(1);
            for (std::size_t c = 0; c < table_columns.size() && c < row_values.size(); ++c) {
                if (row_values[c].is_null()) {
                    wal_chunk->data[c].validity().set_invalid(0);
                } else {
                    wal_chunk->data[c].set_value(0, row_values[c]);
                }
            }
            std::pmr::vector<std::int64_t> wal_row_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                             &wal::manager_wal_replicate_t::write_physical_update,
                                             ctx.session,
                                             pg_attr_oid,
                                             std::move(wal_row_ids),
                                             std::move(wal_chunk),
                                             static_cast<std::uint64_t>(row_ids.size()),
                                             ctx.txn.transaction_id,
                                             components::catalog::well_known_oid::main_database);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_,
                      "update_pg_attribute_commit_id_field: WAL write returned zero id for attoid={}",
                      static_cast<unsigned>(attoid));
            }
        }

        // apply the in-place full-row update. The storage::update
        // call rewrites every column in `row_ids`; we've staged all
        // pre-existing values plus the patched commit_id field.
        direct_update_sync(pg_attr_oid, row_ids, patch);
        co_return;
    }

    manager_disk_t::unique_future<std::uint64_t>
    manager_disk_t::compact_relkind_g_storage(execution_context_t ctx,
                                              components::catalog::oid_t table_oid,
                                              std::set<std::string> live_attnames) {
        record_session(ctx.session);
        // The routed agent slice owns the canonical IN_MEMORY twin for
        // relkind 'g' computed-column carriers. Read the column list via
        // storage_entry_sync (borrow safe while agent mailbox is idle), then
        // forward the mutation via mailbox send to drop_column_inner.
        if (agents_.empty())
            co_return 0;
        const std::size_t pool_idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[pool_idx] == nullptr)
            co_return 0;
        const collection_storage_entry_t* read_entry = agents_[pool_idx]->storage_entry_sync(table_oid);
        if (read_entry == nullptr)
            co_return 0;
        if (read_entry->table_storage.mode() != storage_mode_t::IN_MEMORY) {
            trace(log_,
                  "compact_relkind_g_storage: skip DISK-backed oid={} (out of scope)",
                  static_cast<unsigned>(table_oid));
            co_return 0;
        }

        std::vector<std::string> to_drop;
        {
            const auto& cols = read_entry->table_storage.table().columns();
            to_drop.reserve(cols.size());
            for (const auto& c : cols) {
                if (live_attnames.find(c.name()) == live_attnames.end()) {
                    to_drop.push_back(c.name());
                }
            }
        }

        // Fanout to the owning agent so the IN_MEMORY twin is rebuilt on
        // the agent thread. The column list was computed from a fresh read
        // of the agent's own twin a few lines above, so every column in
        // to_drop is expected to be present at the moment of the call
        // (idempotent re-issue would log "not found" but not change the
        // canonical count). agent_disk_t::drop_column_inner returns void;
        // the dropped counter equals to_drop.size() under the contract that
        // the manager mailbox handler is the sole writer for this OID.
        auto& agent = agents_[pool_idx];
        for (const auto& attname : to_drop) {
            std::pmr::string column_name{attname.data(), attname.size(), resource()};
            auto [needs_sched, fut] =
                actor_zeta::otterbrix::send(agent->address(),
                                             &agent_disk_t::drop_column_inner,
                                             table_oid,
                                             std::move(column_name));
            if (needs_sched) {
                scheduler_disk_->enqueue(agent.get());
            }
            co_await std::move(fut);
        }
        co_return static_cast<std::uint64_t>(to_drop.size());
    }

} // namespace services::disk

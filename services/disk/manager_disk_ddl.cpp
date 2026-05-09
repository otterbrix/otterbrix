#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Crash-safe pg_catalog row append: WAL is written first so a crash before the
    // storage update can be replayed on restart, then storage is updated. WAL is skipped
    // when the WAL actor isn't yet wired up (bootstrap path). The disk actor owns both
    // WAL and storage ends, avoiding a round-trip through the executor.
    manager_disk_t::unique_future<components::pg_catalog_append_range_t>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                            collection_full_name_t name,
                                            components::vector::data_chunk_t row) {
        const bool wal_available = (manager_wal_ != actor_zeta::address_t::empty_address());
        if (wal_available) {
            // Deep-copy the row into a unique_ptr for WAL transport.
            auto wal_chunk = std::make_unique<components::vector::data_chunk_t>(
                resource(), row.types(), row.size());
            wal_chunk->set_cardinality(row.size());
            for (uint64_t col = 0; col < row.column_count(); col++) {
                for (uint64_t r = 0; r < row.size(); r++) {
                    wal_chunk->data[col].set_value(r, row.data[col].value(r));
                }
            }
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                              &wal::manager_wal_replicate_t::write_physical_insert,
                                              ctx.session,
                                              std::string(name.database),
                                              std::string(name.collection),
                                              std::move(wal_chunk),
                                              std::uint64_t{0},
                                              static_cast<std::uint64_t>(row.size()),
                                              ctx.txn.transaction_id);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_, "pg_catalog insert WAL write returned zero id for {}.{}", name.database, name.collection);
            }
        }
        const auto count = static_cast<std::uint64_t>(row.size());
        const auto start_row = direct_append_sync(name, row, ctx.txn);
        // Phase 5b: return the swap range. For txn_id==0 (bootstrap/replay) or
        // count==0, return count=0 so callers can skip recording on the pipeline ctx.
        if (ctx.txn.transaction_id == 0 || count == 0) {
            co_return components::pg_catalog_append_range_t{name, static_cast<int64_t>(start_row), 0};
        }
        co_return components::pg_catalog_append_range_t{name, static_cast<int64_t>(start_row), count};
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::delete_pg_catalog_rows(execution_context_t ctx,
                                            collection_full_name_t name,
                                            std::int64_t oid_col_idx,
                                            components::catalog::oid_t target_oid) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            co_return;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        std::pmr::vector<std::int64_t> row_ids(resource());
        inline_scan(it->second->table_storage.table(), {oid_col_idx}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            row_ids.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return true;
                    });
        if (row_ids.empty()) {
            co_return;
        }
        // WAL-first: write the physical_delete record so a crash before the in-memory
        // tombstone is replayed correctly on restart.
        if (manager_wal_ != actor_zeta::address_t::empty_address()) {
            std::pmr::vector<std::int64_t> wal_ids(row_ids.begin(), row_ids.end(), resource());
            auto [_w, wf] = actor_zeta::send(manager_wal_,
                                              &wal::manager_wal_replicate_t::write_physical_delete,
                                              ctx.session,
                                              std::string(name.database),
                                              std::string(name.collection),
                                              std::move(wal_ids),
                                              static_cast<std::uint64_t>(row_ids.size()),
                                              ctx.txn.transaction_id);
            if (auto wal_id = co_await std::move(wf); wal_id == wal::id_t{}) {
                trace(log_, "pg_catalog delete WAL write returned zero id for {}.{}", name.database, name.collection);
            }
        }
        direct_delete_sync(name, row_ids, static_cast<std::uint64_t>(row_ids.size()), ctx.txn);
        co_return;
    }

    // ALTER TABLE ADD COLUMN lives in operator_alter_column_add; relkind='g'
    // tables track columns via pg_computed_column.

    // ========================================================================
    // Phase 7.5b — physical column compaction for relkind='g' tables.
    // Mirrors the GC strategy used in pg_computed_column (operator_vacuum step 5):
    // after refcount<=0 rows + stale-version rows are deleted from the catalog,
    // their corresponding physical columns in the underlying IN_MEMORY
    // table_storage_t are still occupying memory. We drop them here.
    //
    // Caller passes the set of attnames that ARE alive (resolver-visible), and
    // we drop every storage column whose name is NOT in that set. This avoids
    // requiring the caller to compute the dead set on its end (which would
    // otherwise need to read the storage column list cross-actor).
    //
    // DISK-mode is out of scope for Phase 7.5b — the rebuild constructor uses
    // collection_t::remove_column which is in-memory only; segment-level
    // rewrites + checkpoint coordination would be needed for a disk-backed
    // path. table_storage_t::drop_column returns false on DISK so we simply
    // skip those.
    // ========================================================================
    manager_disk_t::unique_future<std::uint64_t>
    manager_disk_t::compact_relkind_g_storage(execution_context_t /*ctx*/,
                                                collection_full_name_t name,
                                                std::set<std::string> live_attnames) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            co_return 0;
        }
        if (it->second->table_storage.mode() != storage_mode_t::IN_MEMORY) {
            // DISK: out of scope (Phase 7.5b). Logged so it surfaces in tests.
            trace(log_,
                  "compact_relkind_g_storage: skip DISK-backed {}.{} (out of scope)",
                  name.database,
                  name.collection);
            co_return 0;
        }

        // Snapshot the storage column names BEFORE iterating so we don't
        // re-read the (mutating) column list across drop_column calls.
        std::vector<std::string> to_drop;
        {
            const auto& cols = it->second->table_storage.table().columns();
            to_drop.reserve(cols.size());
            for (const auto& c : cols) {
                if (live_attnames.find(c.name()) == live_attnames.end()) {
                    to_drop.push_back(c.name());
                }
            }
        }

        std::uint64_t dropped = 0;
        for (const auto& attname : to_drop) {
            // collection_storage_entry_t::drop_column rebuilds data_table_t AND
            // recreates the storage adapter (which holds a data_table_t&).
            if (it->second->drop_column(attname, resource())) {
                ++dropped;
            }
        }
        co_return dropped;
    }
    // ========================================================================


    // ========================================================================
    // resolve_* coroutines.
    // ------------------------------------------------------------------------
    // Each resolve_* scans the relevant pg_catalog.* table on the disk actor thread
    // (synchronous data_table_t::scan; same pattern as restore_oid_generator_sync).
    // Found result + invalidation-event tail since the caller's last-seen version are
    // returned in one roundtrip — the plan cache caches by (plan_hash, catalog_version)
    // and applies the events to its other entries.
    // ========================================================================

} // namespace services::disk

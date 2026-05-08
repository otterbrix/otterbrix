#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Crash-safe pg_catalog row append: WAL is written first so a crash before the
    // storage update can be replayed on restart, then storage is updated. WAL is skipped
    // when the WAL actor isn't yet wired up (bootstrap path). The disk actor owns both
    // WAL and storage ends, avoiding a round-trip through the executor.
    manager_disk_t::unique_future<void>
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
        // Track this append so commit_pg_catalog_appends can flip MVCC tags after
        // the dispatcher's WAL commit_txn + txn_manager.commit. Skip txn=0 (bootstrap /
        // replay) since those rows are committed-at-zero already.
        if (ctx.txn.transaction_id != 0 && count > 0) {
            pending_pg_catalog_appends_[ctx.txn.transaction_id].push_back(
                {name, static_cast<int64_t>(start_row), count});
        }
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::commit_pg_catalog_appends(execution_context_t ctx, std::uint64_t commit_id) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0) {
            co_return;
        }
        // Inserts: walk the per-txn append log and flip insert_id from txn_id → commit_id.
        auto it = pending_pg_catalog_appends_.find(txn_id);
        if (it != pending_pg_catalog_appends_.end()) {
            for (const auto& p : it->second) {
                auto* s = get_storage(p.name);
                if (s) {
                    s->commit_append(commit_id, p.start_row, p.count);
                }
            }
            pending_pg_catalog_appends_.erase(it);
        }
        // Deletes: ddl_drop_* paths use direct_delete_sync which tombstones rows with
        // delete_id=txn_id. Walk every pg_catalog.* storage and flip tombstones tagged
        // by this txn to commit_id. commit_all_deletes is a no-op when no row matches.
        for (const auto& tbl : catalog::all_system_tables()) {
            const collection_full_name_t sn{"pg_catalog", "main", std::string(tbl.name)};
            auto* s = get_storage(sn);
            if (s) {
                s->commit_all_deletes(txn_id, commit_id);
            }
        }
        // Rebuild lookup indexes so any inserts/deletes committed here are reflected.
        // Bump catalog_version_ so dispatcher's refresh_invalidations_ clears stale
        // plan_cache entries (e.g., after DROP TABLE the table must not be found on
        // the next validate call).
        ++catalog_version_;
        rebuild_lookup_indexes();
        co_return;
    }

    manager_disk_t::unique_future<void>
    manager_disk_t::revert_pg_catalog_appends(execution_context_t ctx) {
        const auto txn_id = ctx.txn.transaction_id;
        if (txn_id == 0) {
            co_return;
        }
        auto it = pending_pg_catalog_appends_.find(txn_id);
        if (it == pending_pg_catalog_appends_.end()) {
            co_return;
        }
        // Iterate in reverse so revert_append cleanup ordering is consistent.
        for (auto rit = it->second.rbegin(); rit != it->second.rend(); ++rit) {
            auto* s = get_storage(rit->name);
            if (s) {
                s->revert_append(rit->start_row, rit->count);
            }
        }
        pending_pg_catalog_appends_.erase(it);
        co_return;
    }

    bool manager_disk_t::pg_oid_exists(const collection_full_name_t& table_name,
                                       std::uint64_t oid_col,
                                       components::catalog::oid_t target_oid) const {
        bool found = false;
        auto it = storages_.find(table_name);
        if (it == storages_.end()) return false;
        std::pmr::synchronized_pool_resource res;
        inline_scan(it->second->table_storage.table(), {static_cast<std::int64_t>(oid_col)}, &res,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (!v.is_null() &&
                            static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) == target_oid) {
                            found = true;
                            return false; // stop
                        }
                        return true;
                    });
        return found;
    }

    void manager_disk_t::delete_system_rows_by_oid_match(const collection_full_name_t& name,
                                                          std::int64_t oid_col_idx,
                                                          components::catalog::oid_t target_oid,
                                                          const components::table::transaction_data& txn) {
        auto it = storages_.find(name);
        if (it == storages_.end()) {
            return;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // Scan storing only the matched column; row_ids on the chunk identify rows.
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
            return;
        }
        direct_delete_sync(name, row_ids, static_cast<std::uint64_t>(row_ids.size()), txn);
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

    // Column lifecycle DDL — pg_attribute mutations under MVCC.

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_add_column(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    components::table::column_definition_t column) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_add_column : table {}", table_oid);
        // Storage side-effect: update in-memory schema of the loaded table so
        // subsequent INSERTs see the new column without requiring a restart.
        // Catalog rows (pg_attribute) are written by the dispatcher before this call.
        {
            std::pmr::synchronized_pool_resource scan_resource;
            std::string rel_ns_name, rel_name;
            std::unordered_map<components::catalog::oid_t, std::string> ns_oid_to_name;
            if (auto ns_it = storages_.find(pg_namespace_name); ns_it != storages_.end()) {
                inline_scan(ns_it->second->table_storage.table(), {0, 1}, &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto oid_v = chunk.value(0, i);
                                auto name_v = chunk.value(1, i);
                                if (!oid_v.is_null() && !name_v.is_null()) {
                                    ns_oid_to_name.emplace(
                                        static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()),
                                        std::string(name_v.value<std::string_view>()));
                                }
                                return true;
                            });
            }
            if (auto cls_it = storages_.find(pg_class_name); cls_it != storages_.end()) {
                inline_scan(cls_it->second->table_storage.table(), {0, 1, 2}, &scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto oid_v = chunk.value(0, i);
                                if (oid_v.is_null())
                                    return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != table_oid)
                                    return true;
                                auto name_v = chunk.value(1, i);
                                auto ns_v = chunk.value(2, i);
                                if (name_v.is_null() || ns_v.is_null())
                                    return false;
                                auto ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                                auto ns_it2 = ns_oid_to_name.find(ns_oid);
                                if (ns_it2 == ns_oid_to_name.end() || ns_it2->second == "pg_catalog")
                                    return false;
                                rel_ns_name = ns_it2->second;
                                rel_name = std::string(name_v.value<std::string_view>());
                                return false;
                            });
            }
            if (!rel_name.empty()) {
                collection_full_name_t user_key{rel_ns_name, rel_name};
                if (auto user_it = storages_.find(user_key); user_it != storages_.end()) {
                    user_it->second->add_column(column, resource());
                }
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                 invalidation_kind::relation_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_adopt_computing_schema(execution_context_t ctx,
                                                components::catalog::oid_t table_oid,
                                                std::vector<components::table::column_definition_t> columns) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_adopt_computing_schema : table {} ({} cols)",
              table_oid, columns.size());
        auto it = storages_.find(pg_class_name);
        if (it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        std::string found_name;
        components::catalog::oid_t found_ns = components::catalog::INVALID_OID;
        std::string found_storagemode;
        std::pmr::vector<std::int64_t> rows_to_delete(resource());
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        if (oid_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != table_oid)
                            return true;
                        auto rname = chunk.value(1, i);
                        auto rns = chunk.value(2, i);
                        auto rkind = chunk.value(3, i);
                        auto rsm = chunk.value(4, i);
                        if (!rname.is_null())
                            found_name = std::string(rname.value<std::string_view>());
                        if (!rns.is_null())
                            found_ns = static_cast<components::catalog::oid_t>(rns.value<std::uint32_t>());
                        if (!rsm.is_null())
                            found_storagemode = std::string(rsm.value<std::string_view>());
                        if (!rkind.is_null() && !rkind.value<std::string_view>().empty() &&
                            rkind.value<std::string_view>().front() == catalog::relkind::computed) {
                            rows_to_delete.push_back(chunk.row_ids.data<std::int64_t>()[i]);
                        }
                        return false;
                    });
        if (rows_to_delete.empty()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        direct_delete_sync(pg_class_name, rows_to_delete, static_cast<std::uint64_t>(rows_to_delete.size()));
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, catalog::relkind::regular);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_str(res, found_name));
                chunk.set_value(2, 0, lv_oid(res, found_ns));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, found_storagemode.empty() ? std::string("d") : found_storagemode));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        // Write pg_attribute rows for the inferred schema (mirrors create_relation_impl).
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            std::int32_t attnum = 0;
            for (auto& col : columns) {
                ++attnum;
                const auto attoid = oid_gen_.allocate();
                col.set_attoid(attoid);
                components::catalog::oid_t atttypid = components::catalog::INVALID_OID;
                {
                    std::string lookup;
                    const auto lt = col.type().type();
                    if (lt == types::logical_type::UNKNOWN) {
                        lookup = col.type().type_name();
                    } else if (lt == types::logical_type::DECIMAL) {
                        lookup = "numeric";
                    } else {
                        lookup = std::string{logical_type_to_pg_name(lt)};
                    }
                    if (!lookup.empty()) {
                        auto type_it = storages_.find(pg_type_name);
                        if (type_it != storages_.end()) {
                            std::pmr::synchronized_pool_resource scan_resource;
                            inline_scan(type_it->second->table_storage.table(), {0, 1}, &scan_resource,
                                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                            if (!str_equals(chunk.value(1, i), lookup))
                                                return true;
                                            atttypid = static_cast<components::catalog::oid_t>(
                                                chunk.value(0, i).value<std::uint32_t>());
                                            return false;
                                        });
                        }
                    }
                }
                col.set_atttypid(atttypid);
                std::string typspec = encode_type_spec(col.type());
                std::string defspec;
                if (col.has_default_value()) {
                    defspec = components::catalog::encode_default_spec(col.default_value());
                }
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, attoid));
                    chunk.set_value(1, 0, lv_oid(res, table_oid));
                    chunk.set_value(2, 0, lv_str(res, col.name()));
                    chunk.set_value(3, 0, lv_oid(res, atttypid));
                    chunk.set_value(4, 0, lv_i32(res, attnum));
                    chunk.set_value(5, 0, lv_bool(res, col.is_not_null()));
                    chunk.set_value(6, 0, lv_bool(res, col.has_default_value()));
                    chunk.set_value(7, 0, lv_bool(res, false));
                    chunk.set_value(8, 0, lv_str(res, typspec));
                    chunk.set_value(9, 0, lv_str(res, defspec));
                });
                co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
            }
        }
        // Rebuild the in-memory lookup index so that subsequent resolve_table calls see
        // relkind='r' (not the stale 'g') and read columns from pg_attribute.
        rebuild_lookup_indexes();
        co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }


    // ddl_computed_append: register (or refcount-bump) a (field_name, type_oid) pair on a
    // computing table. Semantics mirror computed_schema::append in the legacy catalog —
    // a new field starts at attversion=1, attrefcount=1; appending the same type bumps the
    // existing live row's refcount; appending a new type for an existing field allocates
    // a new attoid and bumps attversion (max + 1). MVCC update = delete + insert.
    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_computed_append(execution_context_t ctx,
                                         components::catalog::oid_t table_oid,
                                         std::string field_name,
                                         components::catalog::oid_t type_oid) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_computed_append : table {} field {} type {}",
              table_oid, field_name, type_oid);

        auto it = storages_.find(pg_computed_column_name);
        if (it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        // Scan existing rows for this (table, field). Track:
        //   - max_version       : highest attversion seen (any refcount)
        //   - same_type_row_id  : row id of a live row with matching type_oid (to bump it)
        std::int64_t max_version = 0;
        std::int64_t same_type_row_id = -1;
        std::int64_t same_type_refcount = 0;
        std::int64_t same_type_version = 0;
        components::catalog::oid_t same_type_attoid = components::catalog::INVALID_OID;
        {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto relid_v = chunk.value(0, i);
                            if (relid_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(relid_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            if (!str_equals(chunk.value(2, i), field_name))
                                return true;
                            auto ver_v = chunk.value(4, i);
                            const auto v = ver_v.is_null() ? 0 : ver_v.value<std::int64_t>();
                            if (v > max_version) {
                                max_version = v;
                            }
                            auto type_v = chunk.value(3, i);
                            auto refc_v = chunk.value(5, i);
                            if (type_v.is_null() || refc_v.is_null())
                                return true;
                            const auto rc = refc_v.value<std::int64_t>();
                            const auto t = static_cast<components::catalog::oid_t>(
                                type_v.value<std::uint32_t>());
                            if (t == type_oid && rc > 0) {
                                same_type_row_id = chunk.row_ids.data<std::int64_t>()[i];
                                same_type_refcount = rc;
                                same_type_version = v;
                                auto attoid_v = chunk.value(1, i);
                                if (!attoid_v.is_null()) {
                                    same_type_attoid = static_cast<components::catalog::oid_t>(
                                        attoid_v.value<std::uint32_t>());
                                }
                            }
                            return true;
                        });
        }

        auto* def = components::catalog::find_system_table("pg_computed_column");
        if (def == nullptr) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        if (same_type_row_id >= 0) {
            // Existing live row with same type: MVCC-update by delete + reinsert with refcount+1.
            std::pmr::vector<std::int64_t> row_ids(resource());
            row_ids.push_back(same_type_row_id);
            direct_delete_sync(pg_computed_column_name, row_ids, 1u, ctx.txn);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_oid(res, same_type_attoid));
                chunk.set_value(2, 0, lv_str(res, field_name));
                chunk.set_value(3, 0, lv_oid(res, type_oid));
                chunk.set_value(4, 0, lv_i64(res, same_type_version));
                chunk.set_value(5, 0, lv_i64(res, same_type_refcount + 1));
            });
            co_await append_pg_catalog_row(ctx, pg_computed_column_name, std::move(row));
        } else {
            // New (field, type) pair: allocate attoid, attversion = max + 1, refcount = 1.
            const auto attoid = oid_gen_.allocate();
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_oid(res, attoid));
                chunk.set_value(2, 0, lv_str(res, field_name));
                chunk.set_value(3, 0, lv_oid(res, type_oid));
                chunk.set_value(4, 0, lv_i64(res, max_version + 1));
                chunk.set_value(5, 0, lv_i64(res, 1));
            });
            co_await append_pg_catalog_row(ctx, pg_computed_column_name, std::move(row));
        }

        co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                 invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    // ddl_computed_drop: decrement attrefcount on the latest live row for this (table, field).
    // refcount > 1 → MVCC-update (delete + reinsert with rc-1). refcount == 1 → just delete.
    // No live row → idempotent no-op (still bumps catalog_version since the call is a DDL).
    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_computed_drop(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::string field_name) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_computed_drop : table {} field {}",
              table_oid, field_name);

        auto it = storages_.find(pg_computed_column_name);
        if (it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        std::int64_t target_row_id = -1;
        std::int64_t target_version = -1;
        std::int64_t target_refcount = 0;
        components::catalog::oid_t target_attoid = components::catalog::INVALID_OID;
        components::catalog::oid_t target_type = components::catalog::INVALID_OID;
        {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto relid_v = chunk.value(0, i);
                            if (relid_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(relid_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            if (!str_equals(chunk.value(2, i), field_name))
                                return true;
                            auto refc_v = chunk.value(5, i);
                            if (refc_v.is_null())
                                return true;
                            const auto rc = refc_v.value<std::int64_t>();
                            if (rc <= 0)
                                return true;
                            auto ver_v = chunk.value(4, i);
                            const auto v = ver_v.is_null() ? 0 : ver_v.value<std::int64_t>();
                            if (v > target_version) {
                                target_version = v;
                                target_row_id = chunk.row_ids.data<std::int64_t>()[i];
                                target_refcount = rc;
                                auto attoid_v = chunk.value(1, i);
                                target_attoid = attoid_v.is_null()
                                    ? components::catalog::INVALID_OID
                                    : static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>());
                                auto type_v = chunk.value(3, i);
                                target_type = type_v.is_null()
                                    ? components::catalog::INVALID_OID
                                    : static_cast<components::catalog::oid_t>(type_v.value<std::uint32_t>());
                            }
                            return true;
                        });
        }

        if (target_row_id < 0) {
            // No live entry — idempotent.
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::computing_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        std::pmr::vector<std::int64_t> row_ids(resource());
        row_ids.push_back(target_row_id);
        direct_delete_sync(pg_computed_column_name, row_ids, 1u, ctx.txn);

        if (target_refcount > 1) {
            if (auto* def = components::catalog::find_system_table("pg_computed_column")) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, table_oid));
                    chunk.set_value(1, 0, lv_oid(res, target_attoid));
                    chunk.set_value(2, 0, lv_str(res, field_name));
                    chunk.set_value(3, 0, lv_oid(res, target_type));
                    chunk.set_value(4, 0, lv_i64(res, target_version));
                    chunk.set_value(5, 0, lv_i64(res, target_refcount - 1));
                });
                co_await append_pg_catalog_row(ctx, pg_computed_column_name, std::move(row));
            }
        }

        co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                 invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    // ========================================================================
    // resolve_* coroutines + recent_invalidations_since.
    // ------------------------------------------------------------------------
    // Each resolve_* scans the relevant pg_catalog.* table on the disk actor thread
    // (synchronous data_table_t::scan; same pattern as restore_oid_generator_sync).
    // Found result + invalidation-event tail since the caller's last-seen version are
    // returned in one roundtrip — the plan cache caches by (plan_hash, catalog_version)
    // and applies the events to its other entries.
    // ========================================================================

} // namespace services::disk

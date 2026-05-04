#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::create_relation_impl(execution_context_t ctx,
                                          components::catalog::oid_t namespace_oid,
                                          std::string name,
                                          std::vector<components::table::column_definition_t> columns,
                                          char relkind) {
        const auto table_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::create_relation_impl : {}.{} relkind={} -> oid {}",
              namespace_oid, name, relkind, table_oid);
        auto r = make_ddl_result(resource(), table_oid, invalidation_kind::relation_added,
                                 namespace_oid, version);

        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, relkind);
            const std::string storagemode_str(1, catalog::relstoragemode::disk);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, table_oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (ctx.txn.transaction_id == 0) {
            ns_table_key_t key{namespace_oid, name};
            table_to_oid_.emplace(key, table_index_entry_t{table_oid, relkind});
            table_oid_to_key_.emplace(table_oid, std::move(key));
        }
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            std::int32_t attnum = 0;
            for (auto& col : columns) {
                ++attnum;
                const auto attoid = oid_gen_.allocate();
                col.set_attoid(attoid);
                r.all_oids.emplace(col.name(), attoid);
                // Resolve atttypid via pg_type scan. For UNKNOWN types (produced by
                // transformer), use type_name(). DECIMAL → "numeric". Built-in scalars use
                // the canonical pg_type name from logical_type_to_pg_name().
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

                // (b) atttypspec: text-encoded complex type. Empty for builtin scalars.
                std::string typspec = encode_type_spec(col.type());
                // (c) attdefspec: flat-text encoded default value. Empty when no default.
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

                // Column → type 'n' pg_depend (only when atttypid was resolvable).
                if (atttypid != components::catalog::INVALID_OID) {
                    if (auto* dep_def = components::catalog::find_system_table("pg_depend")) {
                        auto dep_row = make_row(resource(), dep_def->columns,
                                                  [&](data_chunk_t& chunk, auto* res) {
                                                      chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                                                      chunk.set_value(1, 0, lv_oid(res, attoid));
                                                      chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_type_table));
                                                      chunk.set_value(3, 0, lv_oid(res, atttypid));
                                                      chunk.set_value(4, 0, lv_str(res, "n"));
                                                  });
                        co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(dep_row));
                    }
                }
            }
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            // Relation → namespace ('n' normal dependency)
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));

        }
        co_return finalize_ddl(std::move(r));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_database(execution_context_t ctx, std::string name) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_database : {} -> oid {}", name, oid);
        if (auto* def = components::catalog::find_system_table("pg_database")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* r) {
                chunk.set_value(0, 0, lv_oid(r, oid));
                chunk.set_value(1, 0, lv_str(r, name));
            });
            co_await append_pg_catalog_row(ctx, pg_database_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid,
                                                 invalidation_kind::database_added,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_database(execution_context_t ctx,
                                       components::catalog::oid_t database_oid,
                                       drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_database : {} (behavior={})",
              database_oid, static_cast<int>(behavior));
        // Walk pg_depend for dependents (refclassid=pg_database_table, refobjid=db_oid).
        // Today only namespace→database edges are minted (in ddl_create_namespace, future).
        // For each dependent: RESTRICT → fail, CASCADE → recursive drop.
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_database_table,
                                        database_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_database_table,
                database_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        for (auto& dep : deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_namespace_table) {
                co_await ddl_drop_namespace(ctx, dep.objid, drop_behavior_t::cascade_);
            }
        }
        // WAL-then-MVCC-delete the pg_database row itself.
        co_await delete_pg_catalog_rows(ctx, pg_database_name, /*oid_col*/ 0, database_oid);
        co_return finalize_ddl(make_ddl_result(resource(), database_oid,
                                                 invalidation_kind::database_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_namespace(execution_context_t ctx, std::string name) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_namespace : {} -> oid {}", name, oid);
        if (auto* def = components::catalog::find_system_table("pg_namespace")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* r) {
                chunk.set_value(0, 0, lv_oid(r, oid));
                chunk.set_value(1, 0, lv_str(r, name));
            });
            co_await append_pg_catalog_row(ctx, pg_namespace_name, std::move(row));
        }
        if (ctx.txn.transaction_id == 0) {
            ns_name_to_oid_.emplace(name, oid);
            ns_oid_to_name_.emplace(oid, std::move(name));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid,
                                                 invalidation_kind::namespace_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_namespace(execution_context_t ctx,
                                        components::catalog::oid_t namespace_oid,
                                        drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_namespace : {} (behavior={})",
              namespace_oid, static_cast<int>(behavior));
        if (!pg_oid_exists(pg_namespace_name, /*oid_col*/ 0, namespace_oid)) {
            ddl_result_t _nf{resource()};
            _nf.status = ddl_status::not_found;
            co_return _nf;
        }
        const auto version = ++catalog_version_;
        // Walk pg_depend for dependents (refclassid=pg_namespace_table, refobjid=ns_oid).
        // For each dependent table/type/function: RESTRICT → fail, CASCADE → recursive drop.
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_namespace_table, namespace_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE: pre-validate the dependency graph for cycles via dependency_walker.
        // If a cycle exists (extremely rare given no FK references in our schema), abort
        // before issuing any partial drops — better than hanging in infinite recursion.
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_namespace_table,
                namespace_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        for (auto& dep : deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_class_table) {
                // Tables, sequences, views, macros, indexes — uniform pg_class teardown.
                co_await ddl_drop_table(ctx, dep.objid, drop_behavior_t::cascade_);
            } else if (dep.classid == components::catalog::well_known_oid::pg_type_table) {
                co_await ddl_drop_type(ctx, dep.objid, drop_behavior_t::cascade_);
            } else if (dep.classid == components::catalog::well_known_oid::pg_proc_table) {
                co_await ddl_drop_function(ctx, dep.objid, drop_behavior_t::cascade_);
            }
        }
        // WAL-then-MVCC-delete the pg_namespace row itself.
        co_await delete_pg_catalog_rows(ctx, pg_namespace_name, /*oid_col*/ 0, namespace_oid);
        if (ctx.txn.transaction_id == 0) {
            if (auto it = ns_oid_to_name_.find(namespace_oid); it != ns_oid_to_name_.end()) {
                ns_name_to_oid_.erase(it->second);
                ns_oid_to_name_.erase(it);
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), namespace_oid, invalidation_kind::namespace_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_table(execution_context_t ctx,
                                      components::catalog::oid_t namespace_oid,
                                      std::string name,
                                      std::vector<components::table::column_definition_t> columns,
                                      char relkind) {
        co_return co_await create_relation_impl(ctx, namespace_oid, std::move(name), std::move(columns), relkind);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_table(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_table : {} (behavior={})",
              table_oid, static_cast<int>(behavior));
        if (!pg_oid_exists(pg_class_name, /*oid_col*/ 0, table_oid)) {
            ddl_result_t _nf{resource()};
            _nf.status = ddl_status::not_found;
            co_return _nf;
        }
        const auto version = ++catalog_version_;
        // pg_depend traversal — find dependents (e.g. indexes, constraints, computing-table rules).
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_class_table, table_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE: pre-validate the dependency graph for cycles via dependency_walker.
        // Mirrors ddl_drop_namespace's pattern. Cycles can arise via FK self-references or
        // pathological pg_depend rows; without this, the recursive cascade below would loop.
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_class_table,
                table_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        for (auto& dep : deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_class_table) {
                const char kind = read_relkind(dep.objid);
                if (kind == catalog::relkind::index) {
                    co_await ddl_drop_index(ctx, dep.objid, drop_behavior_t::cascade_);
                } else if (kind == catalog::relkind::sequence) {
                    co_await ddl_drop_sequence(ctx, dep.objid, drop_behavior_t::cascade_);
                } else if (kind == catalog::relkind::view) {
                    co_await ddl_drop_view(ctx, dep.objid, drop_behavior_t::cascade_);
                } else if (kind == catalog::relkind::macro) {
                    co_await ddl_drop_macro(ctx, dep.objid, drop_behavior_t::cascade_);
                } else {
                    co_await ddl_drop_table(ctx, dep.objid, drop_behavior_t::cascade_);
                }
            }
        }
        // WAL-then-MVCC-delete: pg_class row + all pg_attribute rows for this table.
        co_await delete_pg_catalog_rows(ctx, pg_class_name, /*oid*/ 0, table_oid);
        co_await delete_pg_catalog_rows(ctx, pg_attribute_name, /*attrelid*/ 1, table_oid);
        // pg_constraint: sweep rows where conrelid==table_oid (col 2) and where
        // confrelid==table_oid (col 4, inbound FK references to this table).
        co_await delete_pg_catalog_rows(ctx, pg_constraint_name, /*conrelid*/ 2, table_oid);
        co_await delete_pg_catalog_rows(ctx, pg_constraint_name, /*confrelid*/ 4, table_oid);
        // pg_index: sweep index metadata rows that reference this table as indrelid (col 1).
        co_await delete_pg_catalog_rows(ctx, pg_index_name, /*indrelid*/ 1, table_oid);
        // pg_depend cleanup: rows where (objid=table_oid) and (refobjid=table_oid).
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, table_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, table_oid);
        if (ctx.txn.transaction_id == 0) {
            if (auto it = table_oid_to_key_.find(table_oid); it != table_oid_to_key_.end()) {
                table_to_oid_.erase(it->second);
                table_oid_to_key_.erase(it);
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::relation_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    // Crash-safe pg_catalog row append: WAL is written first so a crash before the
    // storage update can be replayed on restart, then storage is updated. WAL is skipped
    // when the WAL actor isn't yet wired up (bootstrap path). The disk actor owns both
    // WAL and storage ends, avoiding a round-trip through the executor.
    manager_disk_t::unique_future<void>
    manager_disk_t::append_pg_catalog_row(execution_context_t ctx,
                                            const collection_full_name_t& name,
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
        const std::array<const collection_full_name_t*, 11> pg_storages{
            &pg_namespace_name,    &pg_class_name,  &pg_attribute_name,
            &pg_type_name,         &pg_proc_name,   &pg_index_name,
            &pg_depend_name,       &pg_constraint_name, &pg_computed_column_name,
            &pg_sequence_name,     &pg_rewrite_name};
        for (const auto* sn : pg_storages) {
            auto* s = get_storage(*sn);
            if (s) {
                s->commit_all_deletes(txn_id, commit_id);
            }
        }
        // Rebuild lookup indexes so any inserts/deletes committed here are reflected.
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

    // Helpers: pg_depend traversal + MVCC delete on system tables.
    std::vector<dependency_t>
    manager_disk_t::collect_dependents(components::catalog::oid_t refclassid,
                                        components::catalog::oid_t refobjid) {
        std::vector<dependency_t> out;
        auto it = storages_.find(pg_depend_name);
        if (it == storages_.end()) {
            return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_depend columns: 0=classid 1=objid 2=refclassid 3=refobjid 4=deptype
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto refcls_v = chunk.value(2, i);
                        auto refobj_v = chunk.value(3, i);
                        if (refcls_v.is_null() || refobj_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(refcls_v.value<std::uint32_t>()) != refclassid)
                            return true;
                        if (static_cast<components::catalog::oid_t>(refobj_v.value<std::uint32_t>()) != refobjid)
                            return true;
                        dependency_t r;
                        r.classid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        r.objid = static_cast<components::catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>());
                        r.refclassid = refclassid;
                        r.refobjid = refobjid;
                        auto dt_v = chunk.value(4, i);
                        if (!dt_v.is_null()) {
                            auto dt_s = dt_v.value<std::string_view>();
                            if (!dt_s.empty()) r.deptype = dt_s.front();
                        }
                        out.push_back(r);
                        return true;
                    });
        return out;
    }

    char manager_disk_t::read_relkind(components::catalog::oid_t target_oid) const {
        char result = catalog::relkind::regular;
        auto it = storages_.find(pg_class_name);
        if (it == storages_.end()) {
            return result;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class: oid(0), relname(1), relnamespace(2), relkind(3)
        inline_scan(it->second->table_storage.table(), {0, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto v = chunk.value(0, i);
                        if (v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(v.value<std::uint32_t>()) != target_oid)
                            return true;
                        auto kv = chunk.value(1, i);
                        if (!kv.is_null()) {
                            auto ks = kv.value<std::string_view>();
                            if (!ks.empty()) result = ks.front();
                        }
                        return false; // stop scan
                    });
        return result;
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
                                            const collection_full_name_t& name,
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

    // ========================================================================
    // Sequences / views / macros via pg_class with relkind 'S' / 'v' / 'm'.
    // Each ddl_create_* allocates a fresh OID, writes pg_class + pg_depend rows, and
    // emits an index_added event (an extension of the existing event taxonomy).
    // Drops delegate to ddl_drop_table — the pg_class teardown is identical.
    // ========================================================================

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_sequence(execution_context_t ctx,
                                         components::catalog::oid_t namespace_oid,
                                         std::string name,
                                         std::int64_t start,
                                         std::int64_t increment,
                                         std::int64_t min_value,
                                         std::int64_t max_value,
                                         bool cycle) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_sequence : {}.{} -> oid {}", namespace_oid, name, oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, catalog::relkind::sequence);
            const std::string storagemode_str(1, catalog::relstoragemode::disk);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        // Persist sequence parameters in pg_sequence (seqrelid FK to pg_class.oid).
        if (auto* def = components::catalog::find_system_table("pg_sequence")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));                           // seqrelid
                chunk.set_value(1, 0, lv_i64(res, start));                         // seqstart
                chunk.set_value(2, 0, lv_i64(res, increment));                     // seqincrement
                chunk.set_value(3, 0, lv_i64(res, min_value));                     // seqmin
                chunk.set_value(4, 0, lv_i64(res, max_value));                     // seqmax
                chunk.set_value(5, 0, lv_bool(res, cycle));                        // seqcycle
                chunk.set_value(6, 0, lv_i64(res, start));                         // seqlast = start initially
            });
            co_await append_pg_catalog_row(ctx, pg_sequence_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid, invalidation_kind::sequence_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_sequence(execution_context_t ctx,
                                       components::catalog::oid_t sequence_oid,
                                       drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_sequence : {} (behavior={})",
              sequence_oid, static_cast<int>(behavior));
        // Remove the pg_sequence parameters row (keyed by seqrelid, col 0) before
        // ddl_drop_table cleans up pg_class/pg_attribute/pg_depend entries.
        co_await delete_pg_catalog_rows(ctx, pg_sequence_name, /*seqrelid col*/ 0, sequence_oid);
        co_return co_await ddl_drop_table(ctx, sequence_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_view(execution_context_t ctx,
                                     components::catalog::oid_t namespace_oid,
                                     std::string name,
                                     std::string body) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_view : {}.{} -> oid {}", namespace_oid, name, oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, catalog::relkind::view);
            const std::string storagemode_str(1, catalog::relstoragemode::disk);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_rewrite")) {
            const auto rule_oid = oid_gen_.allocate();
            const std::string ev_type_str(1, 'v');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, rule_oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, oid));
                chunk.set_value(3, 0, lv_str(res, ev_type_str));
                chunk.set_value(4, 0, lv_str(res, body));
            });
            co_await append_pg_catalog_row(ctx, pg_rewrite_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid, invalidation_kind::view_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_view(execution_context_t ctx,
                                   components::catalog::oid_t view_oid,
                                   drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_view : {} (behavior={})", view_oid, static_cast<int>(behavior));
        // Remove pg_rewrite body row (ev_class col 2) before pg_class/pg_depend cleanup.
        co_await delete_pg_catalog_rows(ctx, pg_rewrite_name, /*ev_class col*/ 2, view_oid);
        co_return co_await ddl_drop_table(ctx, view_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_macro(execution_context_t ctx,
                                      components::catalog::oid_t namespace_oid,
                                      std::string name,
                                      std::string body) {
        const auto oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_macro : {}.{} -> oid {}", namespace_oid, name, oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, catalog::relkind::macro);
            const std::string storagemode_str(1, catalog::relstoragemode::disk);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_rewrite")) {
            const auto rule_oid = oid_gen_.allocate();
            const std::string ev_type_str(1, 'm');
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, rule_oid));
                chunk.set_value(1, 0, lv_str(res, name));
                chunk.set_value(2, 0, lv_oid(res, oid));
                chunk.set_value(3, 0, lv_str(res, ev_type_str));
                chunk.set_value(4, 0, lv_str(res, body));
            });
            co_await append_pg_catalog_row(ctx, pg_rewrite_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), oid, invalidation_kind::macro_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_macro(execution_context_t ctx,
                                    components::catalog::oid_t macro_oid,
                                    drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_macro : {} (behavior={})", macro_oid, static_cast<int>(behavior));
        // Remove pg_rewrite body row (ev_class col 2) before pg_class/pg_depend cleanup.
        co_await delete_pg_catalog_rows(ctx, pg_rewrite_name, /*ev_class col*/ 2, macro_oid);
        co_return co_await ddl_drop_table(ctx, macro_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_index(execution_context_t ctx,
                                      components::catalog::oid_t namespace_oid,
                                      components::catalog::oid_t table_oid,
                                      std::string index_name,
                                      std::vector<std::string> column_names) {
        const auto index_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_index : {} on {} ({} cols) -> oid {}",
              index_name, table_oid, column_names.size(), index_oid);
        if (auto* def = components::catalog::find_system_table("pg_class")) {
            const std::string relkind_str(1, catalog::relkind::index);
            const std::string storagemode_str(1, catalog::relstoragemode::disk);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, index_oid));
                chunk.set_value(1, 0, lv_str(res, index_name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                chunk.set_value(3, 0, lv_str(res, relkind_str));
                chunk.set_value(4, 0, lv_str(res, storagemode_str));
            });
            co_await append_pg_catalog_row(ctx, pg_class_name, std::move(row));
        }
        if (ctx.txn.transaction_id == 0) {
            ns_table_key_t key{namespace_oid, index_name};
            table_to_oid_.emplace(key, table_index_entry_t{index_oid, catalog::relkind::index});
            table_oid_to_key_.emplace(index_oid, std::move(key));
        }
        // Resolve column_names → attoids once; used by both pg_index (indkey) and
        // pg_depend (per-column 'i' rows). Declared before both blocks.
        std::unordered_map<std::string, components::catalog::oid_t> name_to_attoid;
        if (auto pa_it = storages_.find(pg_attribute_name); pa_it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(pa_it->second->table_storage.table(), {0, 1, 2}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto attoid_v = chunk.value(0, i);
                            auto attrelid_v = chunk.value(1, i);
                            auto attname_v = chunk.value(2, i);
                            if (attoid_v.is_null() || attrelid_v.is_null() || attname_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    attrelid_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            name_to_attoid.emplace(
                                std::string(attname_v.value<std::string_view>()),
                                static_cast<components::catalog::oid_t>(
                                    attoid_v.value<std::uint32_t>()));
                            return true;
                        });
        }
        if (auto* def = components::catalog::find_system_table("pg_index")) {
            // pg_index.indkey is a CSV of pg_attribute.attoid values; name_to_attoid was
            // built above and is in scope for this block.
            std::string indkey;
            for (size_t i = 0; i < column_names.size(); i++) {
                if (i)
                    indkey += ",";
                auto it = name_to_attoid.find(column_names[i]);
                indkey += std::to_string(
                    it != name_to_attoid.end() ? it->second : components::catalog::INVALID_OID);
            }
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, index_oid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, indkey));
                // Created invalid; ddl_index_set_valid flips to true after backfill.
                chunk.set_value(3, 0, lv_bool(res, false));
            });
            co_await append_pg_catalog_row(ctx, pg_index_name, std::move(row));
        }
        // Index→table 'a' auto-cascade dependency: DROP TABLE drops the index.
        // Also write per-column 'i' (internal) rows so DROP COLUMN can detect index dependency.
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(1, 0, lv_oid(res, index_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(3, 0, lv_oid(res, table_oid));
                chunk.set_value(4, 0, lv_str(res, "a"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));

            // Per-column 'i' deps: index→each indexed column (objsubid = 1-based position).
            for (std::int32_t col_pos = 1; col_pos <= static_cast<std::int32_t>(column_names.size()); ++col_pos) {
                auto it = name_to_attoid.find(column_names[static_cast<std::size_t>(col_pos - 1)]);
                if (it == name_to_attoid.end()) continue;
                const auto col_attoid = it->second;
                auto col_row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                    chunk.set_value(1, 0, lv_oid(res, index_oid));
                    chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                    chunk.set_value(3, 0, lv_oid(res, col_attoid));
                    chunk.set_value(4, 0, lv_str(res, "i"));
                    chunk.set_value(5, 0, lv_i32(res, col_pos));
                });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(col_row));
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), index_oid, invalidation_kind::index_added,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_index(execution_context_t ctx,
                                    components::catalog::oid_t index_oid,
                                    drop_behavior_t behavior) {
        trace(log_, "manager_disk_t::ddl_drop_index : {} (behavior={})", index_oid, static_cast<int>(behavior));
        // Reuse pg_class teardown — pg_index/pg_depend cleanup needs an explicit sweep
        // because ddl_drop_table only knows pg_class/pg_attribute.
        co_await delete_pg_catalog_rows(ctx, pg_index_name, /*indexrelid*/ 0, index_oid);
        co_return co_await ddl_drop_table(ctx, index_oid, behavior);
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_type(execution_context_t ctx,
                                     components::catalog::oid_t namespace_oid,
                                     std::string type_name,
                                     std::string type_spec) {
        const auto type_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_type : {} -> oid {}", type_name, type_oid);
        if (auto* def = components::catalog::find_system_table("pg_type")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, type_oid));
                chunk.set_value(1, 0, lv_str(res, type_name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                if (!type_spec.empty()) {
                    chunk.set_value(3, 0, lv_str(res, type_spec));
                }
            });
            co_await append_pg_catalog_row(ctx, pg_type_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_type_table));
                chunk.set_value(1, 0, lv_oid(res, type_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), type_oid, invalidation_kind::type_added,
                                                 namespace_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_type(execution_context_t ctx,
                                   components::catalog::oid_t type_oid,
                                   drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_type : {} (behavior={})", type_oid, static_cast<int>(behavior));
        // RESTRICT: refuse if any column depends on this type via pg_depend(refclassid=pg_type, refobjid=type_oid).
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_type_table, type_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE: pre-validate dependency graph for cycles (defense in depth — ddl_drop_type
        // is non-recursive today but parity with ddl_drop_namespace/table guards against
        // future refactors that introduce recursive cascade).
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_type_table,
                type_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        // CASCADE: walk dependents (typically pg_attribute rows; column-level cascade is
        // outside scope — we only sweep pg_type itself).
        co_await delete_pg_catalog_rows(ctx, pg_type_name, /*oid*/ 0, type_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, type_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, type_oid);
        co_return finalize_ddl(make_ddl_result(resource(), type_oid, invalidation_kind::type_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_function(execution_context_t ctx,
                                         components::catalog::oid_t namespace_oid,
                                         std::string function_name,
                                         std::int32_t pronargs,
                                         std::int64_t prouid,
                                         std::string proargmatchers,
                                         std::string prorettype) {
        const auto fn_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_function : {} -> oid {}", function_name, fn_oid);
        if (auto* def = components::catalog::find_system_table("pg_proc")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, fn_oid));
                chunk.set_value(1, 0, lv_str(res, function_name));
                chunk.set_value(2, 0, lv_oid(res, namespace_oid));
                // pronargs col=3, prouid col=4, proargmatchers col=5, prorettype col=6.
                chunk.set_value(3, 0, lv_i32(res, pronargs));
                chunk.set_value(4, 0, lv_i64(res, prouid));
                chunk.set_value(5, 0, lv_str(res, proargmatchers));
                chunk.set_value(6, 0, lv_str(res, prorettype));
            });
            co_await append_pg_catalog_row(ctx, pg_proc_name, std::move(row));
        }
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_proc_table));
                chunk.set_value(1, 0, lv_oid(res, fn_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_namespace_table));
                chunk.set_value(3, 0, lv_oid(res, namespace_oid));
                chunk.set_value(4, 0, lv_str(res, "n"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), fn_oid, invalidation_kind::function_added,
                                                 namespace_oid, version));
    }

    // FK enforcement for executor INSERT path.
    // Full composite FK enforcement with MATCH SIMPLE/FULL/PARTIAL semantics.
    // SQL semantics: NULL FK column → row passes (matches PostgreSQL MATCH SIMPLE default).
    manager_disk_t::unique_future<std::optional<std::string>>
    manager_disk_t::fk_validate_insert(execution_context_t /*ctx*/,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> chunk) {
        if (!chunk || chunk->size() == 0) {
            co_return std::nullopt;
        }

        // 1) Resolve table_oid from collection name via pg_class scan.
        components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t ns_oid_for_table = components::catalog::INVALID_OID;
        {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) {
                co_return std::nullopt;
            }
            std::pmr::synchronized_pool_resource sr;
            std::unordered_map<std::string, components::catalog::oid_t> ns_name_to_oid;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            if (oid_v.is_null() || nm_v.is_null()) return true;
                            ns_name_to_oid.emplace(std::string(nm_v.value<std::string_view>()),
                                                     static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()));
                            return true;
                        });
            auto ns_lookup = ns_name_to_oid.find(name.database);
            if (ns_lookup == ns_name_to_oid.end()) {
                co_return std::nullopt;
            }
            ns_oid_for_table = ns_lookup->second;
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (oid_v.is_null() || nm_v.is_null() || ns_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != ns_oid_for_table)
                                return true;
                            if (nm_v.value<std::string_view>() != std::string_view(name.collection)) return true;
                            table_oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            return false;
                        });
            if (table_oid == components::catalog::INVALID_OID) {
                co_return std::nullopt;
            }
        }

        // 2) Fetch FKs whose conrelid == table_oid. Bail fast if none.
        auto fks = fk_constraints_for_table(table_oid);
        if (fks.empty()) {
            co_return std::nullopt;
        }

        // Helper: parse CSV of integers into vector<attoid>.
        auto parse_csv = [](const std::string& s) -> std::vector<components::catalog::oid_t> {
            std::vector<components::catalog::oid_t> out;
            std::size_t i = 0;
            while (i < s.size()) {
                std::size_t j = s.find(',', i);
                std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
                if (!tok.empty()) {
                    try { out.push_back(static_cast<components::catalog::oid_t>(std::stoul(std::string(tok)))); }
                    catch (...) { /* malformed → skip */ }
                }
                if (j == std::string::npos) break;
                i = j + 1;
            }
            return out;
        };

        // Helper: build (attoid → chunk_column_index) for table_oid by scanning pg_attribute
        // sorted by attnum (1-based ordinal). Column N in the chunk corresponds to the Nth
        // non-dropped attribute by attnum. Returns empty map on failure.
        auto chunk_index_by_attoid = [&](components::catalog::oid_t rel_oid) {
            std::unordered_map<components::catalog::oid_t, uint64_t> map;
            auto pa_it = storages_.find(pg_attribute_name);
            if (pa_it == storages_.end()) return map;
            std::pmr::synchronized_pool_resource sr;
            // Collect (attnum, attoid) pairs for rel_oid where attisdropped=false.
            std::vector<std::pair<int32_t, components::catalog::oid_t>> attrs;
            // pg_attribute cols: 0=attoid, 1=attrelid, 2=attname, 3=atttypid, 4=attnum, 5=attnotnull, 6=atthasdefault, 7=attisdropped
            inline_scan(pa_it->second->table_storage.table(), {0, 1, 4, 7}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto rel_v = c.value(1, i);
                            if (rel_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != rel_oid)
                                return true;
                            auto dropped_v = c.value(3, i);
                            if (!dropped_v.is_null() && dropped_v.value<bool>()) return true;
                            auto attoid_v = c.value(0, i);
                            auto num_v = c.value(2, i);
                            if (attoid_v.is_null() || num_v.is_null()) return true;
                            attrs.emplace_back(num_v.value<std::int32_t>(),
                                                 static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>()));
                            return true;
                        });
            std::sort(attrs.begin(), attrs.end(),
                       [](const auto& a, const auto& b) { return a.first < b.first; });
            for (uint64_t i = 0; i < attrs.size(); ++i) {
                map.emplace(attrs[i].second, i);
            }
            return map;
        };

        // Helper: resolve (ref_table_oid → collection_full_name_t) so we can scan the parent.
        auto resolve_table_name = [&](components::catalog::oid_t rel_oid) -> std::optional<collection_full_name_t> {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) return std::nullopt;
            std::pmr::synchronized_pool_resource sr;
            collection_full_name_t out;
            components::catalog::oid_t parent_ns_oid = components::catalog::INVALID_OID;
            std::string parent_relname;
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            if (oid_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != rel_oid)
                                return true;
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (nm_v.is_null() || ns_v.is_null()) return true;
                            parent_ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                            parent_relname = std::string(nm_v.value<std::string_view>());
                            return false;
                        });
            if (parent_ns_oid == components::catalog::INVALID_OID) return std::nullopt;
            std::string parent_ns_name;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            if (oid_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != parent_ns_oid)
                                return true;
                            auto nm_v = c.value(1, i);
                            if (nm_v.is_null()) return true;
                            parent_ns_name = std::string(nm_v.value<std::string_view>());
                            return false;
                        });
            if (parent_ns_name.empty()) return std::nullopt;
            out.database = parent_ns_name;
            out.collection = parent_relname;
            return out;
        };

        // 3) For each FK constraint, validate existence. Supports both single-column and
        //    multi-column (composite) keys with MATCH SIMPLE NULL semantics: any NULL FK
        //    column in the row → row passes (skipped). Composite key check is one parent
        //    scan per row with a tuple-equality predicate across all confkey columns.
        auto child_attoid_map = chunk_index_by_attoid(table_oid);
        for (const auto& fk : fks) {
            auto conkey = parse_csv(fk.conkey);
            auto confkey = parse_csv(fk.confkey);
            if (conkey.empty() || conkey.size() != confkey.size()) {
                continue; // malformed pg_constraint row — skip silently
            }

            // Map child conkey → chunk column indices.
            std::vector<uint64_t> child_cols;
            child_cols.reserve(conkey.size());
            bool child_mapping_ok = true;
            for (auto attoid : conkey) {
                auto it = child_attoid_map.find(attoid);
                if (it == child_attoid_map.end() || it->second >= chunk->column_count()) {
                    child_mapping_ok = false;
                    break;
                }
                child_cols.push_back(it->second);
            }
            if (!child_mapping_ok) continue;

            auto parent_name_opt = resolve_table_name(fk.ref_table_oid);
            if (!parent_name_opt) continue;
            auto parent_it = storages_.find(*parent_name_opt);
            if (parent_it == storages_.end()) continue;

            // Map parent confkey → parent storage column indices.
            auto parent_attoid_map = chunk_index_by_attoid(fk.ref_table_oid);
            std::vector<uint64_t> parent_cols;
            parent_cols.reserve(confkey.size());
            bool parent_mapping_ok = true;
            for (auto attoid : confkey) {
                auto it = parent_attoid_map.find(attoid);
                if (it == parent_attoid_map.end()) {
                    parent_mapping_ok = false;
                    break;
                }
                parent_cols.push_back(it->second);
            }
            if (!parent_mapping_ok) continue;

            // Build the int64 list of parent columns for inline_scan, in conkey/confkey order.
            std::vector<std::int64_t> parent_scan_cols;
            parent_scan_cols.reserve(parent_cols.size());
            for (auto c : parent_cols) parent_scan_cols.emplace_back(static_cast<std::int64_t>(c));

            for (uint64_t r = 0; r < chunk->size(); ++r) {
                // Build the FK tuple for this row.
                //   MATCH SIMPLE ('s', default): any NULL component → row passes (skipped).
                //   MATCH FULL ('f'): all non-NULL → check; all-NULL → pass; partial NULL → reject.
                //   MATCH PARTIAL ('p'): all-NULL → pass; otherwise require parent row matching
                //     every non-NULL FK component (NULL components are wildcards on parent side).
                std::vector<components::types::logical_value_t> fk_tuple;
                fk_tuple.reserve(child_cols.size());
                std::size_t null_count = 0;
                for (auto cc : child_cols) {
                    auto v = chunk->data[cc].value(r);
                    if (v.is_null()) ++null_count;
                    fk_tuple.push_back(std::move(v));
                }
                const bool all_null = (null_count == fk_tuple.size());
                if (fk.matchtype == catalog::fk_match::full) {
                    if (all_null) continue;
                    if (null_count > 0) {
                        std::string msg = "INSERT into " + name.database + "." + name.collection +
                                           " violates FK MATCH FULL (oid=" +
                                           std::to_string(fk.constraint_oid) +
                                           ") — partial NULL in FK columns";
                        co_return std::optional<std::string>(std::move(msg));
                    }
                } else if (fk.matchtype == catalog::fk_match::partial) {
                    if (all_null) continue;
                    // Partial-NULL OK: matched parent must agree on non-NULL components only.
                } else {
                    if (null_count > 0) continue; // SIMPLE: any NULL → row passes
                }

                bool found = false;
                std::pmr::synchronized_pool_resource scan_resource;
                const bool match_partial = (fk.matchtype == catalog::fk_match::partial);
                inline_scan(parent_it->second->table_storage.table(),
                             parent_scan_cols, &scan_resource,
                             [&](components::vector::data_chunk_t& c, uint64_t i) {
                                 // Tuple-equal across all parent FK columns. Indices in chunk
                                 // match parent_scan_cols ordering — column k = k-th FK component.
                                 // MATCH PARTIAL: skip predicate for NULL FK components (those
                                 // act as wildcards on the parent side per SQL spec).
                                 for (uint64_t k = 0; k < fk_tuple.size(); ++k) {
                                     if (match_partial && fk_tuple[k].is_null()) {
                                         continue;
                                     }
                                     auto pv = c.value(k, i);
                                     if (pv.is_null() || !(pv == fk_tuple[k])) {
                                         return true;
                                     }
                                 }
                                 found = true;
                                 return false;
                             });
                if (!found) {
                    std::string msg = "INSERT into " + name.database + "." + name.collection +
                                       " violates FK constraint (oid=" +
                                       std::to_string(fk.constraint_oid) +
                                       ") — value not present in " + parent_name_opt->database +
                                       "." + parent_name_opt->collection;
                    co_return std::optional<std::string>(std::move(msg));
                }
            }
        }

        co_return std::nullopt;
    }

    // UPDATE FK enforcement — delegate to INSERT path. Same SQL semantics: post-write
    // FK column values must reference an existing parent row. If we ever need to handle
    // RI_RESTRICT vs RI_NO_ACTION differences on the updated key columns, this is the
    // place to fork.
    manager_disk_t::unique_future<std::optional<std::string>>
    manager_disk_t::fk_validate_update(execution_context_t ctx,
                                         collection_full_name_t name,
                                         std::unique_ptr<components::vector::data_chunk_t> chunk) {
        // Direct call (we are already inside the disk-actor coroutine) avoids round-trip
        // through the mailbox; fk_validate_insert is a standalone coroutine that doesn't
        // depend on actor message context.
        co_return co_await fk_validate_insert(ctx, std::move(name), std::move(chunk));
    }

    // CHECK constraint lookup — resolve table_oid from collection name, then return all
    // CHECK constraints for that table. Used by executor to retrieve conexpr strings
    // before CHECK validation at INSERT/UPDATE time.
    manager_disk_t::unique_future<std::vector<check_constraint_info_t>>
    manager_disk_t::get_check_constraints(execution_context_t /*ctx*/,
                                           collection_full_name_t name) {
        components::catalog::oid_t table_oid = components::catalog::INVALID_OID;
        {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) {
                co_return std::vector<check_constraint_info_t>{};
            }
            std::pmr::synchronized_pool_resource sr;
            components::catalog::oid_t ns_oid = components::catalog::INVALID_OID;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            if (oid_v.is_null() || nm_v.is_null()) return true;
                            if (nm_v.value<std::string_view>() == std::string_view(name.database)) {
                                ns_oid = static_cast<components::catalog::oid_t>(
                                    oid_v.value<std::uint32_t>());
                                return false;
                            }
                            return true;
                        });
            if (ns_oid == components::catalog::INVALID_OID) {
                co_return std::vector<check_constraint_info_t>{};
            }
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (oid_v.is_null() || nm_v.is_null() || ns_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != ns_oid)
                                return true;
                            if (nm_v.value<std::string_view>() != std::string_view(name.collection))
                                return true;
                            table_oid = static_cast<components::catalog::oid_t>(
                                oid_v.value<std::uint32_t>());
                            return false;
                        });
        }
        if (table_oid == components::catalog::INVALID_OID) {
            co_return std::vector<check_constraint_info_t>{};
        }
        co_return check_constraints_for_table(table_oid);
    }

    // Parent-side FK enforcement for DELETE.
    // For each constraint where confrelid == this_table_oid (i.e. some child references us),
    // scan the child for any row whose FK columns match the to-be-deleted parent rows.
    // RESTRICT only — first match aborts. CASCADE deferred.
    manager_disk_t::unique_future<std::optional<std::string>>
    manager_disk_t::fk_validate_parent_delete(execution_context_t ctx,
                                                collection_full_name_t name,
                                                std::unique_ptr<components::vector::data_chunk_t> chunk_to_delete) {
        if (!chunk_to_delete || chunk_to_delete->size() == 0) {
            co_return std::nullopt;
        }

        // 1) Resolve table_oid from name (same as fk_validate_insert).
        components::catalog::oid_t parent_oid = components::catalog::INVALID_OID;
        components::catalog::oid_t parent_ns_oid = components::catalog::INVALID_OID;
        {
            auto pg_class_it = storages_.find(pg_class_name);
            auto pg_ns_it = storages_.find(pg_namespace_name);
            if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) {
                co_return std::nullopt;
            }
            std::pmr::synchronized_pool_resource sr;
            std::unordered_map<std::string, components::catalog::oid_t> ns_name_to_oid;
            inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            if (oid_v.is_null() || nm_v.is_null()) return true;
                            ns_name_to_oid.emplace(std::string(nm_v.value<std::string_view>()),
                                                     static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()));
                            return true;
                        });
            auto ns_lookup = ns_name_to_oid.find(name.database);
            if (ns_lookup == ns_name_to_oid.end()) {
                co_return std::nullopt;
            }
            parent_ns_oid = ns_lookup->second;
            inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& c, uint64_t i) {
                            auto oid_v = c.value(0, i);
                            auto nm_v = c.value(1, i);
                            auto ns_v = c.value(2, i);
                            if (oid_v.is_null() || nm_v.is_null() || ns_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != parent_ns_oid)
                                return true;
                            if (nm_v.value<std::string_view>() != std::string_view(name.collection)) return true;
                            parent_oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            return false;
                        });
            if (parent_oid == components::catalog::INVALID_OID) {
                co_return std::nullopt;
            }
        }

        // 2) Find FKs that point AT this table.
        auto fks_in = fk_constraints_referencing(parent_oid);
        if (fks_in.empty()) {
            co_return std::nullopt;
        }

        // 3) For each FK, find the child relation (conrelid). fk_constraints_referencing
        //    doesn't carry conrelid, so we re-scan pg_constraint by constraint_oid.
        auto pg_constraint_it = storages_.find(pg_constraint_name);
        if (pg_constraint_it == storages_.end()) {
            co_return std::nullopt;
        }

        for (const auto& fk : fks_in) {
            // Lookup conrelid for this constraint via pg_constraint scan.
            // pg_constraint cols: 0=oid, 1=conrelid, 2=contype, 3=confrelid, 4=conkey, 5=confkey, ...
            components::catalog::oid_t child_table_oid = components::catalog::INVALID_OID;
            {
                std::pmr::synchronized_pool_resource sr;
                inline_scan(pg_constraint_it->second->table_storage.table(), {0, 1}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto oid_v = c.value(0, i);
                                if (oid_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != fk.constraint_oid)
                                    return true;
                                auto rel_v = c.value(1, i);
                                if (!rel_v.is_null()) {
                                    child_table_oid = static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>());
                                }
                                return false;
                            });
            }
            if (child_table_oid == components::catalog::INVALID_OID) continue;

            // Parse conkey/confkey CSV — supports composite (multi-column) keys.
            std::vector<components::catalog::oid_t> conkey;
            std::vector<components::catalog::oid_t> confkey;
            auto parse_csv = [](const std::string& s, std::vector<components::catalog::oid_t>& out) {
                std::size_t i = 0;
                while (i < s.size()) {
                    std::size_t j = s.find(',', i);
                    std::string_view tok(s.data() + i, (j == std::string::npos ? s.size() : j) - i);
                    if (!tok.empty()) {
                        try { out.push_back(static_cast<components::catalog::oid_t>(std::stoul(std::string(tok)))); }
                        catch (...) {}
                    }
                    if (j == std::string::npos) break;
                    i = j + 1;
                }
            };
            parse_csv(fk.conkey, conkey);
            parse_csv(fk.confkey, confkey);
            if (conkey.empty() || conkey.size() != confkey.size()) continue;

            // Build attoid → chunk-column-index for both child and parent.
            auto build_attoid_map = [&](components::catalog::oid_t rel_oid) {
                std::unordered_map<components::catalog::oid_t, uint64_t> map;
                auto pa_it = storages_.find(pg_attribute_name);
                if (pa_it == storages_.end()) return map;
                std::pmr::synchronized_pool_resource sr;
                std::vector<std::pair<int32_t, components::catalog::oid_t>> attrs;
                inline_scan(pa_it->second->table_storage.table(), {0, 1, 4, 7}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto rel_v = c.value(1, i);
                                if (rel_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != rel_oid)
                                    return true;
                                auto dropped_v = c.value(3, i);
                                if (!dropped_v.is_null() && dropped_v.value<bool>()) return true;
                                auto attoid_v = c.value(0, i);
                                auto num_v = c.value(2, i);
                                if (attoid_v.is_null() || num_v.is_null()) return true;
                                attrs.emplace_back(num_v.value<std::int32_t>(),
                                                     static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>()));
                                return true;
                            });
                std::sort(attrs.begin(), attrs.end(),
                           [](const auto& a, const auto& b) { return a.first < b.first; });
                for (uint64_t i = 0; i < attrs.size(); ++i) map.emplace(attrs[i].second, i);
                return map;
            };

            auto parent_map = build_attoid_map(parent_oid);
            auto child_map = build_attoid_map(child_table_oid);
            // Map all confkey/conkey attoids to their respective storage column indices.
            std::vector<uint64_t> parent_cols;
            std::vector<uint64_t> child_cols;
            parent_cols.reserve(confkey.size());
            child_cols.reserve(conkey.size());
            bool mapping_ok = true;
            for (auto attoid : confkey) {
                auto it_idx = parent_map.find(attoid);
                if (it_idx == parent_map.end() || it_idx->second >= chunk_to_delete->column_count()) {
                    mapping_ok = false;
                    break;
                }
                parent_cols.push_back(it_idx->second);
            }
            if (!mapping_ok) continue;
            for (auto attoid : conkey) {
                auto it_idx = child_map.find(attoid);
                if (it_idx == child_map.end()) {
                    mapping_ok = false;
                    break;
                }
                child_cols.push_back(it_idx->second);
            }
            if (!mapping_ok) continue;

            // Resolve child collection name from child_table_oid.
            collection_full_name_t child_name;
            {
                auto pg_class_it = storages_.find(pg_class_name);
                auto pg_ns_it = storages_.find(pg_namespace_name);
                if (pg_class_it == storages_.end() || pg_ns_it == storages_.end()) continue;
                std::pmr::synchronized_pool_resource sr;
                components::catalog::oid_t child_ns_oid = components::catalog::INVALID_OID;
                std::string child_relname;
                inline_scan(pg_class_it->second->table_storage.table(), {0, 1, 2}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto oid_v = c.value(0, i);
                                if (oid_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != child_table_oid)
                                    return true;
                                auto nm_v = c.value(1, i);
                                auto ns_v = c.value(2, i);
                                if (nm_v.is_null() || ns_v.is_null()) return true;
                                child_ns_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                                child_relname = std::string(nm_v.value<std::string_view>());
                                return false;
                            });
                if (child_ns_oid == components::catalog::INVALID_OID) continue;
                std::string child_ns_name;
                inline_scan(pg_ns_it->second->table_storage.table(), {0, 1}, &sr,
                            [&](components::vector::data_chunk_t& c, uint64_t i) {
                                auto oid_v = c.value(0, i);
                                if (oid_v.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != child_ns_oid)
                                    return true;
                                auto nm_v = c.value(1, i);
                                if (nm_v.is_null()) return true;
                                child_ns_name = std::string(nm_v.value<std::string_view>());
                                return false;
                            });
                if (child_ns_name.empty()) continue;
                child_name.database = child_ns_name;
                child_name.collection = child_relname;
            }
            auto child_storage_it = storages_.find(child_name);
            if (child_storage_it == storages_.end()) continue;

            // For each row being deleted: build the parent PK tuple, scan child for any
            // row whose conkey columns tuple-match. MATCH SIMPLE: any NULL parent PK
            // component → skip row (no FK can reference NULL anyway).
            std::vector<std::int64_t> child_scan_cols;
            child_scan_cols.reserve(child_cols.size());
            for (auto c : child_cols) child_scan_cols.emplace_back(static_cast<std::int64_t>(c));

            // Branch on deltype:
            //   'a' (NO ACTION, default) / 'r' (RESTRICT): first match → reject DELETE.
            //   'c' (CASCADE): collect every child row matching any parent PK and direct-delete.
            //   'n' (SET NULL): collect every match and direct_update_sync the FK columns to NULL.
            //   'd' (SET DEFAULT): deferred — defaults need pg_attribute.attdefspec resolution.
            // Single-level CASCADE only: if child has its own FK-referencing grandchildren,
            // deletion does NOT propagate further. Chain CASCADE (#141) requires fetching the
            // deleted-child chunk before direct_delete_sync, recursively calling
            // fk_validate_parent_delete on the child collection with that chunk, plus depth-
            // capped cycle detection (visited (table_oid, conkey) set). Rare in practice.
            const bool cascade = (fk.deltype == catalog::fk_action::cascade);
            const bool set_null = (fk.deltype == catalog::fk_action::set_null);
            const bool collecting = cascade || set_null;
            std::pmr::vector<std::int64_t> action_rows(resource());
            for (uint64_t r = 0; r < chunk_to_delete->size(); ++r) {
                std::vector<components::types::logical_value_t> pk_tuple;
                pk_tuple.reserve(parent_cols.size());
                bool any_null = false;
                for (auto pc : parent_cols) {
                    auto v = chunk_to_delete->data[pc].value(r);
                    if (v.is_null()) { any_null = true; break; }
                    pk_tuple.push_back(std::move(v));
                }
                if (any_null) continue;

                bool found = false;
                std::pmr::synchronized_pool_resource scan_resource;
                inline_scan(child_storage_it->second->table_storage.table(),
                             child_scan_cols, &scan_resource,
                             [&](components::vector::data_chunk_t& c, uint64_t i) {
                                 for (uint64_t k = 0; k < pk_tuple.size(); ++k) {
                                     auto cv = c.value(k, i);
                                     if (cv.is_null() || !(cv == pk_tuple[k])) {
                                         return true;
                                     }
                                 }
                                 found = true;
                                 if (collecting) {
                                     action_rows.push_back(c.row_ids.data<std::int64_t>()[i]);
                                     return true; // keep scanning
                                 }
                                 return false; // RESTRICT: stop at first match
                             });
                if (found && !collecting) {
                    std::string msg = "DELETE from " + name.database + "." + name.collection +
                                       " violates FK RESTRICT (oid=" +
                                       std::to_string(fk.constraint_oid) +
                                       ") — referenced by " + child_name.database + "." +
                                       child_name.collection;
                    co_return std::optional<std::string>(std::move(msg));
                }
            }
            if (cascade && !action_rows.empty()) {
                trace(log_, "fk_validate_parent_delete: CASCADE deleting {} rows from {}.{} via fk oid={}",
                      action_rows.size(), child_name.database, child_name.collection, fk.constraint_oid);
                direct_delete_sync(child_name, action_rows,
                                    static_cast<std::uint64_t>(action_rows.size()),
                                    ctx.txn);
            } else if (set_null && !action_rows.empty()) {
                // Build attoid → column name map for the child relation. We need names because
                // direct_update_sync matches chunk columns to table columns via type alias.
                std::unordered_map<components::catalog::oid_t, std::string> child_attoid_to_name;
                {
                    auto pa_it = storages_.find(pg_attribute_name);
                    if (pa_it != storages_.end()) {
                        std::pmr::synchronized_pool_resource sr;
                        // pg_attribute cols: 0=attoid, 1=attrelid, 2=attname, 7=attisdropped.
                        inline_scan(pa_it->second->table_storage.table(), {0, 1, 2, 7}, &sr,
                                    [&](components::vector::data_chunk_t& c, uint64_t i) {
                                        auto rel_v = c.value(1, i);
                                        if (rel_v.is_null()) return true;
                                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != child_table_oid)
                                            return true;
                                        auto dropped_v = c.value(3, i);
                                        if (!dropped_v.is_null() && dropped_v.value<bool>()) return true;
                                        auto attoid_v = c.value(0, i);
                                        auto name_v = c.value(2, i);
                                        if (attoid_v.is_null() || name_v.is_null()) return true;
                                        child_attoid_to_name.emplace(
                                            static_cast<components::catalog::oid_t>(attoid_v.value<std::uint32_t>()),
                                            std::string(name_v.value<std::string_view>()));
                                        return true;
                                    });
                    }
                }
                std::vector<std::string> fk_names;
                fk_names.reserve(conkey.size());
                bool names_ok = true;
                for (auto attoid : conkey) {
                    auto it = child_attoid_to_name.find(attoid);
                    if (it == child_attoid_to_name.end()) { names_ok = false; break; }
                    fk_names.push_back(it->second);
                }
                if (!names_ok) continue;
                // Build update chunk: one column per FK attoid, aliased with the FK column
                // name, type matched to child storage. Values stay default-constructed (NULL).
                const auto& child_columns = child_storage_it->second->table_storage.table().columns();
                std::pmr::vector<components::types::complex_logical_type> fk_types(resource());
                for (uint64_t k = 0; k < conkey.size(); ++k) {
                    auto child_idx = child_cols[k];
                    if (child_idx >= child_columns.size()) { names_ok = false; break; }
                    auto t = child_columns[child_idx].type();
                    t.set_alias(fk_names[k]);
                    fk_types.push_back(std::move(t));
                }
                if (!names_ok) continue;
                components::vector::data_chunk_t update_chunk(resource(), fk_types,
                                                                 action_rows.size());
                update_chunk.set_cardinality(action_rows.size());
                // Default-constructed cells are NULL — no further population needed.
                trace(log_, "fk_validate_parent_delete: SET NULL on {} rows in {}.{} via fk oid={}",
                      action_rows.size(), child_name.database, child_name.collection, fk.constraint_oid);
                direct_update_sync(child_name, action_rows, update_chunk);
            }
        }
        co_return std::nullopt;
    }

    std::vector<manager_disk_t::fk_constraint_info_t>
    manager_disk_t::fk_constraints_for_table(components::catalog::oid_t table_oid) {
        std::vector<fk_constraint_info_t> out;
        auto it = storages_.find(pg_constraint_name);
        if (it == storages_.end()) return out;
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_constraint cols: 0=oid 1=conname 2=conrelid 3=contype 4=confrelid
        // 5=conkey 6=confkey 7=confmatchtype 8=confdeltype 9=confupdtype
        inline_scan(it->second->table_storage.table(), {0, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_v = chunk.value(1, i);
                        auto contype_v = chunk.value(2, i);
                        if (rel_v.is_null() || contype_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                            return true;
                        auto contype_s = contype_v.value<std::string_view>();
                        if (contype_s.empty() || contype_s.front() != catalog::contype::foreign_key) return true;
                        fk_constraint_info_t info;
                        info.constraint_oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        auto ref_v = chunk.value(3, i);
                        info.ref_table_oid = ref_v.is_null() ? components::catalog::INVALID_OID
                                                              : static_cast<components::catalog::oid_t>(ref_v.value<std::uint32_t>());
                        auto ck_v = chunk.value(4, i);
                        if (!ck_v.is_null()) info.conkey = std::string(ck_v.value<std::string_view>());
                        auto cfk_v = chunk.value(5, i);
                        if (!cfk_v.is_null()) info.confkey = std::string(cfk_v.value<std::string_view>());
                        auto mt_v = chunk.value(6, i);
                        if (!mt_v.is_null()) {
                            auto sv = mt_v.value<std::string_view>();
                            if (!sv.empty()) info.matchtype = sv.front();
                        }
                        auto dt_v = chunk.value(7, i);
                        if (!dt_v.is_null()) {
                            auto sv = dt_v.value<std::string_view>();
                            if (!sv.empty()) info.deltype = sv.front();
                        }
                        auto ut_v = chunk.value(8, i);
                        if (!ut_v.is_null()) {
                            auto sv = ut_v.value<std::string_view>();
                            if (!sv.empty()) info.updtype = sv.front();
                        }
                        out.push_back(std::move(info));
                        return true;
                    });
        return out;
    }

    std::vector<manager_disk_t::fk_constraint_info_t>
    manager_disk_t::fk_constraints_referencing(components::catalog::oid_t ref_table_oid) {
        std::vector<fk_constraint_info_t> out;
        auto it = storages_.find(pg_constraint_name);
        if (it == storages_.end()) return out;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(it->second->table_storage.table(), {0, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto contype_v = chunk.value(2, i);
                        auto ref_v = chunk.value(3, i);
                        if (contype_v.is_null() || ref_v.is_null()) return true;
                        auto contype_s = contype_v.value<std::string_view>();
                        if (contype_s.empty() || contype_s.front() != catalog::contype::foreign_key) return true;
                        if (static_cast<components::catalog::oid_t>(ref_v.value<std::uint32_t>()) != ref_table_oid)
                            return true;
                        fk_constraint_info_t info;
                        info.constraint_oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        info.ref_table_oid = ref_table_oid;
                        auto ck_v = chunk.value(4, i);
                        if (!ck_v.is_null()) info.conkey = std::string(ck_v.value<std::string_view>());
                        auto cfk_v = chunk.value(5, i);
                        if (!cfk_v.is_null()) info.confkey = std::string(cfk_v.value<std::string_view>());
                        auto mt_v = chunk.value(6, i);
                        if (!mt_v.is_null()) {
                            auto sv = mt_v.value<std::string_view>();
                            if (!sv.empty()) info.matchtype = sv.front();
                        }
                        auto dt_v = chunk.value(7, i);
                        if (!dt_v.is_null()) {
                            auto sv = dt_v.value<std::string_view>();
                            if (!sv.empty()) info.deltype = sv.front();
                        }
                        auto ut_v = chunk.value(8, i);
                        if (!ut_v.is_null()) {
                            auto sv = ut_v.value<std::string_view>();
                            if (!sv.empty()) info.updtype = sv.front();
                        }
                        // conrelid lives at column index 1 of the original schema, but our
                        // projection didn't include it. Re-scan would be needed; for now
                        // callers don't need conrelid for ref-side enforcement.
                        out.push_back(std::move(info));
                        return true;
                    });
        return out;
    }

    // CHECK constraint accessor — returns all CHECK exprs for table_oid.
    std::vector<check_constraint_info_t>
    manager_disk_t::check_constraints_for_table(components::catalog::oid_t table_oid) {
        std::vector<check_constraint_info_t> out;
        auto it = storages_.find(pg_constraint_name);
        if (it == storages_.end()) return out;
        // pg_constraint cols: 0=oid, 1=conname, 2=conrelid, 3=contype, ..., 10=conexpr
        // Project: {0, 2, 3, 10} → chunk positions 0, 1, 2, 3.
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(it->second->table_storage.table(), {0, 2, 3, 10}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_v = chunk.value(1, i);
                        if (rel_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                            return true;
                        auto type_v = chunk.value(2, i);
                        if (type_v.is_null()) return true;
                        auto type_s = type_v.value<std::string_view>();
                        if (type_s.empty() || type_s.front() != catalog::contype::check) return true;
                        check_constraint_info_t info;
                        info.constraint_oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        auto expr_v = chunk.value(3, i);
                        if (!expr_v.is_null())
                            info.conexpr = std::string(expr_v.value<std::string_view>());
                        out.push_back(std::move(info));
                        return true;
                    });
        return out;
    }

    // pg_sequence parameter accessor (sync, for tests and internal use).
    std::optional<manager_disk_t::sequence_params_t>
    manager_disk_t::sequence_params_for(components::catalog::oid_t seq_oid) {
        auto it = storages_.find(pg_sequence_name);
        if (it == storages_.end()) return std::nullopt;
        std::optional<sequence_params_t> result;
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_sequence cols: 0=seqrelid 1=seqstart 2=seqincrement 3=seqmin 4=seqmax 5=seqcycle 6=seqlast
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_v = chunk.value(0, i);
                        if (rel_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != seq_oid)
                            return true;
                        sequence_params_t p;
                        auto v1 = chunk.value(1, i);
                        if (!v1.is_null()) p.seqstart = v1.value<std::int64_t>();
                        auto v2 = chunk.value(2, i);
                        if (!v2.is_null()) p.seqincrement = v2.value<std::int64_t>();
                        auto v3 = chunk.value(3, i);
                        if (!v3.is_null()) p.seqmin = v3.value<std::int64_t>();
                        auto v4 = chunk.value(4, i);
                        if (!v4.is_null()) p.seqmax = v4.value<std::int64_t>();
                        auto v5 = chunk.value(5, i);
                        if (!v5.is_null()) p.seqcycle = v5.value<bool>();
                        auto v6 = chunk.value(6, i);
                        if (!v6.is_null()) p.seqlast = v6.value<std::int64_t>();
                        result = p;
                        return false; // stop after first match
                    });
        return result;
    }

    // pg_rewrite ev_action accessor (sync, for tests and internal use).
    std::string manager_disk_t::rewrite_ev_action_for(components::catalog::oid_t relation_oid) {
        auto it = storages_.find(pg_rewrite_name);
        if (it == storages_.end()) return {};
        std::string result;
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_rewrite cols: 0=oid 1=rulename 2=ev_class 3=ev_type 4=ev_action
        inline_scan(it->second->table_storage.table(), {2, 4}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto cls_v = chunk.value(0, i);
                        if (cls_v.is_null()) return true;
                        if (static_cast<components::catalog::oid_t>(cls_v.value<std::uint32_t>()) != relation_oid)
                            return true;
                        auto act_v = chunk.value(1, i);
                        if (!act_v.is_null()) result = std::string(act_v.value<std::string_view>());
                        return false; // stop after first match
                    });
        return result;
    }

    // Constraint DDL — pg_constraint + pg_depend (constraint→table 'i' internal,
    // for FK additionally constraint→ref_table 'n' normal).

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_constraint(execution_context_t ctx,
                                            components::catalog::oid_t table_oid,
                                            std::string constraint_name,
                                            char contype,
                                            components::catalog::oid_t ref_table_oid,
                                            std::vector<components::catalog::oid_t> fk_column_attoids,
                                            std::vector<components::catalog::oid_t> ref_column_attoids,
                                            char fk_matchtype,
                                            char fk_del_action,
                                            char fk_upd_action,
                                            std::string check_expr) {
        const auto constraint_oid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_create_constraint : {} on {} type='{}' ref={} -> oid {}",
              constraint_name, table_oid, contype, ref_table_oid, constraint_oid);
        // Encode column lists as CSV of attoids — mirrors pg_index.indkey encoding.
        auto encode_oids = [](const std::vector<components::catalog::oid_t>& oids) {
            std::string out;
            for (size_t i = 0; i < oids.size(); ++i) {
                if (i) out += ',';
                out += std::to_string(oids[i]);
            }
            return out;
        };
        const std::string conkey_str = encode_oids(fk_column_attoids);
        const std::string confkey_str = encode_oids(ref_column_attoids);
        if (auto* def = components::catalog::find_system_table("pg_constraint")) {
            const std::string contype_str(1, contype);
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, constraint_oid));
                chunk.set_value(1, 0, lv_str(res, constraint_name));
                chunk.set_value(2, 0, lv_oid(res, table_oid));
                chunk.set_value(3, 0, lv_str(res, contype_str));
                chunk.set_value(4, 0, lv_oid(res, ref_table_oid));
                chunk.set_value(5, 0, lv_str(res, conkey_str));
                chunk.set_value(6, 0, lv_str(res, confkey_str));
                // Persist FK semantic flags only when this is a FOREIGN_KEY constraint.
                // Other contypes leave columns 7-9 NULL — fk_constraints_for_table sees that
                // and falls back to defaults ('s'/'a'/'a').
                if (contype == catalog::contype::foreign_key) {
                    chunk.set_value(7, 0, lv_str(res, std::string(1, fk_matchtype)));
                    chunk.set_value(8, 0, lv_str(res, std::string(1, fk_del_action)));
                    chunk.set_value(9, 0, lv_str(res, std::string(1, fk_upd_action)));
                }
                // col 10: conexpr — CHECK expr SQL text; NULL for non-CHECK constraints.
                if (contype == catalog::contype::check && !check_expr.empty()) {
                    chunk.set_value(10, 0, lv_str(res, check_expr));
                }
            });
            co_await append_pg_catalog_row(ctx, pg_constraint_name, std::move(row));
        }
        // constraint→table 'i' internal (drop cascades automatically with the table).
        if (auto* def = components::catalog::find_system_table("pg_depend")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_constraint_table));
                chunk.set_value(1, 0, lv_oid(res, constraint_oid));
                chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                chunk.set_value(3, 0, lv_oid(res, table_oid));
                chunk.set_value(4, 0, lv_str(res, "i"));
            });
            co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));

            // Per-column 'i' deps: constraint→each constrained column (objsubid = 1-based).
            for (std::int32_t col_pos = 1; col_pos <= static_cast<std::int32_t>(fk_column_attoids.size()); ++col_pos) {
                const auto col_attoid = fk_column_attoids[static_cast<std::size_t>(col_pos - 1)];
                if (col_attoid == components::catalog::INVALID_OID) continue;
                auto col_row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_constraint_table));
                    chunk.set_value(1, 0, lv_oid(res, constraint_oid));
                    chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                    chunk.set_value(3, 0, lv_oid(res, col_attoid));
                    chunk.set_value(4, 0, lv_str(res, "i"));
                    chunk.set_value(5, 0, lv_i32(res, col_pos));
                });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(col_row));
            }
        }
        // For FK: also emit constraint→ref_table 'n' normal so DROP TABLE on the referenced
        // table is blocked under RESTRICT.
        if (contype == catalog::contype::foreign_key && ref_table_oid != components::catalog::INVALID_OID) {
            if (auto* def = components::catalog::find_system_table("pg_depend")) {
                auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                    chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_constraint_table));
                    chunk.set_value(1, 0, lv_oid(res, constraint_oid));
                    chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_class_table));
                    chunk.set_value(3, 0, lv_oid(res, ref_table_oid));
                    chunk.set_value(4, 0, lv_str(res, "n"));
                });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(row));
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), constraint_oid,
                                                 invalidation_kind::constraint_added,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_constraint(execution_context_t ctx,
                                          components::catalog::oid_t constraint_oid,
                                          drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_constraint : {} (behavior={})",
              constraint_oid, static_cast<int>(behavior));
        // Constraint dependents are unusual (typically only used for indexes/triggers under
        // a constraint, none of which apply here). Honor RESTRICT for symmetry with other drops.
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_constraint_table,
                                         constraint_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        co_await delete_pg_catalog_rows(ctx, pg_constraint_name, /*oid*/ 0, constraint_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, constraint_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, constraint_oid);
        co_return finalize_ddl(make_ddl_result(resource(), constraint_oid,
                                                 invalidation_kind::constraint_dropped,
                                                 components::catalog::INVALID_OID, version));
    }

    // Column lifecycle DDL — pg_attribute mutations under MVCC.

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_add_column(execution_context_t ctx,
                                    components::catalog::oid_t table_oid,
                                    components::table::column_definition_t column) {
        const auto attoid = oid_gen_.allocate();
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_add_column : {}.{} -> attoid {}",
              table_oid, column.name(), attoid);
        // Walk pg_attribute to find max(attnum) for this table; next attnum is
        // max+1 and never reuses a dropped value, even after tombstone.
        std::int32_t next_attnum = 1;
        if (auto it = storages_.find(pg_attribute_name); it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {1, 4}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(0, i);
                            if (rel_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            auto num_v = chunk.value(1, i);
                            if (num_v.is_null())
                                return true;
                            auto n = num_v.value<std::int32_t>();
                            if (n >= next_attnum)
                                next_attnum = n + 1;
                            return true;
                        });
        }

        // Resolve atttypid via pg_type scan (same logic as ddl_create_table).
        components::catalog::oid_t atttypid = components::catalog::INVALID_OID;
        {
            std::string lookup;
            const auto lt = column.type().type();
            if (lt == types::logical_type::UNKNOWN) {
                lookup = column.type().type_name();
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

        std::string typspec = encode_type_spec(column.type());
        std::string defspec;
        if (column.has_default_value()) {
            defspec = components::catalog::encode_default_spec(column.default_value());
        }

        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            const std::string col_name{column.name()};
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, attoid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, col_name));
                chunk.set_value(3, 0, lv_oid(res, atttypid));
                chunk.set_value(4, 0, lv_i32(res, next_attnum));
                chunk.set_value(5, 0, lv_bool(res, column.is_not_null()));
                chunk.set_value(6, 0, lv_bool(res, column.has_default_value()));
                chunk.set_value(7, 0, lv_bool(res, false));
                chunk.set_value(8, 0, lv_str(res, typspec));
                chunk.set_value(9, 0, lv_str(res, defspec));
            });
            co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
        }
        // Column→type pg_depend ('n').
        if (atttypid != components::catalog::INVALID_OID) {
            if (auto* dep_def = components::catalog::find_system_table("pg_depend")) {
                auto dep_row = make_row(resource(), dep_def->columns,
                                          [&](data_chunk_t& chunk, auto* res) {
                                              chunk.set_value(0, 0, lv_oid(res, components::catalog::well_known_oid::pg_attribute_table));
                                              chunk.set_value(1, 0, lv_oid(res, attoid));
                                              chunk.set_value(2, 0, lv_oid(res, components::catalog::well_known_oid::pg_type_table));
                                              chunk.set_value(3, 0, lv_oid(res, atttypid));
                                              chunk.set_value(4, 0, lv_str(res, "n"));
                                          });
                co_await append_pg_catalog_row(ctx, pg_depend_name, std::move(dep_row));
            }
        }
        // If the user table is currently loaded in storages_, update its in-memory schema so
        // subsequent INSERTs see the new column without requiring a restart. Unloaded tables
        // are fine — they'll be constructed from pg_attribute on first access.
        {
            std::pmr::synchronized_pool_resource scan_resource;
            std::string rel_ns_name, rel_name;
            // Build namespace OID→name from pg_namespace.
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
            // Scan pg_class for this table_oid to get relname + namespace name.
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
                                return false; // found — stop scan
                            });
            }
            if (!rel_name.empty()) {
                collection_full_name_t user_key{rel_ns_name, rel_name};
                if (auto user_it = storages_.find(user_key); user_it != storages_.end()) {
                    user_it->second->add_column(column, resource());
                }
            }
        }
        co_return finalize_ddl(make_ddl_result(resource(), attoid,
                                                 invalidation_kind::relation_schema_changed,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_column(execution_context_t ctx,
                                     components::catalog::oid_t table_oid,
                                     std::string column_name,
                                     drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_column : {}.{}", table_oid, column_name);
        // Find the (attoid, attnum, typspec, defspec, atttypid, attisnull, attishasdefault)
        // for this (table_oid, column_name), then delete + re-insert with attisdropped=true.
        // Tombstone-don't-shift: attnum is preserved so subsequent ADD COLUMN won't reuse it.
        components::catalog::oid_t attoid = components::catalog::INVALID_OID;
        std::int32_t attnum = 0;
        components::catalog::oid_t atttypid = components::catalog::INVALID_OID;
        bool not_null = false;
        bool has_default = false;
        std::string typspec, defspec;
        if (auto it = storages_.find(pg_attribute_name); it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(1, i);
                            if (rel_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            auto name_v = chunk.value(2, i);
                            if (name_v.is_null() || !str_equals(name_v, column_name))
                                return true;
                            auto dropped_v = chunk.value(7, i);
                            if (!dropped_v.is_null() && dropped_v.value<bool>())
                                return true; // already a tombstone
                            attoid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            atttypid = static_cast<components::catalog::oid_t>(chunk.value(3, i).value<std::uint32_t>());
                            attnum = chunk.value(4, i).value<std::int32_t>();
                            not_null = chunk.value(5, i).value<bool>();
                            has_default = chunk.value(6, i).value<bool>();
                            auto ts_v = chunk.value(8, i);
                            if (!ts_v.is_null())
                                typspec = std::string(ts_v.value<std::string_view>());
                            auto ds_v = chunk.value(9, i);
                            if (!ds_v.is_null())
                                defspec = std::string(ds_v.value<std::string_view>());
                            return false;
                        });
        }
        if (attoid == components::catalog::INVALID_OID) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::relation_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }

        // Scan pg_depend for per-column deps (refclassid=pg_attribute, refobjid=attoid).
        // Any dependency on this column blocks RESTRICT; CASCADE drops the dependent objects.
        // pg_depend cols: 0=classid, 1=objid, 2=refclassid, 3=refobjid, 4=deptype, 5=objsubid, 6=refobjsubid
        struct col_dep_t {
            components::catalog::oid_t classid;
            components::catalog::oid_t objid;
        };
        std::vector<col_dep_t> col_deps;
        if (auto pd_it = storages_.find(pg_depend_name); pd_it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_res;
            inline_scan(pd_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_res,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rcls_v = chunk.value(2, i);
                            auto robj_v = chunk.value(3, i);
                            if (rcls_v.is_null() || robj_v.is_null()) return true;
                            if (static_cast<components::catalog::oid_t>(rcls_v.value<std::uint32_t>()) !=
                                components::catalog::well_known_oid::pg_attribute_table)
                                return true;
                            if (static_cast<components::catalog::oid_t>(robj_v.value<std::uint32_t>()) != attoid)
                                return true;
                            auto cls_v = chunk.value(0, i);
                            auto obj_v = chunk.value(1, i);
                            if (cls_v.is_null() || obj_v.is_null()) return true;
                            col_deps.push_back({
                                static_cast<components::catalog::oid_t>(cls_v.value<std::uint32_t>()),
                                static_cast<components::catalog::oid_t>(obj_v.value<std::uint32_t>())
                            });
                            return true;
                        });
        }
        if (!col_deps.empty() && behavior == drop_behavior_t::restrict_) {
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = col_deps.front().objid;
            co_return finalize_ddl(std::move(_r));
        }
        // CASCADE: drop dependent indexes and constraints before tombstoning the column.
        for (const auto& dep : col_deps) {
            if (dep.classid == components::catalog::well_known_oid::pg_class_table) {
                co_await ddl_drop_index(ctx, dep.objid, drop_behavior_t::cascade_);
            } else if (dep.classid == components::catalog::well_known_oid::pg_constraint_table) {
                co_await ddl_drop_constraint(ctx, dep.objid, drop_behavior_t::cascade_);
            }
        }

        // WAL-delete existing row, insert tombstone (attisdropped=true).
        co_await delete_pg_catalog_rows(ctx, pg_attribute_name, /*attoid_col*/ 0, attoid);
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, attoid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, column_name));
                chunk.set_value(3, 0, lv_oid(res, atttypid));
                chunk.set_value(4, 0, lv_i32(res, attnum));
                chunk.set_value(5, 0, lv_bool(res, not_null));
                chunk.set_value(6, 0, lv_bool(res, has_default));
                chunk.set_value(7, 0, lv_bool(res, true)); // attisdropped
                chunk.set_value(8, 0, lv_str(res, typspec));
                chunk.set_value(9, 0, lv_str(res, defspec));
            });
            co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), attoid,
                                                 invalidation_kind::relation_schema_changed,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_rename_column(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::string old_name,
                                       std::string new_name) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_rename_column : {}.{} -> {}",
              table_oid, old_name, new_name);
        // Find the row, delete + re-insert with new attname.
        components::catalog::oid_t attoid = components::catalog::INVALID_OID;
        std::int32_t attnum = 0;
        components::catalog::oid_t atttypid = components::catalog::INVALID_OID;
        bool not_null = false;
        bool has_default = false;
        std::string typspec, defspec;
        if (auto it = storages_.find(pg_attribute_name); it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(1, i);
                            if (rel_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel_v.value<std::uint32_t>()) != table_oid)
                                return true;
                            auto name_v = chunk.value(2, i);
                            if (name_v.is_null() || !str_equals(name_v, old_name))
                                return true;
                            auto dropped_v = chunk.value(7, i);
                            if (!dropped_v.is_null() && dropped_v.value<bool>())
                                return true;
                            attoid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            atttypid = static_cast<components::catalog::oid_t>(chunk.value(3, i).value<std::uint32_t>());
                            attnum = chunk.value(4, i).value<std::int32_t>();
                            not_null = chunk.value(5, i).value<bool>();
                            has_default = chunk.value(6, i).value<bool>();
                            auto ts_v = chunk.value(8, i);
                            if (!ts_v.is_null())
                                typspec = std::string(ts_v.value<std::string_view>());
                            auto ds_v = chunk.value(9, i);
                            if (!ds_v.is_null())
                                defspec = std::string(ds_v.value<std::string_view>());
                            return false;
                        });
        }
        if (attoid == components::catalog::INVALID_OID) {
            co_return finalize_ddl(make_ddl_result(resource(), table_oid,
                                                     invalidation_kind::relation_schema_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        co_await delete_pg_catalog_rows(ctx, pg_attribute_name, /*attoid_col*/ 0, attoid);
        if (auto* def = components::catalog::find_system_table("pg_attribute")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, attoid));
                chunk.set_value(1, 0, lv_oid(res, table_oid));
                chunk.set_value(2, 0, lv_str(res, new_name));
                chunk.set_value(3, 0, lv_oid(res, atttypid));
                chunk.set_value(4, 0, lv_i32(res, attnum));
                chunk.set_value(5, 0, lv_bool(res, not_null));
                chunk.set_value(6, 0, lv_bool(res, has_default));
                chunk.set_value(7, 0, lv_bool(res, false));
                chunk.set_value(8, 0, lv_str(res, typspec));
                chunk.set_value(9, 0, lv_str(res, defspec));
            });
            co_await append_pg_catalog_row(ctx, pg_attribute_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), attoid,
                                                 invalidation_kind::relation_schema_changed,
                                                 table_oid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_index_set_valid(execution_context_t ctx,
                                         components::catalog::oid_t index_oid,
                                         bool valid) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_index_set_valid : {} valid={}", index_oid, valid);
        // Read current pg_index row to recover indrelid + indkey, then delete + re-insert
        // with the new indisvalid flag. MVCC handles the visibility — readers under the
        // old txn snapshot still see the prior value.
        auto pg_index_it = storages_.find(pg_index_name);
        if (pg_index_it == storages_.end()) {
            co_return finalize_ddl(make_ddl_result(resource(), index_oid,
                                                     invalidation_kind::index_validity_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        components::catalog::oid_t indrelid = components::catalog::INVALID_OID;
        std::string indkey;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(pg_index_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto oid_v = chunk.value(0, i);
                        if (oid_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()) != index_oid)
                            return true;
                        auto irel_v = chunk.value(1, i);
                        auto ikey_v = chunk.value(2, i);
                        if (!irel_v.is_null())
                            indrelid = static_cast<components::catalog::oid_t>(irel_v.value<std::uint32_t>());
                        if (!ikey_v.is_null())
                            indkey = std::string(ikey_v.value<std::string_view>());
                        return false; // stop on first match
                    });
        if (indrelid == components::catalog::INVALID_OID) {
            co_return finalize_ddl(make_ddl_result(resource(), index_oid,
                                                     invalidation_kind::index_validity_changed,
                                                     components::catalog::INVALID_OID, version));
        }
        co_await delete_pg_catalog_rows(ctx, pg_index_name, /*indexrelid*/ 0, index_oid);
        if (auto* def = components::catalog::find_system_table("pg_index")) {
            auto row = make_row(resource(), def->columns, [&](data_chunk_t& chunk, auto* res) {
                chunk.set_value(0, 0, lv_oid(res, index_oid));
                chunk.set_value(1, 0, lv_oid(res, indrelid));
                chunk.set_value(2, 0, lv_str(res, indkey));
                chunk.set_value(3, 0, lv_bool(res, valid));
            });
            co_await append_pg_catalog_row(ctx, pg_index_name, std::move(row));
        }
        co_return finalize_ddl(make_ddl_result(resource(), index_oid,
                                                 invalidation_kind::index_validity_changed,
                                                 indrelid, version));
    }

    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_drop_function(execution_context_t ctx,
                                       components::catalog::oid_t function_oid,
                                       drop_behavior_t behavior) {
        const auto version = ++catalog_version_;
        trace(log_, "manager_disk_t::ddl_drop_function : {} (behavior={})", function_oid, static_cast<int>(behavior));
        auto deps = collect_dependents(components::catalog::well_known_oid::pg_proc_table, function_oid);
        if (std::any_of(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); }) && behavior == drop_behavior_t::restrict_) {
            auto _blocker = std::find_if(deps.begin(), deps.end(), [](const dependency_t& d) { return deptype::blocks_restrict(d.deptype); });
            ddl_result_t _r{resource()};
            _r.status = ddl_status::restrict_blocked;
            _r.blocking_oid = (_blocker != deps.end()) ? _blocker->objid : components::catalog::INVALID_OID;
            co_return _r;
        }
        // CASCADE cycle pre-validation — parity with ddl_drop_namespace/table.
        try {
            (void)topological_drop_order(
                components::catalog::well_known_oid::pg_proc_table,
                function_oid,
                [this](components::catalog::oid_t cls, components::catalog::oid_t oid) {
                    return collect_dependents(cls, oid);
                });
        } catch (const cycle_detected_error& _cyc) {
            ddl_result_t _cr{resource()};
            _cr.status = ddl_status::cycle_detected;
            _cr.blocking_oid = _cyc.offending_oid();
            co_return _cr;
        }
        co_await delete_pg_catalog_rows(ctx, pg_proc_name, /*oid*/ 0, function_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*objid*/ 1, function_oid);
        co_await delete_pg_catalog_rows(ctx, pg_depend_name, /*refobjid*/ 3, function_oid);
        co_return finalize_ddl(make_ddl_result(resource(), function_oid, invalidation_kind::function_dropped,
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
        co_return finalize_ddl(make_ddl_result(resource(), table_oid, invalidation_kind::computing_schema_changed,
                                                 components::catalog::INVALID_OID, version));
    }

    // ddl_create_computing_table: pg_class row only (relkind='g'), no pg_attribute, no
    // pg_computed_column rows. Subsequent ddl_computed_append / ddl_computed_drop populate
    // the field state. Equivalent to ddl_create_table with relkind='g' and no columns —
    // exposed as a separate entry point so callers don't have to remember the relkind code.
    manager_disk_t::unique_future<ddl_result_t>
    manager_disk_t::ddl_create_computing_table(execution_context_t ctx,
                                                components::catalog::oid_t namespace_oid,
                                                std::string name) {
        co_return co_await create_relation_impl(ctx, namespace_oid, std::move(name), {}, 'g');
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

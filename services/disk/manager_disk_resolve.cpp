#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // ----------------------------------------------------------------------
    // Version B* Step 8.11 Path B (catalog) — APPLIED. All 10 catalog read
    // sites in this file (pg_namespace x2, pg_class x2, pg_attribute x2,
    // pg_type, pg_proc x2, pg_computed_column) probe agents_[0] (CATALOG
    // agent) via storage_entry_sync(<pg_*_oid>) and treat a null result as
    // a terminal "no entry" outcome. The manager-map fallback that
    // appeared between Step 8.9 (Path A) and Step 8.1.B is now gone:
    // Step 8.1.B moved the catalog SFBM ownership onto the catalog agent,
    // so manager_disk_t::storages_ holds no entries for these OIDs and
    // the fallback could never fire.
    //
    // Catalog migration pattern (post-fallback removal):
    //
    //     const collection_storage_entry_t* entry = nullptr;
    //     if (!agents_.empty() && agents_[0] != nullptr) {
    //         entry = agents_[0]->storage_entry_sync(<pg_*_oid>);
    //     }
    //     if (entry != nullptr) { /* scan via entry->table_storage.table() */ }
    //
    // The two remaining `storages_.find(...)` sites are the generic-
    // table_oid primitives `scan_by_key` and `read_rows_by_key`. They
    // probe agents_[pool_idx_for_oid(table_oid, agents_.size())] first
    // and KEEP the manager-map fallback until Step 8.1.C moves user-table
    // SFBM ownership onto routed agents and deletes the manager-side
    // user-table entries.
    //
    // Decision rationale (still applies): turning the agent-probe into a
    // mailbox `co_await` round-trip would add per-resolve scheduler
    // latency to every catalog probe — measured ~1100 LOC of body that
    // would need to move to `agent_resolve.cpp`. The storage_entry_sync
    // raw-pointer borrow is the documented Constraint #11 carve-out (see
    // header comment on agent_disk_t::storage_entry_sync) — pre-scheduler
    // reads are race-free; post-scheduler reads happen from inside the
    // manager mailbox while the agent thread is idle for the agent's
    // slice.
    // ----------------------------------------------------------------------

    manager_disk_t::unique_future<resolve_namespace_result_t>
    manager_disk_t::resolve_namespace(execution_context_t /*ctx*/, std::string name, std::uint64_t /*since_version*/) {
        resolve_namespace_result_t out(resource());

        // ──────────────── Step 8.11 Path B (catalog) ──────────────────────
        //
        // Catalog OIDs always route to agents_[0] (CATALOG agent — see
        // agent_role_t::CATALOG in agent_disk.hpp). Step 8.1.B moved the
        // catalog SFBM ownership onto that agent, so manager_disk_t::
        // storages_ no longer holds entries for catalog OIDs and the prior
        // manager-map fallback is dead code — deleted here. A null
        // storage_entry_sync result is now a terminal "no entry" outcome.
        const collection_storage_entry_t* ns_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            ns_entry = agents_[0]->storage_entry_sync(pg_namespace_oid_tbl);
        }
        if (ns_entry != nullptr) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(ns_entry->table_storage.table(),
                        {0, 1},
                        &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto oid_v = chunk.value(0, i);
                            auto name_v = chunk.value(1, i);
                            if (oid_v.is_null() || name_v.is_null())
                                return true;
                            if (name_v.value<std::string_view>() != name)
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                            out.name = name;
                            return false;
                        });
        }
        co_return out;
    }

    manager_disk_t::unique_future<resolve_table_result_t>
    manager_disk_t::resolve_table(execution_context_t ctx,
                                  components::catalog::oid_t namespace_oid,
                                  std::string name,
                                  std::uint64_t /*since_version*/) {
        resolve_table_result_t out(resource());
        out.namespace_oid = namespace_oid;

        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_class; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* cls_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            cls_entry = agents_[0]->storage_entry_sync(pg_class_oid);
        }
        if (cls_entry != nullptr) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(cls_entry->table_storage.table(),
                        {0, 1, 2, 3},
                        &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns_v = chunk.value(2, i);
                            if (ns_v.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            auto name_v = chunk.value(1, i);
                            if (name_v.is_null() || name_v.value<std::string_view>() != name)
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            out.name = name;
                            auto kind_v = chunk.value(3, i);
                            if (!kind_v.is_null()) {
                                auto ks = kind_v.value<std::string_view>();
                                if (!ks.empty())
                                    out.relkind = ks.front();
                            }
                            return false;
                        });
        }

        if (!out.found) {
            co_return out;
        }

        if (out.relkind == catalog::relkind::computed) {
            // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
            // pg_computed_column; manager-map fallback removed (Step 8.1.B).
            const collection_storage_entry_t* cc_entry = nullptr;
            if (!agents_.empty() && agents_[0] != nullptr) {
                cc_entry = agents_[0]->storage_entry_sync(pg_computed_column_oid);
            }
            if (cc_entry != nullptr) {
                std::pmr::synchronized_pool_resource cc_scan_resource;
                struct cc_row_t {
                    components::catalog::oid_t attoid;
                    std::string attname;
                    components::catalog::oid_t atttypid;
                    std::int64_t attversion;
                };
                std::unordered_map<std::string, cc_row_t> latest;
                // Collect ALL rows (including tombstones with rc=0) and pick
                // max-version per attname. Then drop entries whose chosen
                // (max-version) row is a tombstone. Skipping tombstones early
                // would let a lower-version live row survive a DROP COLUMN whose
                // tombstone has version=N+1.
                // Read atttypspec (column 4) for complex types — attversion /
                // attrefcount live at columns 5 / 6.
                struct cc_row_with_rc_t {
                    cc_row_t base;
                    std::string atttypspec;
                    std::int64_t attrefcount;
                };
                std::unordered_map<std::string, cc_row_with_rc_t> latest_any;
                inline_scan(cc_entry->table_storage.table(),
                            {0, 1, 2, 3, 4, 5, 6},
                            &cc_scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto rel = chunk.value(0, i);
                                if (rel.is_null())
                                    return true;
                                if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != out.oid)
                                    return true;
                                cc_row_with_rc_t row;
                                row.base.attoid =
                                    static_cast<components::catalog::oid_t>(chunk.value(1, i).value<std::uint32_t>());
                                row.base.attname = std::string(chunk.value(2, i).value<std::string_view>());
                                row.base.atttypid = chunk.value(3, i).is_null()
                                                        ? components::catalog::INVALID_OID
                                                        : static_cast<components::catalog::oid_t>(
                                                              chunk.value(3, i).value<std::uint32_t>());
                                auto spec_v = chunk.value(4, i);
                                if (!spec_v.is_null())
                                    row.atttypspec = std::string(spec_v.value<std::string_view>());
                                row.base.attversion = chunk.value(5, i).value<std::int64_t>();
                                auto rc_v = chunk.value(6, i);
                                row.attrefcount = rc_v.is_null() ? 0 : rc_v.value<std::int64_t>();
                                auto it = latest_any.find(row.base.attname);
                                if (it == latest_any.end() || it->second.base.attversion < row.base.attversion) {
                                    latest_any[row.base.attname] = std::move(row);
                                }
                                return true;
                            });
                // Filter: column is live iff its max-version row has rc > 0.
                std::unordered_map<std::string, std::string> typspec_by_name;
                for (auto& [name, full] : latest_any) {
                    if (full.attrefcount > 0) {
                        typspec_by_name[name] = std::move(full.atttypspec);
                        latest[name] = std::move(full.base);
                    }
                }
                // Order by attoid ASC: attoid is allocated monotonically in
                // operator_computed_field_register::await_async_and_resume by iterating
                // columns_ vector in order, so attoid order == register order ==
                // storage's adopt_schema(local.types()) order. Without this sort the
                // unordered_map's bucket-order would produce indices that don't match
                // the storage chunk's column layout, breaking WHERE-filter routing
                // (filter applied to the wrong column → 0 rows).
                std::vector<cc_row_t> ordered;
                ordered.reserve(latest.size());
                for (auto& [_, row] : latest) ordered.push_back(std::move(row));
                std::sort(ordered.begin(), ordered.end(), [](const cc_row_t& a, const cc_row_t& b) {
                    return a.attoid < b.attoid;
                });
                std::int32_t synthetic_attnum = 1;
                for (auto& row : ordered) {
                    column_info_t info;
                    info.attoid = row.attoid;
                    info.attname = row.attname; // copy: typspec lookup below needs name
                    info.atttypid = row.atttypid;
                    auto ts_it = typspec_by_name.find(info.attname);
                    if (ts_it != typspec_by_name.end())
                        info.atttypspec = std::move(ts_it->second);
                    info.attnum = synthetic_attnum++;
                    info.attnotnull = false;
                    info.atthasdefault = false;
                    info.attisdropped = false;
                    out.columns.push_back(std::move(info));
                }
            }
            co_return out;
        }

        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_attribute; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* att_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            att_entry = agents_[0]->storage_entry_sync(pg_attribute_oid);
        }
        if (att_entry != nullptr) {
            std::pmr::synchronized_pool_resource scan_resource;
            std::vector<column_info_t> rows;
            // Block C §3.5 dec 32 V2 column visibility — read added_at_commit_id
            // (col 10) + dropped_at_commit_id (col 11) and filter by ctx.txn.start_time.
            const auto snapshot_start_time = ctx.txn.start_time;
            inline_scan(att_entry->table_storage.table(),
                        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
                        &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel = chunk.value(1, i);
                            if (rel.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != out.oid)
                                return true;
                            auto dropped = chunk.value(7, i);
                            const bool is_dropped = !dropped.is_null() && dropped.value<bool>();
                            if (is_dropped)
                                return true;
                            // dec 32 V2 MVCC visibility — column added after snapshot is hidden;
                            // column dropped before snapshot is hidden.
                            auto added_at_v = chunk.value(10, i);
                            if (!added_at_v.is_null()) {
                                auto added_at = static_cast<uint64_t>(added_at_v.value<std::int64_t>());
                                if (added_at > snapshot_start_time)
                                    return true;
                            }
                            auto dropped_at_v = chunk.value(11, i);
                            if (!dropped_at_v.is_null()) {
                                auto dropped_at = static_cast<uint64_t>(dropped_at_v.value<std::int64_t>());
                                if (dropped_at != 0 && dropped_at <= snapshot_start_time)
                                    return true;
                            }
                            column_info_t info;
                            info.attoid =
                                static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            auto name_v = chunk.value(2, i);
                            if (!name_v.is_null())
                                info.attname = std::string(name_v.value<std::string_view>());
                            auto typid_v = chunk.value(3, i);
                            if (!typid_v.is_null())
                                info.atttypid = static_cast<components::catalog::oid_t>(typid_v.value<std::uint32_t>());
                            info.attnum = chunk.value(4, i).value<std::int32_t>();
                            auto nn_v = chunk.value(5, i);
                            info.attnotnull = !nn_v.is_null() && nn_v.value<bool>();
                            auto def_v = chunk.value(6, i);
                            info.atthasdefault = !def_v.is_null() && def_v.value<bool>();
                            info.attisdropped = false;
                            auto typspec_v = chunk.value(8, i);
                            if (!typspec_v.is_null())
                                info.atttypspec = std::string(typspec_v.value<std::string_view>());
                            auto defspec_v = chunk.value(9, i);
                            if (!defspec_v.is_null())
                                info.attdefspec = std::string(defspec_v.value<std::string_view>());
                            rows.push_back(std::move(info));
                            return true;
                        });
            std::sort(rows.begin(), rows.end(), [](const column_info_t& a, const column_info_t& b) {
                return a.attnum < b.attnum;
            });
            out.columns = std::move(rows);
            trace(log_, "resolve_table: oid={} found {} columns", out.oid, out.columns.size());
            for (const auto& c : out.columns) {
                trace(log_,
                      "  col={} atttypid={} atttypspec='{}'",
                      c.attname,
                      static_cast<unsigned>(c.atttypid),
                      c.atttypspec);
            }
        }
        co_return out;
    }

    resolve_type_result_t manager_disk_t::resolve_type_sync(components::catalog::oid_t namespace_oid,
                                                            const std::string& name) {
        resolve_type_result_t out(resource());
        out.namespace_oid = namespace_oid;

        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_type; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* type_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            type_entry = agents_[0]->storage_entry_sync(pg_type_oid);
        }
        if (type_entry != nullptr) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(type_entry->table_storage.table(),
                        {0, 1, 2, 3},
                        &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns = chunk.value(2, i);
                            if (ns.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(ns.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            if (!str_equals(chunk.value(1, i), name))
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            out.name = name;
                            auto def_v = chunk.value(3, i);
                            if (!def_v.is_null())
                                out.typdefspec = std::string(def_v.value<std::string_view>());
                            return false;
                        });
        }
        if (out.found) {
            return out;
        }
        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_class; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* cls_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            cls_entry = agents_[0]->storage_entry_sync(pg_class_oid);
        }
        if (cls_entry == nullptr) {
            return out;
        }
        components::catalog::oid_t composite_oid = components::catalog::INVALID_OID;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(cls_entry->table_storage.table(),
                    {0, 1, 2, 3},
                    &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rns_v = chunk.value(2, i);
                        if (rns_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(rns_v.value<std::uint32_t>()) != namespace_oid)
                            return true;
                        auto kind_v = chunk.value(3, i);
                        if (kind_v.is_null())
                            return true;
                        auto kind_s = kind_v.value<std::string_view>();
                        if (kind_s.empty() || kind_s.front() != catalog::relkind::composite_type)
                            return true;
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        composite_oid =
                            static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        return false;
                    });
        if (composite_oid == components::catalog::INVALID_OID) {
            return out;
        }
        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_attribute; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* att_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            att_entry = agents_[0]->storage_entry_sync(pg_attribute_oid);
        }
        if (att_entry == nullptr) {
            return out;
        }
        struct field_row {
            std::string attname;
            components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
            std::int32_t attnum{0};
            std::string atttypspec;
        };
        std::vector<field_row> fields;
        inline_scan(att_entry->table_storage.table(),
                    {1, 2, 3, 4, 7, 8},
                    &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel = chunk.value(0, i);
                        if (rel.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != composite_oid)
                            return true;
                        auto dropped = chunk.value(4, i);
                        if (!dropped.is_null() && dropped.value<bool>())
                            return true;
                        field_row r;
                        auto name_v = chunk.value(1, i);
                        if (!name_v.is_null())
                            r.attname = std::string(name_v.value<std::string_view>());
                        auto typid_v = chunk.value(2, i);
                        if (!typid_v.is_null())
                            r.atttypid = static_cast<components::catalog::oid_t>(typid_v.value<std::uint32_t>());
                        r.attnum = chunk.value(3, i).value<std::int32_t>();
                        auto spec_v = chunk.value(5, i);
                        if (!spec_v.is_null())
                            r.atttypspec = std::string(spec_v.value<std::string_view>());
                        fields.push_back(std::move(r));
                        return true;
                    });
        std::sort(fields.begin(), fields.end(), [](const field_row& a, const field_row& b) {
            return a.attnum < b.attnum;
        });
        std::pmr::vector<components::types::complex_logical_type> child_types(resource());
        child_types.reserve(fields.size());
        for (auto& f : fields) {
            components::types::complex_logical_type ft =
                f.atttypspec.empty()
                    ? components::types::complex_logical_type{components::catalog::oid_to_builtin_type(f.atttypid)}
                    : components::catalog::decode_type_spec(resource(), f.atttypspec);
            if (ft.type() == components::types::logical_type::UNKNOWN) {
                std::string ref_name(ft.type_name());
                if (!ref_name.empty()) {
                    auto nested = resolve_type_sync(namespace_oid, ref_name);
                    if (nested.found && !nested.typdefspec.empty()) {
                        ft = components::catalog::decode_type_spec(resource(), nested.typdefspec);
                    }
                }
            }
            ft.set_alias(f.attname);
            child_types.push_back(std::move(ft));
        }
        auto struct_t = components::types::complex_logical_type::create_struct(name, child_types);
        out.found = true;
        out.oid = composite_oid;
        out.name = name;
        out.typdefspec = components::catalog::encode_type_spec(struct_t);
        return out;
    }

    manager_disk_t::unique_future<resolve_type_result_t>
    manager_disk_t::resolve_type(execution_context_t /*ctx*/,
                                 components::catalog::oid_t namespace_oid,
                                 std::string name,
                                 std::uint64_t /*since_version*/) {
        co_return resolve_type_sync(namespace_oid, name);
    }

    manager_disk_t::unique_future<resolve_function_result_t>
    manager_disk_t::resolve_function(execution_context_t /*ctx*/,
                                     components::catalog::oid_t namespace_oid,
                                     std::string name,
                                     std::uint64_t /*since_version*/) {
        resolve_function_result_t out(resource());
        out.namespace_oid = namespace_oid;

        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_proc; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* proc_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            proc_entry = agents_[0]->storage_entry_sync(pg_proc_oid);
        }
        if (proc_entry != nullptr) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(proc_entry->table_storage.table(),
                        {0, 1, 2, 3, 4, 5, 6},
                        &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns = chunk.value(2, i);
                            if (ns.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(ns.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            if (!str_equals(chunk.value(1, i), name))
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                            out.name = name;
                            auto nargs_v = chunk.value(3, i);
                            if (!nargs_v.is_null())
                                out.pronargs = nargs_v.value<std::int32_t>();
                            auto uid_v = chunk.value(4, i);
                            if (!uid_v.is_null())
                                out.prouid = uid_v.value<std::uint64_t>();
                            auto args_v = chunk.value(5, i);
                            if (!args_v.is_null())
                                out.proargmatchers = std::string(args_v.value<std::string_view>());
                            auto ret_v = chunk.value(6, i);
                            if (!ret_v.is_null())
                                out.prorettype = std::string(ret_v.value<std::string_view>());
                            return false;
                        });
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<resolve_function_result_t>>
    manager_disk_t::resolve_function_by_name(execution_context_t /*ctx*/,
                                             std::string name,
                                             std::uint64_t /*since_version*/) {
        std::pmr::vector<resolve_function_result_t> out(resource());
        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_proc; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* proc_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            proc_entry = agents_[0]->storage_entry_sync(pg_proc_oid);
        }
        if (proc_entry == nullptr) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(proc_entry->table_storage.table(),
                    {0, 1, 2, 3, 4, 5, 6},
                    &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        resolve_function_result_t r(resource());
                        r.found = true;
                        r.name = name;
                        r.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                        auto ns_v = chunk.value(2, i);
                        if (!ns_v.is_null())
                            r.namespace_oid = static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>());
                        auto nargs_v = chunk.value(3, i);
                        if (!nargs_v.is_null())
                            r.pronargs = nargs_v.value<std::int32_t>();
                        auto uid_v = chunk.value(4, i);
                        if (!uid_v.is_null())
                            r.prouid = uid_v.value<std::uint64_t>();
                        auto args_v = chunk.value(5, i);
                        if (!args_v.is_null())
                            r.proargmatchers = std::string(args_v.value<std::string_view>());
                        auto ret_v = chunk.value(6, i);
                        if (!ret_v.is_null())
                            r.prorettype = std::string(ret_v.value<std::string_view>());
                        out.push_back(std::move(r));
                        return true;
                    });
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::string>>
    manager_disk_t::list_namespaces(execution_context_t /*ctx*/) {
        std::pmr::vector<std::string> out(resource());
        // Step 8.11 Path B (catalog) — agents_[0] is the sole reader for
        // pg_namespace; manager-map fallback removed (Step 8.1.B).
        const collection_storage_entry_t* ns_entry = nullptr;
        if (!agents_.empty() && agents_[0] != nullptr) {
            ns_entry = agents_[0]->storage_entry_sync(pg_namespace_oid_tbl);
        }
        if (ns_entry == nullptr) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(ns_entry->table_storage.table(),
                    {0, 1},
                    &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto name_v = chunk.value(1, i);
                        if (!name_v.is_null()) {
                            out.emplace_back(std::string(name_v.value<std::string_view>()));
                        }
                        return true;
                    });
        co_return out;
    }

    // --- Direct replay methods (synchronous, no MVCC, for physical WAL replay) ---

    manager_disk_t::unique_future<std::vector<components::catalog::oid_t>>
    manager_disk_t::allocate_oids_batch(std::size_t count) {
        std::vector<components::catalog::oid_t> batch;
        batch.reserve(count);
        for (std::size_t i = 0; i < count; ++i) {
            batch.push_back(oid_gen_.allocate());
        }
        co_return batch;
    }

    // ---------------------------------------------------------------------------
    // scan_by_key — pure storage primitive, oid-keyed.
    // ---------------------------------------------------------------------------

    manager_disk_t::unique_future<std::pmr::vector<std::int64_t>>
    manager_disk_t::scan_by_key(execution_context_t ctx,
                                components::catalog::oid_t table_oid,
                                std::pmr::vector<std::string> key_col_names,
                                std::pmr::vector<components::types::logical_value_t> key_values) {
        std::pmr::vector<std::int64_t> out(resource());
        if (key_col_names.size() != key_values.size() || key_col_names.empty()) {
            co_return out;
        }

        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. The routed
        // agent slice (catalog → agents_[0], user → agents_[1..N-1] via
        // pool_idx_for_oid) is the sole canonical SFBM/IN_MEMORY owner.
        if (agents_.empty())
            co_return out;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[idx] == nullptr)
            co_return out;
        const collection_storage_entry_t* entry = agents_[idx]->storage_entry_sync(table_oid);
        if (entry == nullptr)
            co_return out;

        auto& tbl = entry->table_storage.table();
        const auto& all_cols = tbl.columns();

        auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
        for (std::size_t ki = 0; ki < key_col_names.size(); ++ki) {
            std::size_t col_idx = all_cols.size();
            for (std::size_t ci = 0; ci < all_cols.size(); ++ci) {
                if (all_cols[ci].name() == key_col_names[ki]) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == all_cols.size())
                co_return out;
            std::pmr::vector<uint64_t> idx_vec(resource());
            idx_vec.push_back(static_cast<uint64_t>(col_idx));
            filter->child_filters.push_back(
                std::make_unique<components::table::constant_filter_t>(components::expressions::compare_type::eq,
                                                                       key_values[ki],
                                                                       std::move(idx_vec)));
        }

        auto types = entry->storage->types();
        components::vector::data_chunk_t chunk(resource(), types);
        entry->storage->scan(chunk, filter.get(), -1, ctx.txn);
        for (uint64_t i = 0; i < chunk.size(); ++i) {
            out.push_back(chunk.row_ids.data<std::int64_t>()[i]);
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::pmr::vector<components::types::logical_value_t>>>
    manager_disk_t::read_rows_by_key(execution_context_t ctx,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<std::string> key_col_names,
                                     std::pmr::vector<components::types::logical_value_t> key_values) {
        using row_t = std::pmr::vector<components::types::logical_value_t>;
        std::pmr::vector<row_t> out(resource());
        if (key_col_names.size() != key_values.size() || key_col_names.empty())
            co_return out;

        // Step 8.11 wrap (2026-05-31): manager.storages_ deleted. The routed
        // agent slice (catalog → agents_[0], user → agents_[1..N-1] via
        // pool_idx_for_oid) is the sole canonical SFBM/IN_MEMORY owner.
        if (agents_.empty())
            co_return out;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        if (agents_[idx] == nullptr)
            co_return out;
        const collection_storage_entry_t* entry = agents_[idx]->storage_entry_sync(table_oid);
        if (entry == nullptr)
            co_return out;

        auto& tbl = entry->table_storage.table();
        const auto& all_cols = tbl.columns();

        auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
        for (std::size_t ki = 0; ki < key_col_names.size(); ++ki) {
            std::size_t col_idx = all_cols.size();
            for (std::size_t ci = 0; ci < all_cols.size(); ++ci) {
                if (all_cols[ci].name() == key_col_names[ki]) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == all_cols.size())
                co_return out;
            std::pmr::vector<uint64_t> idx_vec(resource());
            idx_vec.push_back(static_cast<uint64_t>(col_idx));
            filter->child_filters.push_back(
                std::make_unique<components::table::constant_filter_t>(components::expressions::compare_type::eq,
                                                                       key_values[ki],
                                                                       std::move(idx_vec)));
        }

        auto types = entry->storage->types();
        components::vector::data_chunk_t chunk(resource(), types);
        entry->storage->scan(chunk, filter.get(), -1, ctx.txn);

        for (uint64_t i = 0; i < chunk.size(); ++i) {
            row_t row(resource());
            row.reserve(chunk.column_count());
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                row.push_back(chunk.value(c, i));
            }
            out.push_back(std::move(row));
        }
        co_return out;
    }

} // namespace services::disk

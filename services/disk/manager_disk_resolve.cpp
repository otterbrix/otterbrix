#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    manager_disk_t::unique_future<resolve_namespace_result_t>
    manager_disk_t::resolve_namespace(execution_context_t /*ctx*/,
                                       std::string name,
                                       std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_namespace.fetch_add(1, std::memory_order_relaxed);
        resolve_namespace_result_t out(resource());
        out.catalog_version = catalog_version_;

        if (auto it = ns_name_to_oid_.find(name); it != ns_name_to_oid_.end()) {
            out.found = true;
            out.oid = it->second;
            out.name = std::move(name);
        }
        // Note: events list is intentionally left empty for now; consumers pass in their
        // last-seen version and call recent_invalidations_since separately. M5 may inline
        // this once the plan-cache contract solidifies.
        co_return out;
    }

    manager_disk_t::unique_future<resolve_table_result_t>
    manager_disk_t::resolve_table(execution_context_t /*ctx*/,
                                   components::catalog::oid_t namespace_oid,
                                   std::string name,
                                   std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_table.fetch_add(1, std::memory_order_relaxed);
        resolve_table_result_t out(resource());
        out.catalog_version = catalog_version_;
        out.namespace_oid = namespace_oid;

        // O(1) lookup in table_to_oid_ index (populated by DDL and rebuild_lookup_indexes).
        if (auto it = table_to_oid_.find(ns_table_key_t{namespace_oid, name}); it != table_to_oid_.end()) {
            out.found = true;
            out.oid = it->second.oid;
            out.relkind = it->second.relkind;
            out.name = name;
        }

        if (!out.found) {
            co_return out;
        }

        // If this is a DISK-backed regular relation whose storage hasn't been instantiated
        // yet, load it from disk now. Skips computing tables (relkind='g') and indexes
        // (relkind='i') — they don't have a .otbx file.
        if (out.relkind == catalog::relkind::regular && !config_.path.empty()) {
            // O(1) reverse lookup: namespace OID → namespace name.
            if (auto ns_it = ns_oid_to_name_.find(namespace_oid); ns_it != ns_oid_to_name_.end()) {
                const auto& ns_name = ns_it->second;
                collection_full_name_t coll_name{ns_name, name};
                if (storages_.find(coll_name) == storages_.end()) {
                    auto otbx_path = config_.path / ns_name / "main" / name / "table.otbx";
                    if (std::filesystem::exists(otbx_path)) {
                        try {
                            load_storage_disk_sync(coll_name, otbx_path);
                        } catch (const std::exception& e) {
                            warn(log_,
                                 "resolve_table lazy-load failed for {}/{}: {}",
                                 ns_name,
                                 name,
                                 e.what());
                        }
                    }
                }
            }
        }

        // Computing tables (relkind='g') store columns in pg_computed_column
        // (versioned + ref-counted). Pick latest attversion per attname where
        // attrefcount > 0, ordered by first appearance.
        if (out.relkind == catalog::relkind::computed) {
            auto cc_it = storages_.find(pg_computed_column_name);
            if (cc_it != storages_.end()) {
                std::pmr::synchronized_pool_resource cc_scan_resource;
                // pg_computed_column: 0=relid 1=attoid 2=attname 3=atttypid 4=attversion 5=attrefcount
                struct cc_row_t {
                    components::catalog::oid_t attoid;
                    std::string attname;
                    components::catalog::oid_t atttypid;
                    std::int64_t attversion;
                };
                std::unordered_map<std::string, cc_row_t> latest;
                inline_scan(cc_it->second->table_storage.table(), {0, 1, 2, 3, 4, 5}, &cc_scan_resource,
                            [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                                auto rel = chunk.value(0, i);
                                if (rel.is_null()) return true;
                                if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != out.oid)
                                    return true;
                                auto rc = chunk.value(5, i);
                                if (rc.is_null() || rc.value<std::int64_t>() <= 0) return true;
                                cc_row_t row;
                                row.attoid = static_cast<components::catalog::oid_t>(
                                    chunk.value(1, i).value<std::uint32_t>());
                                row.attname = std::string(chunk.value(2, i).value<std::string_view>());
                                row.atttypid = static_cast<components::catalog::oid_t>(
                                    chunk.value(3, i).value<std::uint32_t>());
                                row.attversion = chunk.value(4, i).value<std::int64_t>();
                                auto it = latest.find(row.attname);
                                if (it == latest.end() || it->second.attversion < row.attversion) {
                                    latest[row.attname] = std::move(row);
                                }
                                return true;
                            });
                std::int32_t synthetic_attnum = 1;
                for (auto& [_, row] : latest) {
                    column_info_t info;
                    info.attoid = row.attoid;
                    info.attname = row.attname;
                    info.atttypid = row.atttypid;
                    info.attnum = synthetic_attnum++;
                    info.attnotnull = false;
                    info.atthasdefault = false;
                    info.attisdropped = false;
                    out.columns.push_back(std::move(info));
                }
            }
            co_return out;
        }

        // Collect column metadata from pg_attribute where attrelid == out.oid AND
        // attisdropped == false. Sorted by attnum.
        auto att_it = storages_.find(pg_attribute_name);
        if (att_it != storages_.end()) {
            // pg_attribute layout:
            // 0=attoid 1=attrelid 2=attname 3=atttypid 4=attnum 5=attnotnull
            // 6=atthasdefault 7=attisdropped 8=atttypspec 9=attdefspec
            std::pmr::synchronized_pool_resource scan_resource;
            std::vector<column_info_t> rows;
            inline_scan(att_it->second->table_storage.table(),
                        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel = chunk.value(1, i);
                            if (rel.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    rel.value<std::uint32_t>()) != out.oid)
                                return true;
                            auto dropped = chunk.value(7, i);
                            const bool is_dropped = !dropped.is_null() && dropped.value<bool>();
                            if (is_dropped)
                                return true;
                            column_info_t info;
                            info.attoid = static_cast<components::catalog::oid_t>(
                                chunk.value(0, i).value<std::uint32_t>());
                            auto name_v = chunk.value(2, i);
                            if (!name_v.is_null())
                                info.attname = std::string(name_v.value<std::string_view>());
                            auto typid_v = chunk.value(3, i);
                            if (!typid_v.is_null())
                                info.atttypid = static_cast<components::catalog::oid_t>(
                                    typid_v.value<std::uint32_t>());
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
            std::sort(rows.begin(), rows.end(),
                       [](const column_info_t& a, const column_info_t& b) {
                           return a.attnum < b.attnum;
                       });
            out.columns = std::move(rows);
        }
        co_return out;
    }

    // V4 helper: synchronous resolve of a type by name. Used by both resolve_type and
    // its own recursive expansion path (composite STRUCT field references resolved
    // against pg_class with relkind='c' / 'd'). Splitting this out as sync (no
    // unique_future) lets us self-recurse without cross-actor co_await, which doesn't
    // work from within an actor's own coroutine.
    resolve_type_result_t
    manager_disk_t::resolve_type_sync(components::catalog::oid_t namespace_oid,
                                       const std::string& name) {
        resolve_type_result_t out(resource());
        out.catalog_version = catalog_version_;
        out.namespace_oid = namespace_oid;

        auto it = storages_.find(pg_type_name);
        if (it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns = chunk.value(2, i);
                            if (ns.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    ns.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            if (!str_equals(chunk.value(1, i), name))
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(
                                chunk.value(0, i).value<std::uint32_t>());
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
        // Composite STRUCT (CREATE TYPE ... AS (...)) — persisted in pg_class with
        // relkind='c'. Reconstruct STRUCT shape by scanning pg_attribute.
        auto cls_it = storages_.find(pg_class_name);
        if (cls_it == storages_.end()) {
            return out;
        }
        components::catalog::oid_t composite_oid = components::catalog::INVALID_OID;
        std::pmr::synchronized_pool_resource scan_resource;
        inline_scan(cls_it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rns_v = chunk.value(2, i);
                        if (rns_v.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(
                                rns_v.value<std::uint32_t>()) != namespace_oid)
                            return true;
                        auto kind_v = chunk.value(3, i);
                        if (kind_v.is_null())
                            return true;
                        auto kind_s = kind_v.value<std::string_view>();
                        if (kind_s.empty() || kind_s.front() != catalog::relkind::composite_type)
                            return true;
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        composite_oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        return false;
                    });
        if (composite_oid == components::catalog::INVALID_OID) {
            return out;
        }
        auto att_it = storages_.find(pg_attribute_name);
        if (att_it == storages_.end()) {
            return out;
        }
        struct field_row {
            std::string attname;
            components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
            std::int32_t attnum{0};
            std::string atttypspec;
        };
        std::vector<field_row> fields;
        // pg_attribute (col_indices map): 0=attrelid 1=attname 2=atttypid 3=attnum 4=attisdropped 5=atttypspec
        inline_scan(att_it->second->table_storage.table(),
                    {1, 2, 3, 4, 7, 8}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel = chunk.value(0, i);
                        if (rel.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(
                                rel.value<std::uint32_t>()) != composite_oid)
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
                            r.atttypid = static_cast<components::catalog::oid_t>(
                                typid_v.value<std::uint32_t>());
                        r.attnum = chunk.value(3, i).value<std::int32_t>();
                        auto spec_v = chunk.value(5, i);
                        if (!spec_v.is_null())
                            r.atttypspec = std::string(spec_v.value<std::string_view>());
                        fields.push_back(std::move(r));
                        return true;
                    });
        std::sort(fields.begin(), fields.end(),
                  [](const field_row& a, const field_row& b) { return a.attnum < b.attnum; });
        std::vector<components::types::complex_logical_type> child_types;
        child_types.reserve(fields.size());
        for (auto& f : fields) {
            components::types::complex_logical_type ft = f.atttypspec.empty()
                ? components::types::complex_logical_type{
                      components::catalog::oid_to_builtin_type(f.atttypid)}
                : components::catalog::decode_type_spec(resource(), f.atttypspec);
            // Composite fields can reference other UDTs via UNKNOWN — recurse so the
            // returned STRUCT is fully expanded (mirrors populate-path).
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
        resolve_counters_.resolve_type.fetch_add(1, std::memory_order_relaxed);
        co_return resolve_type_sync(namespace_oid, name);
    }

    manager_disk_t::unique_future<resolve_function_result_t>
    manager_disk_t::resolve_function(execution_context_t /*ctx*/,
                                      components::catalog::oid_t namespace_oid,
                                      std::string name,
                                      std::uint64_t /*since_version*/) {
        resolve_counters_.resolve_function.fetch_add(1, std::memory_order_relaxed);
        resolve_function_result_t out(resource());
        out.catalog_version = catalog_version_;
        out.namespace_oid = namespace_oid;

        auto it = storages_.find(pg_proc_name);
        if (it != storages_.end()) {
            std::pmr::synchronized_pool_resource scan_resource;
            // pg_proc layout:
            // 0=oid 1=proname 2=pronamespace 3=pronargs 4=prouid 5=proargmatchers 6=prorettype
            inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6}, &scan_resource,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto ns = chunk.value(2, i);
                            if (ns.is_null())
                                return true;
                            if (static_cast<components::catalog::oid_t>(
                                    ns.value<std::uint32_t>()) != namespace_oid)
                                return true;
                            if (!str_equals(chunk.value(1, i), name))
                                return true;
                            out.found = true;
                            out.oid = static_cast<components::catalog::oid_t>(
                                chunk.value(0, i).value<std::uint32_t>());
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
        resolve_counters_.resolve_function_by_name.fetch_add(1, std::memory_order_relaxed);
        std::pmr::vector<resolve_function_result_t> out(resource());
        auto it = storages_.find(pg_proc_name);
        if (it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_proc layout: 0=oid 1=proname 2=pronamespace 3=pronargs 4=prouid
        //                 5=proargmatchers 6=prorettype
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3, 4, 5, 6}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        if (!str_equals(chunk.value(1, i), name))
                            return true;
                        resolve_function_result_t r(resource());
                        r.found = true;
                        r.catalog_version = catalog_version_;
                        r.name = name;
                        r.oid = static_cast<components::catalog::oid_t>(
                            chunk.value(0, i).value<std::uint32_t>());
                        auto ns_v = chunk.value(2, i);
                        if (!ns_v.is_null())
                            r.namespace_oid = static_cast<components::catalog::oid_t>(
                                ns_v.value<std::uint32_t>());
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
        auto it = storages_.find(pg_namespace_name);
        if (it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_namespace: 0=oid 1=nspname
        inline_scan(it->second->table_storage.table(), {0, 1}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto name_v = chunk.value(1, i);
                        if (!name_v.is_null()) {
                            out.emplace_back(std::string(name_v.value<std::string_view>()));
                        }
                        return true;
                    });
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::pair<components::catalog::oid_t, std::string>>>
    manager_disk_t::list_tables_in_namespace(execution_context_t /*ctx*/,
                                              components::catalog::oid_t namespace_oid) {
        std::pmr::vector<std::pair<components::catalog::oid_t, std::string>> out(resource());
        auto it = storages_.find(pg_class_name);
        if (it == storages_.end()) {
            co_return out;
        }
        std::pmr::synchronized_pool_resource scan_resource;
        // pg_class: 0=oid 1=relname 2=relnamespace 3=relkind
        inline_scan(it->second->table_storage.table(), {0, 1, 2, 3}, &scan_resource,
                    [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                        auto rel_ns = chunk.value(2, i);
                        if (rel_ns.is_null())
                            return true;
                        if (static_cast<components::catalog::oid_t>(rel_ns.value<std::uint32_t>()) != namespace_oid)
                            return true;
                        auto kind_v = chunk.value(3, i);
                        if (kind_v.is_null())
                            return true;
                        auto kind_s = kind_v.value<std::string_view>();
                        if (kind_s.empty())
                            return true;
                        const char relkind = kind_s.front();
                        // Only "live storage" kinds: regular tables and computing tables.
                        // Indexes, sequences, views, macros, composites are filtered.
                        if (relkind != catalog::relkind::regular && relkind != catalog::relkind::computed)
                            return true;
                        auto oid_v = chunk.value(0, i);
                        auto name_v = chunk.value(1, i);
                        if (oid_v.is_null() || name_v.is_null())
                            return true;
                        out.emplace_back(static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>()),
                                          std::string(name_v.value<std::string_view>()));
                        return true;
                    });
        co_return out;
    }

    manager_disk_t::unique_future<invalidation_ring_buffer_t::snapshot_t>
    manager_disk_t::recent_invalidations_since(session_id_t /*session*/, std::uint64_t since_version) {
        co_return invalidations_.since(since_version);
    }

    // ========================================================================
    // populate_catalog_snapshot retired (V4): catalog_view_t serves all per-name lookups,
    // and dispatcher's collections_ rebuild uses list_namespaces +
    // list_tables_in_namespace. Batch resolve_*_batch methods are deferred until
    // profiling shows warm-cache hit rate is too low.
    // ========================================================================

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
    // scan_by_key — pure storage primitive, no FK/semantic knowledge.
    // ---------------------------------------------------------------------------

    manager_disk_t::unique_future<std::pmr::vector<std::int64_t>>
    manager_disk_t::scan_by_key(execution_context_t ctx,
                                 collection_full_name_t name,
                                 std::vector<std::string> key_col_names,
                                 std::vector<components::types::logical_value_t> key_values) {
        std::pmr::vector<std::int64_t> out(resource());
        if (key_col_names.size() != key_values.size() || key_col_names.empty()) {
            co_return out;
        }

        auto it = storages_.find(name);
        if (it == storages_.end()) co_return out;

        auto& tbl = it->second->table_storage.table();
        const auto& all_cols = tbl.columns();

        // Map each key column name to its table column index.
        auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
        std::pmr::synchronized_pool_resource fres;
        for (std::size_t ki = 0; ki < key_col_names.size(); ++ki) {
            std::size_t col_idx = all_cols.size();
            for (std::size_t ci = 0; ci < all_cols.size(); ++ci) {
                if (all_cols[ci].name() == key_col_names[ki]) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == all_cols.size()) co_return out; // unknown column → empty result
            std::pmr::vector<uint64_t> idx_vec(&fres);
            idx_vec.push_back(static_cast<uint64_t>(col_idx));
            filter->child_filters.push_back(
                std::make_unique<components::table::constant_filter_t>(
                    components::expressions::compare_type::eq,
                    key_values[ki],
                    std::move(idx_vec)));
        }

        auto types = it->second->storage->types();
        components::vector::data_chunk_t chunk(resource(), types);
        it->second->storage->scan(chunk, filter.get(), -1, ctx.txn);
        for (uint64_t i = 0; i < chunk.size(); ++i) {
            out.push_back(chunk.row_ids.data<std::int64_t>()[i]);
        }
        co_return out;
    }

    // ---------------------------------------------------------------------------
    // point_lookup_by_index — look up first matching row via pg_index metadata.
    // Falls back to a sequential scan (no real B-tree yet).
    // ---------------------------------------------------------------------------

    manager_disk_t::unique_future<std::optional<std::int64_t>>
    manager_disk_t::point_lookup_by_index(execution_context_t ctx,
                                           components::catalog::oid_t index_oid,
                                           std::vector<components::types::logical_value_t> key_values) {
        // 1. Scan pg_index for the row with indexrelid == index_oid.
        //    pg_index cols: 0=indexrelid, 1=indrelid, 2=indkey(csv), 3=indisvalid
        auto idx_it = storages_.find(pg_index_name);
        if (idx_it == storages_.end()) co_return std::nullopt;

        catalog::oid_t indrelid{0};
        std::string indkey_csv;
        bool found_index = false;
        {
            std::pmr::synchronized_pool_resource sr;
            inline_scan(idx_it->second->table_storage.table(), {0, 1, 2, 3}, &sr,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto oid_v = chunk.value(0, i);
                            if (oid_v.is_null()) return true;
                            if (static_cast<catalog::oid_t>(oid_v.value<std::uint32_t>()) != index_oid)
                                return true;
                            auto valid_v = chunk.value(3, i);
                            if (!valid_v.is_null() && !valid_v.value<bool>()) return false; // indisvalid=false
                            auto rel_v = chunk.value(1, i);
                            if (!rel_v.is_null())
                                indrelid = static_cast<catalog::oid_t>(rel_v.value<std::uint32_t>());
                            auto key_v = chunk.value(2, i);
                            if (!key_v.is_null())
                                indkey_csv = std::string(key_v.value<std::string_view>());
                            found_index = true;
                            return false;
                        });
        }
        if (!found_index) co_return std::nullopt;

        // 2. Parse indkey CSV to get ordered attoid list.
        const auto attoids = catalog::parse_oid_csv(indkey_csv);
        if (attoids.empty() || attoids.size() != key_values.size()) co_return std::nullopt;

        // 3. Find collection name for indrelid via reverse lookup.
        auto key_it = table_oid_to_key_.find(indrelid);
        if (key_it == table_oid_to_key_.end()) co_return std::nullopt;
        const auto& [ns_oid, tbl_name] = key_it->second;
        auto ns_name_it = ns_oid_to_name_.find(ns_oid);
        if (ns_name_it == ns_oid_to_name_.end()) co_return std::nullopt;
        collection_full_name_t tbl_full{ns_name_it->second, "main", tbl_name};

        // 4. Scan pg_attribute to map each attoid → column name (for indrelid).
        //    pg_attribute cols: 0=attoid, 1=attrelid, 2=attname, ...
        std::unordered_map<catalog::oid_t, std::string> attoid_to_name;
        auto pa_it = storages_.find(pg_attribute_name);
        if (pa_it == storages_.end()) co_return std::nullopt;
        {
            std::pmr::synchronized_pool_resource sr;
            inline_scan(pa_it->second->table_storage.table(), {0, 1, 2}, &sr,
                        [&](components::vector::data_chunk_t& chunk, uint64_t i) {
                            auto rel_v = chunk.value(1, i);
                            if (rel_v.is_null()) return true;
                            if (static_cast<catalog::oid_t>(rel_v.value<std::uint32_t>()) != indrelid)
                                return true;
                            auto att_v = chunk.value(0, i);
                            auto nm_v  = chunk.value(2, i);
                            if (!att_v.is_null() && !nm_v.is_null()) {
                                attoid_to_name[static_cast<catalog::oid_t>(att_v.value<std::uint32_t>())] =
                                    std::string(nm_v.value<std::string_view>());
                            }
                            return true;
                        });
        }

        // 5. Map attoids → column names in indkey order.
        std::vector<std::string> col_names;
        col_names.reserve(attoids.size());
        for (auto ao : attoids) {
            auto nm_it = attoid_to_name.find(ao);
            if (nm_it == attoid_to_name.end()) co_return std::nullopt;
            col_names.push_back(nm_it->second);
        }

        // 6. scan_by_key on the user table.
        auto row_ids = co_await scan_by_key(ctx, tbl_full, std::move(col_names), std::move(key_values));
        if (row_ids.empty()) co_return std::nullopt;
        co_return row_ids[0];
    }

    // ---------------------------------------------------------------------------
    // read_rows_by_key — full row-data variant of scan_by_key.
    // ---------------------------------------------------------------------------

    manager_disk_t::unique_future<std::vector<std::vector<components::types::logical_value_t>>>
    manager_disk_t::read_rows_by_key(execution_context_t ctx,
                                       collection_full_name_t name,
                                       std::vector<std::string> key_col_names,
                                       std::vector<components::types::logical_value_t> key_values) {
        using row_t = std::vector<components::types::logical_value_t>;
        std::vector<row_t> out;
        if (key_col_names.size() != key_values.size() || key_col_names.empty()) co_return out;

        auto it = storages_.find(name);
        if (it == storages_.end()) co_return out;

        auto& tbl = it->second->table_storage.table();
        const auto& all_cols = tbl.columns();

        auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
        std::pmr::synchronized_pool_resource fres;
        for (std::size_t ki = 0; ki < key_col_names.size(); ++ki) {
            std::size_t col_idx = all_cols.size();
            for (std::size_t ci = 0; ci < all_cols.size(); ++ci) {
                if (all_cols[ci].name() == key_col_names[ki]) { col_idx = ci; break; }
            }
            if (col_idx == all_cols.size()) co_return out;
            std::pmr::vector<uint64_t> idx_vec(&fres);
            idx_vec.push_back(static_cast<uint64_t>(col_idx));
            filter->child_filters.push_back(
                std::make_unique<components::table::constant_filter_t>(
                    components::expressions::compare_type::eq,
                    key_values[ki],
                    std::move(idx_vec)));
        }

        auto types = it->second->storage->types();
        components::vector::data_chunk_t chunk(resource(), types);
        it->second->storage->scan(chunk, filter.get(), -1, ctx.txn);

        for (uint64_t i = 0; i < chunk.size(); ++i) {
            row_t row;
            row.reserve(chunk.column_count());
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                row.push_back(chunk.value(c, i));
            }
            out.push_back(std::move(row));
        }
        co_return out;
    }

    // ---------------------------------------------------------------------------
    // scan_by_table_oid — OID-keyed variant of scan_by_key.
    // ---------------------------------------------------------------------------

    manager_disk_t::unique_future<std::pmr::vector<std::int64_t>>
    manager_disk_t::scan_by_table_oid(execution_context_t ctx,
                                        components::catalog::oid_t table_oid,
                                        std::vector<std::string> key_col_names,
                                        std::vector<components::types::logical_value_t> key_values) {
        std::pmr::vector<std::int64_t> out(resource());
        auto key_it = table_oid_to_key_.find(table_oid);
        if (key_it == table_oid_to_key_.end()) co_return out;

        const auto& [ns_oid, tbl_name] = key_it->second;
        auto ns_it = ns_oid_to_name_.find(ns_oid);
        if (ns_it == ns_oid_to_name_.end()) co_return out;

        collection_full_name_t full{ns_it->second, "main", tbl_name};
        out = co_await scan_by_key(ctx, std::move(full), std::move(key_col_names), std::move(key_values));
        co_return out;
    }

} // namespace services::disk

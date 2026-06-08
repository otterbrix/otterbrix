#include "manager_disk_impl.hpp"

namespace services::disk {

    using namespace core::filesystem;
    namespace catalog = components::catalog;
    using namespace detail;

    // Every catalog read here goes through agent-0's storage_scan_batched_inner
    // (catalog oids route to agent-0). Reading via the mailbox — not a borrowed
    // storage_entry_sync pointer — serialises against agent-0's compact path
    // (checkpoint/vacuum/maybe_cleanup_inner) running on the scheduler_disk_
    // threads, avoiding a borrowed-pointer race. transaction_data{} = "see all
    // committed". read_chunks_by_key adds one hop:
    // storage_column_names_inner resolves column NAMES to indices, then the
    // filter is built manager-side and shipped to storage_scan_batched_inner.

    manager_disk_t::unique_future<resolve_namespace_result_t>
    manager_disk_t::resolve_namespace(execution_context_t /*ctx*/, std::string name, std::uint64_t /*since_version*/) {
        resolve_namespace_result_t out(resource());

        if (!agents_.empty() && agents_[0] != nullptr) {
            const std::size_t idx = pool_idx_for_oid(pg_namespace_oid_tbl, agents_.size());
            std::vector<size_t> projected{0, 1};
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                                  &agent_disk_t::storage_scan_batched_inner,
                                                                  pg_namespace_oid_tbl,
                                                                  std::unique_ptr<components::table::table_filter_t>{},
                                                                  int64_t{-1},
                                                                  std::move(projected),
                                                                  components::table::transaction_data{});
            if (needs_sched) {
                scheduler_disk_->enqueue(agents_[idx].get());
            }
            auto batches = co_await std::move(fut);
            for (auto& chunk : batches) {
                bool stop = false;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto oid_v = chunk.value(0, i);
                    auto name_v = chunk.value(1, i);
                    if (oid_v.is_null() || name_v.is_null())
                        continue;
                    if (name_v.value<std::string_view>() != name)
                        continue;
                    out.found = true;
                    out.oid = static_cast<components::catalog::oid_t>(oid_v.value<std::uint32_t>());
                    out.name = name;
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
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

        if (!agents_.empty() && agents_[0] != nullptr) {
            const std::size_t idx = pool_idx_for_oid(pg_class_oid, agents_.size());
            std::vector<size_t> projected{0, 1, 2, 3};
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                                  &agent_disk_t::storage_scan_batched_inner,
                                                                  pg_class_oid,
                                                                  std::unique_ptr<components::table::table_filter_t>{},
                                                                  int64_t{-1},
                                                                  std::move(projected),
                                                                  components::table::transaction_data{});
            if (needs_sched) {
                scheduler_disk_->enqueue(agents_[idx].get());
            }
            auto batches = co_await std::move(fut);
            for (auto& chunk : batches) {
                bool stop = false;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto ns_v = chunk.value(2, i);
                    if (ns_v.is_null())
                        continue;
                    if (static_cast<components::catalog::oid_t>(ns_v.value<std::uint32_t>()) != namespace_oid)
                        continue;
                    auto name_v = chunk.value(1, i);
                    if (name_v.is_null() || name_v.value<std::string_view>() != name)
                        continue;
                    out.found = true;
                    out.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                    out.name = name;
                    auto kind_v = chunk.value(3, i);
                    if (!kind_v.is_null()) {
                        auto ks = kind_v.value<std::string_view>();
                        if (!ks.empty())
                            out.relkind = ks.front();
                    }
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }

        if (!out.found) {
            co_return out;
        }

        if (out.relkind == catalog::relkind::computed) {
            if (!agents_.empty() && agents_[0] != nullptr) {
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
                const std::size_t cc_idx = pool_idx_for_oid(pg_computed_column_oid, agents_.size());
                std::vector<size_t> cc_projected{0, 1, 2, 3, 4, 5, 6};
                auto [cc_needs_sched, cc_fut] =
                    actor_zeta::otterbrix::send(agents_[cc_idx]->address(),
                                                &agent_disk_t::storage_scan_batched_inner,
                                                pg_computed_column_oid,
                                                std::unique_ptr<components::table::table_filter_t>{},
                                                int64_t{-1},
                                                std::move(cc_projected),
                                                components::table::transaction_data{});
                if (cc_needs_sched) {
                    scheduler_disk_->enqueue(agents_[cc_idx].get());
                }
                auto cc_batches = co_await std::move(cc_fut);
                for (auto& chunk : cc_batches) {
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        auto rel = chunk.value(0, i);
                        if (rel.is_null())
                            continue;
                        if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != out.oid)
                            continue;
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
                    }
                }
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

        if (!agents_.empty() && agents_[0] != nullptr) {
            std::vector<column_info_t> rows;
            // Column MVCC: read added_at_commit_id (col 10) + dropped_at_commit_id
            // (col 11), filter by ctx.txn.start_time.
            const auto snapshot_start_time = ctx.txn.start_time;
            const std::size_t att_idx = pool_idx_for_oid(pg_attribute_oid, agents_.size());
            std::vector<size_t> att_projected{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
            auto [att_needs_sched, att_fut] =
                actor_zeta::otterbrix::send(agents_[att_idx]->address(),
                                            &agent_disk_t::storage_scan_batched_inner,
                                            pg_attribute_oid,
                                            std::unique_ptr<components::table::table_filter_t>{},
                                            int64_t{-1},
                                            std::move(att_projected),
                                            components::table::transaction_data{});
            if (att_needs_sched) {
                scheduler_disk_->enqueue(agents_[att_idx].get());
            }
            auto att_batches = co_await std::move(att_fut);
            for (auto& chunk : att_batches) {
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto rel = chunk.value(1, i);
                    if (rel.is_null())
                        continue;
                    if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != out.oid)
                        continue;
                    auto dropped = chunk.value(7, i);
                    const bool is_dropped = !dropped.is_null() && dropped.value<bool>();
                    if (is_dropped)
                        continue;
                    // MVCC visibility — column added after snapshot is hidden;
                    // column dropped before snapshot is hidden.
                    auto added_at_v = chunk.value(10, i);
                    if (!added_at_v.is_null()) {
                        auto added_at = static_cast<uint64_t>(added_at_v.value<std::int64_t>());
                        if (added_at > snapshot_start_time)
                            continue;
                    }
                    auto dropped_at_v = chunk.value(11, i);
                    if (!dropped_at_v.is_null()) {
                        auto dropped_at = static_cast<uint64_t>(dropped_at_v.value<std::int64_t>());
                        if (dropped_at != 0 && dropped_at <= snapshot_start_time)
                            continue;
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
                }
            }
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

    manager_disk_t::unique_future<resolve_type_result_t>
    manager_disk_t::resolve_type_sync(components::catalog::oid_t namespace_oid, const std::string& name) {
        resolve_type_result_t out(resource());
        out.namespace_oid = namespace_oid;

        if (!agents_.empty() && agents_[0] != nullptr) {
            const std::size_t type_idx = pool_idx_for_oid(pg_type_oid, agents_.size());
            std::vector<size_t> type_projected{0, 1, 2, 3};
            auto [type_needs_sched, type_fut] =
                actor_zeta::otterbrix::send(agents_[type_idx]->address(),
                                            &agent_disk_t::storage_scan_batched_inner,
                                            pg_type_oid,
                                            std::unique_ptr<components::table::table_filter_t>{},
                                            int64_t{-1},
                                            std::move(type_projected),
                                            components::table::transaction_data{});
            if (type_needs_sched) {
                scheduler_disk_->enqueue(agents_[type_idx].get());
            }
            auto type_batches = co_await std::move(type_fut);
            for (auto& chunk : type_batches) {
                bool stop = false;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto ns = chunk.value(2, i);
                    if (ns.is_null())
                        continue;
                    if (static_cast<components::catalog::oid_t>(ns.value<std::uint32_t>()) != namespace_oid)
                        continue;
                    if (!str_equals(chunk.value(1, i), name))
                        continue;
                    out.found = true;
                    out.oid = static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                    out.name = name;
                    auto def_v = chunk.value(3, i);
                    if (!def_v.is_null())
                        out.typdefspec = std::string(def_v.value<std::string_view>());
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }
        if (out.found) {
            co_return out;
        }
        if (agents_.empty() || agents_[0] == nullptr) {
            co_return out;
        }
        components::catalog::oid_t composite_oid = components::catalog::INVALID_OID;
        {
            const std::size_t cls_idx = pool_idx_for_oid(pg_class_oid, agents_.size());
            std::vector<size_t> cls_projected{0, 1, 2, 3};
            auto [cls_needs_sched, cls_fut] =
                actor_zeta::otterbrix::send(agents_[cls_idx]->address(),
                                            &agent_disk_t::storage_scan_batched_inner,
                                            pg_class_oid,
                                            std::unique_ptr<components::table::table_filter_t>{},
                                            int64_t{-1},
                                            std::move(cls_projected),
                                            components::table::transaction_data{});
            if (cls_needs_sched) {
                scheduler_disk_->enqueue(agents_[cls_idx].get());
            }
            auto cls_batches = co_await std::move(cls_fut);
            for (auto& chunk : cls_batches) {
                bool stop = false;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto rns_v = chunk.value(2, i);
                    if (rns_v.is_null())
                        continue;
                    if (static_cast<components::catalog::oid_t>(rns_v.value<std::uint32_t>()) != namespace_oid)
                        continue;
                    auto kind_v = chunk.value(3, i);
                    if (kind_v.is_null())
                        continue;
                    auto kind_s = kind_v.value<std::string_view>();
                    if (kind_s.empty() || kind_s.front() != catalog::relkind::composite_type)
                        continue;
                    if (!str_equals(chunk.value(1, i), name))
                        continue;
                    composite_oid =
                        static_cast<components::catalog::oid_t>(chunk.value(0, i).value<std::uint32_t>());
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }
        if (composite_oid == components::catalog::INVALID_OID) {
            co_return out;
        }
        struct field_row {
            std::string attname;
            components::catalog::oid_t atttypid{components::catalog::INVALID_OID};
            std::int32_t attnum{0};
            std::string atttypspec;
        };
        std::vector<field_row> fields;
        {
            const std::size_t att_idx = pool_idx_for_oid(pg_attribute_oid, agents_.size());
            std::vector<size_t> att_projected{1, 2, 3, 4, 7, 8};
            auto [att_needs_sched, att_fut] =
                actor_zeta::otterbrix::send(agents_[att_idx]->address(),
                                            &agent_disk_t::storage_scan_batched_inner,
                                            pg_attribute_oid,
                                            std::unique_ptr<components::table::table_filter_t>{},
                                            int64_t{-1},
                                            std::move(att_projected),
                                            components::table::transaction_data{});
            if (att_needs_sched) {
                scheduler_disk_->enqueue(agents_[att_idx].get());
            }
            auto att_batches = co_await std::move(att_fut);
            for (auto& chunk : att_batches) {
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto rel = chunk.value(0, i);
                    if (rel.is_null())
                        continue;
                    if (static_cast<components::catalog::oid_t>(rel.value<std::uint32_t>()) != composite_oid)
                        continue;
                    auto dropped = chunk.value(4, i);
                    if (!dropped.is_null() && dropped.value<bool>())
                        continue;
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
                }
            }
        }
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
                    auto nested = co_await resolve_type_sync(namespace_oid, ref_name);
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
        co_return out;
    }

    manager_disk_t::unique_future<resolve_type_result_t>
    manager_disk_t::resolve_type(execution_context_t /*ctx*/,
                                 components::catalog::oid_t namespace_oid,
                                 std::string name,
                                 std::uint64_t /*since_version*/) {
        co_return co_await resolve_type_sync(namespace_oid, name);
    }

    manager_disk_t::unique_future<resolve_function_result_t>
    manager_disk_t::resolve_function(execution_context_t /*ctx*/,
                                     components::catalog::oid_t namespace_oid,
                                     std::string name,
                                     std::uint64_t /*since_version*/) {
        resolve_function_result_t out(resource());
        out.namespace_oid = namespace_oid;

        if (!agents_.empty() && agents_[0] != nullptr) {
            const std::size_t idx = pool_idx_for_oid(pg_proc_oid, agents_.size());
            std::vector<size_t> projected{0, 1, 2, 3, 4, 5, 6};
            auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                                  &agent_disk_t::storage_scan_batched_inner,
                                                                  pg_proc_oid,
                                                                  std::unique_ptr<components::table::table_filter_t>{},
                                                                  int64_t{-1},
                                                                  std::move(projected),
                                                                  components::table::transaction_data{});
            if (needs_sched) {
                scheduler_disk_->enqueue(agents_[idx].get());
            }
            auto batches = co_await std::move(fut);
            for (auto& chunk : batches) {
                bool stop = false;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    auto ns = chunk.value(2, i);
                    if (ns.is_null())
                        continue;
                    if (static_cast<components::catalog::oid_t>(ns.value<std::uint32_t>()) != namespace_oid)
                        continue;
                    if (!str_equals(chunk.value(1, i), name))
                        continue;
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
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<resolve_function_result_t>>
    manager_disk_t::resolve_function_by_name(execution_context_t /*ctx*/,
                                             std::string name,
                                             std::uint64_t /*since_version*/) {
        std::pmr::vector<resolve_function_result_t> out(resource());
        if (agents_.empty() || agents_[0] == nullptr) {
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(pg_proc_oid, agents_.size());
        std::vector<size_t> projected{0, 1, 2, 3, 4, 5, 6};
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                              &agent_disk_t::storage_scan_batched_inner,
                                                              pg_proc_oid,
                                                              std::unique_ptr<components::table::table_filter_t>{},
                                                              int64_t{-1},
                                                              std::move(projected),
                                                              components::table::transaction_data{});
        if (needs_sched) {
            scheduler_disk_->enqueue(agents_[idx].get());
        }
        auto batches = co_await std::move(fut);
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!str_equals(chunk.value(1, i), name))
                    continue;
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
            }
        }
        co_return out;
    }

    manager_disk_t::unique_future<std::pmr::vector<std::string>>
    manager_disk_t::list_namespaces(execution_context_t /*ctx*/) {
        std::pmr::vector<std::string> out(resource());
        if (agents_.empty() || agents_[0] == nullptr) {
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(pg_namespace_oid_tbl, agents_.size());
        std::vector<size_t> projected{0, 1};
        auto [needs_sched, fut] = actor_zeta::otterbrix::send(agents_[idx]->address(),
                                                              &agent_disk_t::storage_scan_batched_inner,
                                                              pg_namespace_oid_tbl,
                                                              std::unique_ptr<components::table::table_filter_t>{},
                                                              int64_t{-1},
                                                              std::move(projected),
                                                              components::table::transaction_data{});
        if (needs_sched) {
            scheduler_disk_->enqueue(agents_[idx].get());
        }
        auto batches = co_await std::move(fut);
        for (auto& chunk : batches) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto name_v = chunk.value(1, i);
                if (!name_v.is_null()) {
                    out.emplace_back(std::string(name_v.value<std::string_view>()));
                }
            }
        }
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

    // Batched keyed scan for one table_oid. Every key routes to the SAME owning
    // agent (keyed by table_oid), so the per-key loop runs intra-agent: one
    // scan_by_keys_inner message carries the whole batch and the agent resolves the
    // shared key column names to indices once. result[i] corresponds to keys[i].
    manager_disk_t::unique_future<std::pmr::vector<std::pmr::vector<std::int64_t>>>
    manager_disk_t::scan_by_keys(execution_context_t ctx,
                                 components::catalog::oid_t table_oid,
                                 std::pmr::vector<std::string> key_col_names,
                                 components::vector::data_chunk_t keys) {
        std::pmr::vector<std::pmr::vector<std::int64_t>> out(resource());
        // INVARIANT: result.size() == keys.size() on EVERY path — one (possibly
        // empty) row per input key, in input order, so result[i] always maps to
        // keys[i]. Consumers (operator_fk_check / operator_fk_cascade) index
        // result[i] positionally and treat an empty row as "no parent match", so a
        // short outer vector would silently skip checks. No-key-columns, no-agents
        // and null-agent paths therefore still emit keys.size() empty rows rather
        // than a 0-size vector. (Per-key arity / unknown column is handled
        // agent-side, also yielding empty rows in result order.) keys.empty()
        // collapses to an empty result, which is keys.size()==0 — still the invariant.
        auto fill_empty_rows = [&]() {
            for (std::size_t i = 0; i < keys.size(); ++i) {
                out.emplace_back();
            }
        };
        if (keys.empty() || key_col_names.empty()) {
            fill_empty_rows();
            co_return out;
        }
        if (agents_.empty()) {
            fill_empty_rows();
            co_return out;
        }
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr) {
            fill_empty_rows();
            co_return out;
        }

        auto [scan_ns, scan_fut] = actor_zeta::otterbrix::send(agent->address(),
                                                               &agent_disk_t::scan_by_keys_inner,
                                                               table_oid,
                                                               std::move(key_col_names),
                                                               std::move(keys),
                                                               ctx.txn);
        if (scan_ns) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(scan_fut);
    }

    manager_disk_t::unique_future<std::pmr::vector<components::vector::data_chunk_t>>
    manager_disk_t::read_chunks_by_key(execution_context_t ctx,
                                       components::catalog::oid_t table_oid,
                                       std::pmr::vector<std::string> key_col_names,
                                       std::pmr::vector<components::types::logical_value_t> key_values) {
        // Name→index resolution + filtered scan, with the storage_scan_batched_inner
        // chunks returned as-is (no row-major flatten). Callers read cells via
        // chunk.value(col_idx, row_idx).
        std::pmr::vector<components::vector::data_chunk_t> empty(resource());
        if (key_col_names.size() != key_values.size() || key_col_names.empty())
            co_return empty;

        if (agents_.empty())
            co_return empty;
        const std::size_t idx = pool_idx_for_oid(table_oid, agents_.size());
        auto& agent = agents_[idx];
        if (agent == nullptr)
            co_return empty;

        auto [names_ns, names_fut] = actor_zeta::otterbrix::send(agent->address(),
                                                                  &agent_disk_t::storage_column_names_inner,
                                                                  table_oid);
        if (names_ns) {
            scheduler_disk_->enqueue(agent.get());
        }
        auto all_names = co_await std::move(names_fut);
        if (all_names.empty())
            co_return empty;

        auto filter = std::make_unique<components::table::conjunction_and_filter_t>();
        for (std::size_t ki = 0; ki < key_col_names.size(); ++ki) {
            std::size_t col_idx = all_names.size();
            for (std::size_t ci = 0; ci < all_names.size(); ++ci) {
                if (all_names[ci] == key_col_names[ki]) {
                    col_idx = ci;
                    break;
                }
            }
            if (col_idx == all_names.size())
                co_return empty;
            std::pmr::vector<uint64_t> idx_vec(resource());
            idx_vec.push_back(static_cast<uint64_t>(col_idx));
            filter->child_filters.push_back(
                std::make_unique<components::table::constant_filter_t>(components::expressions::compare_type::eq,
                                                                       key_values[ki],
                                                                       std::move(idx_vec)));
        }

        auto [scan_ns, scan_fut] =
            actor_zeta::otterbrix::send(agent->address(),
                                        &agent_disk_t::storage_scan_batched_inner,
                                        table_oid,
                                        std::unique_ptr<components::table::table_filter_t>{std::move(filter)},
                                        int64_t{-1},
                                        std::vector<size_t>{},
                                        ctx.txn);
        if (scan_ns) {
            scheduler_disk_->enqueue(agent.get());
        }
        co_return co_await std::move(scan_fut);
    }

} // namespace services::disk

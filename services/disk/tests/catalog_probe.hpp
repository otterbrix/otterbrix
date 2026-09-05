#pragma once

// Test-only catalog-read oracle.
//
// Production resolves catalog objects via physical-plan operators that call
// manager_disk_t::read_chunks_by_key. The disk-layer methods resolve_table /
// resolve_type / resolve_function used to provide the same lookups as a single
// mailbox call and were kept ONLY because disk tests used them as a convenient
// read oracle. Those methods have been deleted; this header reproduces the exact
// lookup logic they performed, but issues every catalog read through the live
// read_chunks_by_key path (the same boundary production uses).
//
// Each probe_* function mirrors a former resolve_*_result_t (plain std fields the
// tests assert) and reproduces the former filtering rules:
//   - probe_table  : pg_class scan by (relnamespace, relname); then pg_attribute
//                    columns with the same MVCC visibility filter (added_at /
//                    dropped_at vs ctx.txn.start_time) + attnum sort, OR the
//                    pg_computed_column branch (max-version-per-name, refcount>0,
//                    attoid-ASC order) for relkind='g' tables.
//   - probe_type   : pg_type scan by (typnamespace, typname); composite fallback
//                    via pg_class (relkind='c') + pg_attribute fields.
//   - probe_function : pg_proc scan by (pronamespace, proname).
//
// Reads route through the fixture's invoke mechanism (fx.invoke / fx.invoke_async),
// so this header is fixture-agnostic: both the disk-test `fixture` and the
// integration `fresh_disk` expose a compatible invoke template.

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/execution_context.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <limits>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace services::disk::test_probe {

    namespace catalog = components::catalog;

    // A snapshot that really does see every committed column.
    //
    // components::table::transaction_data{} -- and the transaction_data{0, 0} most probe
    // fixtures spell out -- is only HALF a see-all. Its snapshot_horizon defaults to
    // UINT64_MAX, so ROW visibility sees everything committed; its start_time is 0, and
    // COLUMN visibility (added_at_commit_id <= start_time) is judged against start_time. A
    // probe built that way therefore reads as "a snapshot taken before the first commit" and
    // hides every column an ALTER ... ADD COLUMN ever added.
    //
    // That was unobservable while the pg_attribute backfill never ran and added_at_commit_id
    // was permanently 0 (agent_disk_t::update_pg_attribute_commit_id_field_inner scanned with
    // a transaction that could not see the row it was asked to patch). Now that the column
    // carries the real commit id, a probe has to name the snapshot it means on BOTH halves.
    //
    // THE SITES THAT STILL BUILD A start_time == 0 CONTEXT, AND WHY EACH IS STILL SAFE.
    // Both dispatcher probes that DO drive a real ALTER through the commit operator were
    // moved onto this helper: services/dispatcher/tests/test_oid_alloc_operator_refusal.cpp:250
    // and services/dispatcher/tests/test_variant_e3_differential.cpp:163. The rest were left
    // alone deliberately, and the reason is not "nobody noticed":
    //   * services/disk/tests/test_error_handling.cpp:85 and
    //     services/disk/tests/test_d4_lazy_load.cpp:86 build
    //     execution_context_t{..., transaction_data{0, 0}, {}} and hand it to probe_table,
    //     which judges added_at against ctx.txn.start_time (below, ~line 315). Their columns
    //     are written by disk_test_helpers::test_add_column, which calls
    //     catalog::build_pg_attribute_row with the DEFAULT added_at_commit_id = 0 and then
    //     storage_publish_commits directly. It never goes through
    //     operator_commit_transaction_t's STEP 4, so no backfill ever stamps those rows and
    //     a start_time == 0 probe still sees them. They are safe BECAUSE they bypass the
    //     backfill, not because start_time 0 is a see-all.
    //   * services/disk/tests/test_checkpoint_dirty.cpp:135 is a transaction_data{0, 0} too,
    //     but it is a storage_append context, not a probe context -- that file never calls
    //     probe_table. start_time is not read on the append path.
    // Any probe fixture that starts driving ALTER ... ADD COLUMN through the real commit
    // pipeline has to switch to probe_see_all_txn(), or it will stop seeing the column.
    //
    // AND THE ASYMMETRY THIS EXPOSES, which is NOT a test-only matter. In production the same
    // pg_attribute rows are filtered by TWO DIFFERENT CLOCKS:
    //   * components/physical_plan/operators/operator_resolve_table.cpp:91 takes
    //     ctx->txn.start_time and applies added_at <= start_time (lines 418/424);
    //   * components/physical_plan/operators/alter_validators.cpp:46 takes
    //     exec_ctx.txn.snapshot_horizon and applies added_at <= horizon;
    //   * ROW visibility (row_version_manager's use_inserted_version) uses horizon plus
    //     in_flight_snapshot, i.e. the alter_validators clock.
    // components/table/transaction_manager.cpp draws start_time and every commit_id from the
    // ONE current_timestamp_ counter, and published_horizon_ only ever holds an already
    // allocated commit_id, so start_time > snapshot_horizon ALWAYS: resolve_table is strictly
    // the weaker filter, and it also ignores in_flight_snapshot entirely. The window that
    // opens: B commits and is handed commit id C (in_flight, not yet published); A begins with
    // start_time > C and horizon < C; A's resolve_table admits a column added at C while A's
    // row reads reject every row B wrote. Until this wave the divergence was unreachable
    // because added_at was permanently 0 -- making the backfill real is what woke it. Both
    // files are outside this change; recorded here, deliberately not fixed here.
    inline components::table::transaction_data probe_see_all_txn() {
        return components::table::transaction_data{0, std::numeric_limits<std::uint64_t>::max()};
    }

    // --- test-local result structs (mirror the deleted resolve_*_result_t) ---

    struct probe_column_info_t {
        std::string attname;
        std::string atttypspec;
        std::string attdefspec;
        catalog::oid_t atttypid{catalog::INVALID_OID};
        catalog::oid_t attoid{catalog::INVALID_OID};
        std::int32_t attnum{0};
        bool attnotnull{false};
        bool atthasdefault{false};
        bool attisdropped{false};
    };

    struct probe_table_result_t {
        bool found{false};
        catalog::oid_t oid{catalog::INVALID_OID};
        catalog::oid_t namespace_oid{catalog::INVALID_OID};
        char relkind{'r'};
        std::string name;
        std::vector<probe_column_info_t> columns;
    };

    struct probe_type_result_t {
        bool found{false};
        catalog::oid_t oid{catalog::INVALID_OID};
        catalog::oid_t namespace_oid{catalog::INVALID_OID};
        std::string name;
        std::string typdefspec;
    };

    struct probe_function_result_t {
        std::string name;
        std::string proargmatchers;
        std::string prorettype;
        std::uint64_t prouid{0};
        catalog::oid_t oid{catalog::INVALID_OID};
        catalog::oid_t namespace_oid{catalog::INVALID_OID};
        std::int32_t pronargs{0};
        bool found{false};
    };

    // --- local key-chunk builder (mirrors components::operators::make_key_chunk) ---
    //
    // Disk tests do not link physical_plan/operators, so the 1-row columnar key
    // carrier for read_chunks_by_key is built here. Column j carries values[j]'s own
    // type so the cell is written without a cast; cardinality is set to 1.
    inline components::vector::data_chunk_t
    build_key_chunk(std::pmr::memory_resource* resource, std::pmr::vector<components::types::logical_value_t> values) {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.reserve(values.size());
        for (const auto& v : values) {
            types.emplace_back(v.type());
        }
        components::vector::data_chunk_t chunk(resource, types, 1);
        for (std::size_t j = 0; j < values.size(); ++j) {
            chunk.set_value(static_cast<std::uint64_t>(j), 0, values[j]);
        }
        chunk.set_cardinality(1);
        return chunk;
    }

    // Issue a single-key read against `table_oid` filtering key_cols[j] == key_vals[j].
    // Returns the batched data chunks the owning agent's slice yields (txn-visible
    // rows per ctx.txn), exactly as the production read path would observe them.
    template<typename Fx>
    std::pmr::vector<components::vector::data_chunk_t>
    probe_read(Fx& fx,
               components::execution_context_t ctx,
               catalog::oid_t table_oid,
               std::pmr::vector<std::uint64_t> key_cols,
               std::pmr::vector<components::types::logical_value_t> key_vals,
               bool committed_scan = true) {
        auto keys = build_key_chunk(&fx.resource, std::move(key_vals));
        // committed_scan=true (default) mirrors the deleted resolve_* disk path: scan the
        // catalog committed-only (transaction_data{}) so a txn's own uncommitted catalog
        // writes are invisible; the caller still filters by ctx.txn.start_time.
        // committed_scan=false uses the caller's REAL ctx.txn — the PRODUCTION
        // operator_resolve_* semantics (read-your-own-uncommitted-catalog-writes).
        if (committed_scan) {
            ctx.txn = components::table::transaction_data{};
        }
        auto r = fx.invoke(&manager_disk_t::read_chunks_by_key,
                           ctx,
                           table_oid,
                           std::move(key_cols),
                           std::move(keys),
                           std::pmr::vector<std::uint64_t>{&fx.resource});
        // A probe read that could not be performed is a broken probe, not "no rows":
        // returning an empty vector here would make every caller below silently assert
        // "not found". Assert loudly instead — no test in this suite expects a failure.
        assert(!r.has_error() && "catalog_probe::probe_read: keyed catalog read failed");
        return std::move(r.value());
    }

    // --- probe_table ---------------------------------------------------------
    //
    // pg_class layout: oid(0), relname(1), relnamespace(2), relkind(3).
    // pg_attribute layout: attoid(0), attrelid(1), attname(2), atttypid(3),
    //   attnum(4), attnotnull(5), atthasdefault(6), attisdropped(7), atttypspec(8),
    //   attdefspec(9), added_at_commit_id(10), dropped_at_commit_id(11).
    // pg_computed_column layout: relid(0), attoid(1), attname(2), atttypid(3),
    //   atttypspec(4), attversion(5), attrefcount(6).
    template<typename Fx>
    probe_table_result_t probe_table(Fx& fx,
                                     components::execution_context_t ctx,
                                     catalog::oid_t namespace_oid,
                                     std::string name,
                                     bool committed_scan = true) {
        probe_table_result_t out;
        out.namespace_oid = namespace_oid;

        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_attribute = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_computed_column = catalog::well_known_oid::pg_computed_column_table;

        // pg_class scan by (relnamespace, relname).
        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_class_col::relnamespace);
            keys.emplace_back(catalog::pg_class_col::relname);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, namespace_oid);
            vals.emplace_back(&fx.resource, name);
            auto batches = probe_read(fx, ctx, pg_class, std::move(keys), std::move(vals), committed_scan);
            for (const auto& chunk : batches) {
                bool stop = false;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    auto oid_v = chunk.value(0, i);
                    if (oid_v.is_null())
                        continue;
                    out.found = true;
                    out.oid = static_cast<catalog::oid_t>(oid_v.template value<std::uint32_t>());
                    out.name = name;
                    auto kind_v = chunk.value(3, i);
                    if (!kind_v.is_null()) {
                        const auto kind_cell = kind_v;
                        const auto ks = kind_cell.template value<std::string_view>();
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

        if (!out.found)
            return out;

        if (out.relkind == catalog::relkind::computed) {
            // pg_computed_column: collect ALL rows for relid (incl. tombstones),
            // pick max-version per attname, drop names whose max-version row has
            // refcount<=0, then order surviving columns by attoid ASC.
            struct cc_row_t {
                catalog::oid_t attoid{catalog::INVALID_OID};
                std::string attname;
                catalog::oid_t atttypid{catalog::INVALID_OID};
                std::string atttypspec;
                std::int64_t attversion{0};
                std::int64_t attrefcount{0};
            };
            std::unordered_map<std::string, cc_row_t> latest_any;

            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_computed_column_col::relid);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, out.oid);
            auto batches = probe_read(fx, ctx, pg_computed_column, std::move(keys), std::move(vals), committed_scan);
            for (const auto& chunk : batches) {
                if (chunk.column_count() < 7)
                    continue;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    cc_row_t row;
                    auto attoid_v = chunk.value(1, i);
                    row.attoid = attoid_v.is_null()
                                     ? catalog::INVALID_OID
                                     : static_cast<catalog::oid_t>(attoid_v.template value<std::uint32_t>());
                    auto name_v = chunk.value(2, i);
                    if (!name_v.is_null()) {
                        const auto name_cell = name_v;
                        row.attname = std::string(name_cell.template value<std::string_view>());
                    }
                    auto typid_v = chunk.value(3, i);
                    row.atttypid = typid_v.is_null()
                                       ? catalog::INVALID_OID
                                       : static_cast<catalog::oid_t>(typid_v.template value<std::uint32_t>());
                    auto spec_v = chunk.value(4, i);
                    if (!spec_v.is_null()) {
                        const auto spec_cell = spec_v;
                        row.atttypspec = std::string(spec_cell.template value<std::string_view>());
                    }
                    row.attversion = chunk.value(5, i).template value<std::int64_t>();
                    auto rc_v = chunk.value(6, i);
                    row.attrefcount = rc_v.is_null() ? 0 : rc_v.template value<std::int64_t>();
                    auto it = latest_any.find(row.attname);
                    if (it == latest_any.end() || it->second.attversion < row.attversion) {
                        latest_any[row.attname] = std::move(row);
                    }
                }
            }
            std::vector<cc_row_t> ordered;
            ordered.reserve(latest_any.size());
            for (auto& [_, row] : latest_any) {
                if (row.attrefcount > 0)
                    ordered.push_back(std::move(row));
            }
            std::sort(ordered.begin(), ordered.end(), [](const cc_row_t& a, const cc_row_t& b) {
                return a.attoid < b.attoid;
            });
            std::int32_t synthetic_attnum = 1;
            for (auto& row : ordered) {
                probe_column_info_t info;
                info.attoid = row.attoid;
                info.attname = std::move(row.attname);
                info.atttypid = row.atttypid;
                info.atttypspec = std::move(row.atttypspec);
                info.attnum = synthetic_attnum++;
                info.attnotnull = false;
                info.atthasdefault = false;
                info.attisdropped = false;
                out.columns.push_back(std::move(info));
            }
            return out;
        }

        // pg_attribute: columns for attrelid == out.oid, with column-level MVCC
        // visibility (added_at / dropped_at vs ctx.txn.start_time) + attnum sort.
        {
            const auto snapshot_start_time = ctx.txn.start_time;
            std::vector<probe_column_info_t> rows;
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_attribute_col::attrelid);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, out.oid);
            auto batches = probe_read(fx, ctx, pg_attribute, std::move(keys), std::move(vals), committed_scan);
            for (const auto& chunk : batches) {
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    auto dropped = chunk.value(7, i);
                    if (!dropped.is_null() && dropped.template value<bool>())
                        continue;
                    auto added_at_v = chunk.value(10, i);
                    if (!added_at_v.is_null()) {
                        const auto added_at = static_cast<std::uint64_t>(added_at_v.template value<std::int64_t>());
                        if (added_at > snapshot_start_time)
                            continue;
                    }
                    auto dropped_at_v = chunk.value(11, i);
                    if (!dropped_at_v.is_null()) {
                        const auto dropped_at = static_cast<std::uint64_t>(dropped_at_v.template value<std::int64_t>());
                        if (dropped_at != 0 && dropped_at <= snapshot_start_time)
                            continue;
                    }
                    probe_column_info_t info;
                    info.attoid = static_cast<catalog::oid_t>(chunk.value(0, i).template value<std::uint32_t>());
                    auto name_v = chunk.value(2, i);
                    if (!name_v.is_null()) {
                        const auto name_cell = name_v;
                        info.attname = std::string(name_cell.template value<std::string_view>());
                    }
                    auto typid_v = chunk.value(3, i);
                    if (!typid_v.is_null())
                        info.atttypid = static_cast<catalog::oid_t>(typid_v.template value<std::uint32_t>());
                    info.attnum = chunk.value(4, i).template value<std::int32_t>();
                    auto nn_v = chunk.value(5, i);
                    info.attnotnull = !nn_v.is_null() && nn_v.template value<bool>();
                    auto def_v = chunk.value(6, i);
                    info.atthasdefault = !def_v.is_null() && def_v.template value<bool>();
                    info.attisdropped = false;
                    auto typspec_v = chunk.value(8, i);
                    if (!typspec_v.is_null()) {
                        const auto typspec_cell = typspec_v;
                        info.atttypspec = std::string(typspec_cell.template value<std::string_view>());
                    }
                    auto defspec_v = chunk.value(9, i);
                    if (!defspec_v.is_null()) {
                        const auto defspec_cell = defspec_v;
                        info.attdefspec = std::string(defspec_cell.template value<std::string_view>());
                    }
                    rows.push_back(std::move(info));
                }
            }
            std::sort(rows.begin(), rows.end(), [](const probe_column_info_t& a, const probe_column_info_t& b) {
                return a.attnum < b.attnum;
            });
            out.columns.assign(std::make_move_iterator(rows.begin()), std::make_move_iterator(rows.end()));
        }
        return out;
    }

    // --- probe_type ----------------------------------------------------------
    //
    // pg_type layout: oid(0), typname(1), typnamespace(2), typdefspec(3).
    // Composite fallback: pg_class (relkind='c') by (relnamespace, relname), then
    // pg_attribute fields by attrelid, encoded as a STRUCT type spec.
    template<typename Fx>
    probe_type_result_t
    probe_type(Fx& fx, components::execution_context_t ctx, catalog::oid_t namespace_oid, std::string name) {
        probe_type_result_t out;
        out.namespace_oid = namespace_oid;

        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_attribute = catalog::well_known_oid::pg_attribute_table;

        // pg_type scan by (typnamespace, typname).
        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_type_col::typnamespace);
            keys.emplace_back(catalog::pg_type_col::typname);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, namespace_oid);
            vals.emplace_back(&fx.resource, name);
            auto batches = probe_read(fx, ctx, pg_type, std::move(keys), std::move(vals));
            for (const auto& chunk : batches) {
                bool stop = false;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    auto oid_v = chunk.value(0, i);
                    if (oid_v.is_null())
                        continue;
                    out.found = true;
                    out.oid = static_cast<catalog::oid_t>(oid_v.template value<std::uint32_t>());
                    out.name = name;
                    auto def_v = chunk.value(3, i);
                    if (!def_v.is_null()) {
                        const auto def_cell = def_v;
                        out.typdefspec = std::string(def_cell.template value<std::string_view>());
                    }
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }
        if (out.found)
            return out;

        // Composite fallback via pg_class relkind='c'.
        catalog::oid_t composite_oid = catalog::INVALID_OID;
        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_class_col::relnamespace);
            keys.emplace_back(catalog::pg_class_col::relname);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, namespace_oid);
            vals.emplace_back(&fx.resource, name);
            auto batches = probe_read(fx, ctx, pg_class, std::move(keys), std::move(vals));
            for (const auto& chunk : batches) {
                bool stop = false;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    auto kind_v = chunk.value(3, i);
                    if (kind_v.is_null())
                        continue;
                    const auto kind_cell = kind_v;
                    const auto kind_s = kind_cell.template value<std::string_view>();
                    if (kind_s.empty() || kind_s.front() != catalog::relkind::composite_type)
                        continue;
                    composite_oid = static_cast<catalog::oid_t>(chunk.value(0, i).template value<std::uint32_t>());
                    stop = true;
                    break;
                }
                if (stop)
                    break;
            }
        }
        if (composite_oid == catalog::INVALID_OID)
            return out;

        struct field_row {
            std::string attname;
            catalog::oid_t atttypid{catalog::INVALID_OID};
            std::int32_t attnum{0};
            std::string atttypspec;
        };
        std::vector<field_row> fields;
        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_attribute_col::attrelid);
            std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
            vals.emplace_back(&fx.resource, composite_oid);
            auto batches = probe_read(fx, ctx, pg_attribute, std::move(keys), std::move(vals));
            for (const auto& chunk : batches) {
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    auto dropped = chunk.value(7, i);
                    if (!dropped.is_null() && dropped.template value<bool>())
                        continue;
                    field_row r;
                    auto name_v = chunk.value(2, i);
                    if (!name_v.is_null()) {
                        const auto name_cell = name_v;
                        r.attname = std::string(name_cell.template value<std::string_view>());
                    }
                    auto typid_v = chunk.value(3, i);
                    if (!typid_v.is_null())
                        r.atttypid = static_cast<catalog::oid_t>(typid_v.template value<std::uint32_t>());
                    r.attnum = chunk.value(4, i).template value<std::int32_t>();
                    auto spec_v = chunk.value(8, i);
                    if (!spec_v.is_null()) {
                        const auto spec_cell = spec_v;
                        r.atttypspec = std::string(spec_cell.template value<std::string_view>());
                    }
                    fields.push_back(std::move(r));
                }
            }
        }
        std::sort(fields.begin(), fields.end(), [](const field_row& a, const field_row& b) {
            return a.attnum < b.attnum;
        });
        std::pmr::vector<components::types::complex_logical_type> child_types(&fx.resource);
        child_types.reserve(fields.size());
        for (auto& f : fields) {
            components::types::complex_logical_type ft{components::types::logical_type::UNKNOWN};
            if (f.atttypspec.empty()) {
                ft = components::types::complex_logical_type{catalog::oid_to_builtin_type(f.atttypid)};
            } else {
                auto ft_r = catalog::decode_type_spec(&fx.resource, f.atttypspec);
                assert(!ft_r.has_error());
                ft = std::move(ft_r.value());
            }
            if (ft.type() == components::types::logical_type::UNKNOWN) {
                std::string ref_name(ft.type_name());
                if (!ref_name.empty()) {
                    auto nested = probe_type(fx, ctx, namespace_oid, ref_name);
                    if (nested.found && !nested.typdefspec.empty()) {
                        auto nested_r = catalog::decode_type_spec(&fx.resource, nested.typdefspec);
                        assert(!nested_r.has_error());
                        ft = std::move(nested_r.value());
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
        out.typdefspec = catalog::encode_type_spec(struct_t);
        return out;
    }

    // --- probe_function ------------------------------------------------------
    //
    // pg_proc layout: oid(0), proname(1), pronamespace(2), pronargs(3), prouid(4),
    //   proargmatchers(5), prorettype(6).
    template<typename Fx>
    probe_function_result_t
    probe_function(Fx& fx, components::execution_context_t ctx, catalog::oid_t namespace_oid, std::string name) {
        probe_function_result_t out;
        out.namespace_oid = namespace_oid;

        constexpr catalog::oid_t pg_proc = catalog::well_known_oid::pg_proc_table;
        std::pmr::vector<std::uint64_t> keys{&fx.resource};
        keys.emplace_back(catalog::pg_proc_col::pronamespace);
        keys.emplace_back(catalog::pg_proc_col::proname);
        std::pmr::vector<components::types::logical_value_t> vals{&fx.resource};
        vals.emplace_back(&fx.resource, namespace_oid);
        vals.emplace_back(&fx.resource, name);
        auto batches = probe_read(fx, ctx, pg_proc, std::move(keys), std::move(vals));
        for (const auto& chunk : batches) {
            bool stop = false;
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                auto oid_v = chunk.value(0, i);
                if (oid_v.is_null())
                    continue;
                out.found = true;
                out.oid = static_cast<catalog::oid_t>(oid_v.template value<std::uint32_t>());
                out.name = name;
                auto nargs_v = chunk.value(3, i);
                if (!nargs_v.is_null())
                    out.pronargs = nargs_v.template value<std::int32_t>();
                auto uid_v = chunk.value(4, i);
                if (!uid_v.is_null())
                    out.prouid = uid_v.template value<std::uint64_t>();
                auto args_v = chunk.value(5, i);
                if (!args_v.is_null()) {
                    const auto args_cell = args_v;
                    out.proargmatchers = std::string(args_cell.template value<std::string_view>());
                }
                auto ret_v = chunk.value(6, i);
                if (!ret_v.is_null()) {
                    const auto ret_cell = ret_v;
                    out.prorettype = std::string(ret_cell.template value<std::string_view>());
                }
                stop = true;
                break;
            }
            if (stop)
                break;
        }
        return out;
    }

} // namespace services::disk::test_probe

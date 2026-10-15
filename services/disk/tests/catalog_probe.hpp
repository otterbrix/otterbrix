#pragma once

// Test-only catalog-read oracle: production resolves catalog objects via physical-plan operators
// calling manager_disk_t::read_chunks_by_key; the disk layer has no resolve_table/resolve_type
// shortcut of its own, so each probe_* function below reproduces one resolve_*'s filtering over
// that same read path. Works with both the disk-test `fixture` and the integration `fresh_disk`.

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

    // transaction_data::committed() (what most probe fixtures pass) sees rows fine but judges column
    // visibility against start_time == 0, hiding a column an ALTER ... ADD COLUMN added later; a
    // fixture whose test drives that backfill must use this instead. This is a real production gap,
    // not just a test quirk: operator_resolve_table.cpp judges column visibility against start_time
    // while row visibility is judged against snapshot_horizon, so resolve_table can admit a column
    // whose row reads still reject — reachable only once added_at stops being a permanent 0.
    inline components::table::transaction_data probe_see_all_txn() {
        return components::table::transaction_data{0, std::numeric_limits<std::uint64_t>::max()};
    }

    // test-local result structs (mirror the deleted resolve_*_result_t).

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

    // Local key-chunk builder (mirrors components::operators::make_key_chunk): disk tests do not
    // link physical_plan/operators, so the 1-row columnar key carrier is rebuilt here. Pass string keys
    // as std::string_view.
    template<typename Val, typename... Vals>
    components::vector::data_chunk_t test_key_chunk(std::pmr::memory_resource* resource, Val&& val, Vals&&... vals) {
        std::pmr::vector<components::types::complex_logical_type> col_types(resource);
        col_types.reserve(1 + sizeof...(Vals));
        col_types.emplace_back(components::types::to_logical_type<std::remove_cvref_t<Val>>());
        (col_types.emplace_back(components::types::to_logical_type<std::remove_cvref_t<Vals>>()), ...);
        components::vector::data_chunk_t chunk(resource, col_types, 1);
        std::uint64_t col = 0;
        chunk.set_value(col++, std::uint64_t{0}, std::forward<Val>(val));
        (chunk.set_value(col++, std::uint64_t{0}, std::forward<Vals>(vals)), ...);
        chunk.set_cardinality(1);
        return chunk;
    }

    // Issue a single-key read against `table_oid` filtering key_cols[j] == key_vals[j], exactly as
    // the production read path would observe the rows.
    template<typename Fx>
    std::pmr::vector<components::vector::data_chunk_t>
    probe_read(Fx& fx,
               components::execution_context_t ctx,
               catalog::oid_t table_oid,
               std::pmr::vector<std::uint64_t> key_cols,
               components::vector::data_chunk_t keys,
               bool committed_scan = true) {
        // committed_scan=true (default) scans committed-only (transaction_data::committed()), mirroring the deleted
        // resolve_* disk path; committed_scan=false uses the caller's real ctx.txn, matching production
        // operator_resolve_* semantics (read-your-own-uncommitted-catalog-writes).
        if (committed_scan) {
            ctx.txn = components::table::transaction_data::committed();
        }
        auto r = fx.invoke(&manager_disk_t::read_chunks_by_key,
                           ctx,
                           table_oid,
                           std::move(key_cols),
                           std::move(keys),
                           std::pmr::vector<std::uint64_t>{&fx.resource});
        // A probe read that could not be performed is a broken probe, not "no rows" — an empty vector
        // here would make every caller below silently read as "not found", so assert loudly instead.
        assert(!r.has_error() && "catalog_probe::probe_read: keyed catalog read failed");
        return std::move(r.value());
    }

    // --- probe_table ---------------------------------------------------------
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

        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_class_col::relnamespace);
            keys.emplace_back(catalog::pg_class_col::relname);
            auto batches = probe_read(fx, ctx, pg_class, std::move(keys), test_key_chunk(&fx.resource, namespace_oid, std::string_view{name}), committed_scan);
            for (const auto& chunk : batches) {
                bool stop = false;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.data[0].is_null(i))
                        continue;
                    out.found = true;
                    out.oid = static_cast<catalog::oid_t>(chunk.data[0].template get_value<std::uint32_t>(i));
                    out.name = name;
                    if (!chunk.data[3].is_null(i)) {
                        const auto ks = chunk.data[3].template get_value<std::string_view>(i);
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
            // pg_computed_column: collect ALL rows for relid (incl. tombstones), pick max-version per
            // attname, drop names whose max-version row has refcount<=0, order survivors by attoid ASC.
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
            auto batches = probe_read(fx, ctx, pg_computed_column, std::move(keys), test_key_chunk(&fx.resource, out.oid), committed_scan);
            for (const auto& chunk : batches) {
                if (chunk.column_count() < 7)
                    continue;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    cc_row_t row;
                    row.attoid = chunk.data[1].is_null(i)
                                     ? catalog::INVALID_OID
                                     : static_cast<catalog::oid_t>(chunk.data[1].template get_value<std::uint32_t>(i));
                    if (!chunk.data[2].is_null(i)) {
                        row.attname = std::string(chunk.data[2].template get_value<std::string_view>(i));
                    }
                    row.atttypid = chunk.data[3].is_null(i)
                                       ? catalog::INVALID_OID
                                       : static_cast<catalog::oid_t>(chunk.data[3].template get_value<std::uint32_t>(i));
                    if (!chunk.data[4].is_null(i)) {
                        row.atttypspec = std::string(chunk.data[4].template get_value<std::string_view>(i));
                    }
                    row.attversion = chunk.data[5].template get_value<std::int64_t>(i);
                    row.attrefcount = chunk.data[6].is_null(i) ? 0 : chunk.data[6].template get_value<std::int64_t>(i);
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

        // pg_attribute: columns for attrelid == out.oid, with column-level MVCC visibility
        // (added_at / dropped_at vs ctx.txn.start_time) + attnum sort.
        {
            const auto snapshot_start_time = ctx.txn.start_time;
            std::vector<probe_column_info_t> rows;
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_attribute_col::attrelid);
            auto batches = probe_read(fx, ctx, pg_attribute, std::move(keys), test_key_chunk(&fx.resource, out.oid), committed_scan);
            for (const auto& chunk : batches) {
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    if (!chunk.data[7].is_null(i) && chunk.data[7].template get_value<bool>(i))
                        continue;
                    if (!chunk.data[10].is_null(i)) {
                        const auto added_at = static_cast<std::uint64_t>(chunk.data[10].template get_value<std::int64_t>(i));
                        if (added_at > snapshot_start_time)
                            continue;
                    }
                    if (!chunk.data[11].is_null(i)) {
                        const auto dropped_at = static_cast<std::uint64_t>(chunk.data[11].template get_value<std::int64_t>(i));
                        if (dropped_at != 0 && dropped_at <= snapshot_start_time)
                            continue;
                    }
                    probe_column_info_t info;
                    info.attoid = static_cast<catalog::oid_t>(chunk.data[0].template get_value<std::uint32_t>(i));
                    if (!chunk.data[2].is_null(i)) {
                        info.attname = std::string(chunk.data[2].template get_value<std::string_view>(i));
                    }
                    if (!chunk.data[3].is_null(i))
                        info.atttypid = static_cast<catalog::oid_t>(chunk.data[3].template get_value<std::uint32_t>(i));
                    info.attnum = chunk.data[4].template get_value<std::int32_t>(i);
                    info.attnotnull = !chunk.data[5].is_null(i) && chunk.data[5].template get_value<bool>(i);
                    info.atthasdefault = !chunk.data[6].is_null(i) && chunk.data[6].template get_value<bool>(i);
                    info.attisdropped = false;
                    if (!chunk.data[8].is_null(i)) {
                        info.atttypspec = std::string(chunk.data[8].template get_value<std::string_view>(i));
                    }
                    if (!chunk.data[9].is_null(i)) {
                        info.attdefspec = std::string(chunk.data[9].template get_value<std::string_view>(i));
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

    // pg_type layout: oid(0), typname(1), typnamespace(2), typdefspec(3).
    // Composite fallback: pg_class (relkind='c') by (relnamespace, relname), then pg_attribute fields
    // by attrelid, encoded as a STRUCT type spec.
    template<typename Fx>
    probe_type_result_t
    probe_type(Fx& fx, components::execution_context_t ctx, catalog::oid_t namespace_oid, std::string name) {
        probe_type_result_t out;
        out.namespace_oid = namespace_oid;

        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_class = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_attribute = catalog::well_known_oid::pg_attribute_table;

        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_type_col::typnamespace);
            keys.emplace_back(catalog::pg_type_col::typname);
            auto batches = probe_read(fx, ctx, pg_type, std::move(keys), test_key_chunk(&fx.resource, namespace_oid, std::string_view{name}));
            for (const auto& chunk : batches) {
                bool stop = false;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.data[0].is_null(i))
                        continue;
                    out.found = true;
                    out.oid = static_cast<catalog::oid_t>(chunk.data[0].template get_value<std::uint32_t>(i));
                    out.name = name;
                    if (!chunk.data[3].is_null(i)) {
                        out.typdefspec = std::string(chunk.data[3].template get_value<std::string_view>(i));
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

        catalog::oid_t composite_oid = catalog::INVALID_OID;
        {
            std::pmr::vector<std::uint64_t> keys{&fx.resource};
            keys.emplace_back(catalog::pg_class_col::relnamespace);
            keys.emplace_back(catalog::pg_class_col::relname);
            auto batches = probe_read(fx, ctx, pg_class, std::move(keys), test_key_chunk(&fx.resource, namespace_oid, std::string_view{name}));
            for (const auto& chunk : batches) {
                bool stop = false;
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    if (chunk.data[3].is_null(i))
                        continue;
                    const auto kind_s = chunk.data[3].template get_value<std::string_view>(i);
                    if (kind_s.empty() || kind_s.front() != catalog::relkind::composite_type)
                        continue;
                    composite_oid = static_cast<catalog::oid_t>(chunk.data[0].template get_value<std::uint32_t>(i));
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
            auto batches = probe_read(fx, ctx, pg_attribute, std::move(keys), test_key_chunk(&fx.resource, composite_oid));
            for (const auto& chunk : batches) {
                for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                    if (!chunk.data[7].is_null(i) && chunk.data[7].template get_value<bool>(i))
                        continue;
                    field_row r;
                    if (!chunk.data[2].is_null(i)) {
                        r.attname = std::string(chunk.data[2].template get_value<std::string_view>(i));
                    }
                    if (!chunk.data[3].is_null(i))
                        r.atttypid = static_cast<catalog::oid_t>(chunk.data[3].template get_value<std::uint32_t>(i));
                    r.attnum = chunk.data[4].template get_value<std::int32_t>(i);
                    if (!chunk.data[8].is_null(i)) {
                        r.atttypspec = std::string(chunk.data[8].template get_value<std::string_view>(i));
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
        auto batches = probe_read(
            fx, ctx, pg_proc, std::move(keys), test_key_chunk(&fx.resource, namespace_oid, std::string_view{name}));
        for (const auto& chunk : batches) {
            bool stop = false;
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.data[0].is_null(i))
                    continue;
                out.found = true;
                out.oid = static_cast<catalog::oid_t>(chunk.data[0].template get_value<std::uint32_t>(i));
                out.name = name;
                if (!chunk.data[3].is_null(i))
                    out.pronargs = chunk.data[3].template get_value<std::int32_t>(i);
                if (!chunk.data[4].is_null(i))
                    out.prouid = static_cast<std::uint64_t>(chunk.data[4].template get_value<std::int64_t>(i));
                if (!chunk.data[5].is_null(i)) {
                    out.proargmatchers = std::string(chunk.data[5].template get_value<std::string_view>(i));
                }
                if (!chunk.data[6].is_null(i)) {
                    out.prorettype = std::string(chunk.data[6].template get_value<std::string_view>(i));
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

#pragma once

// Internal translation-unit header: included by every manager_disk_*.cpp split file.
// Centralises includes and exposes the shared helpers that all TUs need.

#include "manager_disk.hpp"

#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <array>
#include <components/catalog/builtin_seed.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/fk_rules.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <fstream>
#include <limits>
#include <components/catalog/dependency_walker.hpp>
#include <services/wal/manager_wal_replicate.hpp>
#include <unordered_set>

namespace services::disk {

using data_chunk_t = components::vector::data_chunk_t;

namespace detail {

    // Namespace aliases brought in scope via "using namespace detail"
    namespace types   = components::types;
    namespace catalog = components::catalog;

    // Bring dependency_walker types into services::disk scope (via "using namespace detail").
    using components::catalog::dependency_t;
    using components::catalog::cycle_detected_error;
    using components::catalog::topological_drop_order;
    using components::catalog::fetch_deps_fn;
    namespace deptype = components::catalog::deptype;

    // ---------------------------------------------------------------------------
    // Logical value constructors (shortcuts for single-row pg_catalog writes)
    // ---------------------------------------------------------------------------

    inline components::types::logical_value_t
    lv_oid(std::pmr::memory_resource* r, components::catalog::oid_t v) {
        return components::types::logical_value_t(r, v);
    }

    inline components::types::logical_value_t
    lv_str(std::pmr::memory_resource* r, std::string v) {
        return components::types::logical_value_t(r, std::move(v));
    }

    inline components::types::logical_value_t
    lv_i32(std::pmr::memory_resource* r, std::int32_t v) {
        return components::types::logical_value_t(r, v);
    }

    inline components::types::logical_value_t
    lv_i64(std::pmr::memory_resource* r, std::int64_t v) {
        return components::types::logical_value_t(r, v);
    }

    inline components::types::logical_value_t
    lv_bool(std::pmr::memory_resource* r, bool v) {
        return components::types::logical_value_t(r, v);
    }

    // ---------------------------------------------------------------------------
    // make_row: allocate a single-row data_chunk_t and fill it via a lambda.
    // Usage: make_row(resource, def->columns, [&](data_chunk_t& chunk, auto* res) { ... })
    // ---------------------------------------------------------------------------

    template<typename Fn>
    components::vector::data_chunk_t make_row(
        std::pmr::memory_resource* resource,
        const std::vector<components::table::column_definition_t>& columns,
        Fn&& fn)
    {
        std::pmr::vector<components::types::complex_logical_type> types(resource);
        types.reserve(columns.size());
        for (const auto& col : columns) {
            types.push_back(col.type());
        }
        components::vector::data_chunk_t chunk(resource, types, 1);
        chunk.set_cardinality(1);
        fn(chunk, resource);
        return chunk;
    }

    // ---------------------------------------------------------------------------
    // str_equals: test whether a string-typed logical_value_t equals a std::string.
    // ---------------------------------------------------------------------------

    inline bool str_equals(const components::types::logical_value_t& v, const std::string& s) {
        if (v.is_null()) return false;
        return v.value<std::string_view>() == std::string_view(s);
    }

    // ---------------------------------------------------------------------------
    // inline_scan: scan all committed rows of a data_table_t, projecting the given
    // column indices.  Calls fn(chunk, row_index) for every row; returning false
    // from fn stops the scan early.
    // ---------------------------------------------------------------------------

    namespace detail_impl_ {
        template<typename Range, typename Fn>
        void inline_scan_range(components::table::data_table_t& table,
                                const Range& col_indices,
                                std::pmr::memory_resource* resource,
                                Fn&& fn)
        {
            std::vector<components::table::storage_index_t> col_ids;
            const auto& all_cols = table.columns();
            std::pmr::vector<components::types::complex_logical_type> col_types(resource);
            for (auto idx : col_indices) {
                col_ids.emplace_back(static_cast<uint64_t>(idx));
                col_types.push_back(all_cols[static_cast<std::size_t>(idx)].type());
            }

            components::table::table_scan_state state(resource);
            table.initialize_scan(state, col_ids);

            while (true) {
                components::vector::data_chunk_t chunk(resource, col_types);
                table.scan_committed(chunk, state);
                if (chunk.size() == 0) break;
                for (uint64_t i = 0; i < chunk.size(); ++i) {
                    if (!fn(chunk, i)) return;
                }
            }
        }
    } // namespace detail_impl_

    template<typename Fn>
    void inline_scan(components::table::data_table_t& table,
                      std::initializer_list<std::int64_t> col_indices,
                      std::pmr::memory_resource* resource,
                      Fn&& fn)
    {
        detail_impl_::inline_scan_range(table, col_indices, resource, std::forward<Fn>(fn));
    }

    template<typename Fn>
    void inline_scan(components::table::data_table_t& table,
                      const std::vector<std::int64_t>& col_indices,
                      std::pmr::memory_resource* resource,
                      Fn&& fn)
    {
        detail_impl_::inline_scan_range(table, col_indices, resource, std::forward<Fn>(fn));
    }

    // ---------------------------------------------------------------------------
    // make_ddl_result: build a ddl_result_t from a single invalidation event.
    // ---------------------------------------------------------------------------

    inline ddl_result_t make_ddl_result(std::pmr::memory_resource* resource,
                                         components::catalog::oid_t oid,
                                         invalidation_kind kind,
                                         components::catalog::oid_t parent_oid,
                                         std::uint64_t version)
    {
        ddl_result_t r(resource);
        r.result = components::cursor::make_cursor(resource,
                                                    components::cursor::operation_status_t::success);
        r.created_oid = oid;
        r.new_catalog_version = version;
        r.events.push_back(invalidation_event_t{version, kind, oid, parent_oid});
        return r;
    }

    // ---------------------------------------------------------------------------
    // rebuild_chunk: copy a data_chunk_t into a new one backed by `resource`.
    // Ensures WAL-replay chunks created with a foreign allocator are safe to
    // pass to table.append() which requires a consistent memory resource.
    // ---------------------------------------------------------------------------

    inline components::vector::data_chunk_t rebuild_chunk(
        std::pmr::memory_resource* resource,
        components::vector::data_chunk_t& data)
    {
        auto types = data.types();
        const uint64_t n = data.size();
        const uint64_t cap = n > 0 ? n : 1;
        components::vector::data_chunk_t local(resource, types, cap);
        local.set_cardinality(0);
        if (n > 0) {
            local.append(data, true);
        }
        return local;
    }

    // ---------------------------------------------------------------------------
    // Catalog function aliases (used unqualified via "using namespace detail")
    // ---------------------------------------------------------------------------

    using components::catalog::encode_type_spec;
    using components::catalog::decode_type_spec;
    using components::catalog::logical_type_to_pg_name;
    using components::catalog::oid_to_builtin_type;

    // ---------------------------------------------------------------------------
    // pg_catalog.main collection name constants
    // ---------------------------------------------------------------------------

    inline const collection_full_name_t pg_database_name        {"pg_catalog", "main", "pg_database"};
    inline const collection_full_name_t pg_namespace_name       {"pg_catalog", "main", "pg_namespace"};
    inline const collection_full_name_t pg_class_name           {"pg_catalog", "main", "pg_class"};
    inline const collection_full_name_t pg_attribute_name       {"pg_catalog", "main", "pg_attribute"};
    inline const collection_full_name_t pg_type_name            {"pg_catalog", "main", "pg_type"};
    inline const collection_full_name_t pg_depend_name          {"pg_catalog", "main", "pg_depend"};
    inline const collection_full_name_t pg_index_name           {"pg_catalog", "main", "pg_index"};
    inline const collection_full_name_t pg_proc_name            {"pg_catalog", "main", "pg_proc"};
    inline const collection_full_name_t pg_constraint_name      {"pg_catalog", "main", "pg_constraint"};
    inline const collection_full_name_t pg_sequence_name        {"pg_catalog", "main", "pg_sequence"};
    inline const collection_full_name_t pg_rewrite_name         {"pg_catalog", "main", "pg_rewrite"};
    inline const collection_full_name_t pg_computed_column_name {"pg_catalog", "main", "pg_computed_column"};

} // namespace detail

} // namespace services::disk
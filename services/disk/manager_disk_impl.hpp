#pragma once

// Internal translation-unit header: included by every manager_disk_*.cpp split file.
// Centralises includes and exposes the shared helpers that all TUs need.

#include "manager_disk.hpp"
#include "inline_scan.hpp" // services::disk::detail::inline_scan (shared with agent_disk)

#include <actor-zeta/spawn.hpp>
#include <algorithm>
#include <array>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/dependency_walker.hpp> // deptype namespace
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <fstream>
#include <limits>
#include <services/wal/manager_wal_replicate.hpp>
#include <unordered_set>

namespace services::disk {

    using data_chunk_t = components::vector::data_chunk_t;

    namespace detail {

        // Namespace aliases brought in scope via "using namespace detail"
        namespace types = components::types;
        namespace catalog = components::catalog;

        // deptype helpers used by ddl_drop_type/constraint/function RESTRICT checks.
        namespace deptype = components::catalog::deptype;

        // ---------------------------------------------------------------------------
        // Logical value constructors (shortcuts for single-row pg_catalog writes)
        // ---------------------------------------------------------------------------

        inline components::types::logical_value_t lv_oid(std::pmr::memory_resource* r, components::catalog::oid_t v) {
            return components::types::logical_value_t(r, v);
        }

        inline components::types::logical_value_t lv_str(std::pmr::memory_resource* r, std::string v) {
            return components::types::logical_value_t(r, std::move(v));
        }

        // ---------------------------------------------------------------------------
        // make_row: allocate a single-row data_chunk_t and fill it via a lambda.
        // Usage: make_row(resource, def->columns, [&](data_chunk_t& chunk, auto* res) { ... })
        // ---------------------------------------------------------------------------

        template<typename Fn>
        components::vector::data_chunk_t make_row(std::pmr::memory_resource* resource,
                                                  const std::vector<components::table::column_definition_t>& columns,
                                                  Fn&& fn) {
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
            if (v.is_null())
                return false;
            return v.value<std::string_view>() == std::string_view(s);
        }

        // inline_scan(...) lives in services/disk/inline_scan.hpp (shared with agent_disk);
        // it is in namespace services::disk::detail, so "using namespace detail" exposes it here.

        // ---------------------------------------------------------------------------
        // rebuild_chunk: copy a data_chunk_t into a new one backed by `resource`.
        // Ensures WAL-replay chunks created with a foreign allocator are safe to
        // pass to table.append() which requires a consistent memory resource.
        // ---------------------------------------------------------------------------

        inline components::vector::data_chunk_t rebuild_chunk(std::pmr::memory_resource* resource,
                                                              components::vector::data_chunk_t& data) {
            auto types = data.types();
            const uint64_t n = data.size();
            const uint64_t cap = n > 0 ? n : 1;
            components::vector::data_chunk_t local(resource, types, cap);
            local.set_cardinality(0);
            if (n > 0) {
                data.copy(local, 0);
            }
            return local;
        }

        // ---------------------------------------------------------------------------
        // Catalog function aliases (used unqualified via "using namespace detail")
        // ---------------------------------------------------------------------------

        using components::catalog::decode_type_spec;
        using components::catalog::encode_type_spec;
        using components::catalog::logical_type_to_pg_name;
        using components::catalog::oid_to_builtin_type;

        // ---------------------------------------------------------------------------
        // pg_catalog system table OIDs (well-known) — internal aliases.
        // ---------------------------------------------------------------------------

        namespace wk = components::catalog::well_known_oid;
        inline constexpr components::catalog::oid_t pg_database_oid = wk::pg_database_table;
        inline constexpr components::catalog::oid_t pg_namespace_oid_tbl = wk::pg_namespace_table;
        inline constexpr components::catalog::oid_t pg_class_oid = wk::pg_class_table;
        inline constexpr components::catalog::oid_t pg_attribute_oid = wk::pg_attribute_table;
        inline constexpr components::catalog::oid_t pg_type_oid = wk::pg_type_table;
        inline constexpr components::catalog::oid_t pg_depend_oid = wk::pg_depend_table;
        inline constexpr components::catalog::oid_t pg_index_oid = wk::pg_index_table;
        inline constexpr components::catalog::oid_t pg_proc_oid = wk::pg_proc_table;
        inline constexpr components::catalog::oid_t pg_constraint_oid = wk::pg_constraint_table;
        inline constexpr components::catalog::oid_t pg_sequence_oid = wk::pg_sequence_table;
        inline constexpr components::catalog::oid_t pg_rewrite_oid = wk::pg_rewrite_table;
        inline constexpr components::catalog::oid_t pg_computed_column_oid = wk::pg_computed_column_table;

    } // namespace detail

} // namespace services::disk
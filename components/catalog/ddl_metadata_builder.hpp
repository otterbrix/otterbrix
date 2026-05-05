#pragma once

// Free functions for building pg_catalog rows from DDL parameters.
// All functions are pure: no I/O, no actor deps, no state. Row construction
// uses find_system_table() for column layouts (same as disk does today).
//
// Callers (planner, Etap 3.5) are responsible for:
//  - allocating OIDs before calling (via disk.allocate_oids)
//  - resolving atttypid per column (via catalog_view pg_type snapshot)
//  - calling encode_type_spec / encode_default_spec for complex columns

#include <components/catalog/catalog_oids.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::catalog {

    // Parameters for building a pg_class row.
    struct pg_class_build_t {
        oid_t       table_oid{INVALID_OID};
        oid_t       namespace_oid{INVALID_OID};
        std::string relname;
        char        relkind{'r'};
        char        relstoragemode{'d'};
    };

    // Parameters for a single pg_attribute row.
    struct pg_attribute_build_t {
        oid_t        attoid{INVALID_OID};
        oid_t        attrelid{INVALID_OID};
        std::string  attname;
        oid_t        atttypid{INVALID_OID};
        std::int32_t attnum{0};   // 1-based ordinal
        bool         attnotnull{false};
        bool         atthasdefault{false};
        std::string  atttypspec;  // encode_type_spec(col.type()); empty for builtins
        std::string  attdefspec;  // encode_default_spec(default); empty when no default
    };

    // Parameters for a pg_constraint row.
    struct pg_constraint_build_t {
        oid_t       con_oid{INVALID_OID};
        oid_t       conrelid{INVALID_OID};
        std::string conname;
        char        contype{'\0'};     // 'p','f','u','c'
        oid_t       confrelid{INVALID_OID};
        std::string conkey;            // CSV of attoids on this side
        std::string confkey;           // CSV of attoids on referenced side (FK only)
        char        matchtype{'s'};
        char        deltype{'a'};
        char        updtype{'a'};
        std::string conexpr;           // CHECK SQL text; empty if not CHECK
    };

    // Build a single-row data_chunk_t for pg_class.
    vector::data_chunk_t build_pg_class_row(
        std::pmr::memory_resource*  resource,
        const pg_class_build_t&     p);

    // Build one single-row data_chunk_t per attribute.
    std::vector<vector::data_chunk_t> build_pg_attribute_rows(
        std::pmr::memory_resource*                  resource,
        const std::vector<pg_attribute_build_t>&    attrs);

    // Build a single-row data_chunk_t for pg_constraint.
    vector::data_chunk_t build_pg_constraint_row(
        std::pmr::memory_resource*  resource,
        const pg_constraint_build_t& p);

    // Build a single-row pg_depend chunk linking (classid,objid) → (refclassid,refobjid).
    vector::data_chunk_t build_pg_depend_row(
        std::pmr::memory_resource* resource,
        oid_t classid,    oid_t objid,
        oid_t refclassid, oid_t refobjid,
        char  deptype,
        std::int32_t objsubid    = 0,
        std::int32_t refobjsubid = 0);

    // Build a single-row pg_namespace chunk.
    vector::data_chunk_t build_pg_namespace_row(
        std::pmr::memory_resource* resource,
        oid_t       ns_oid,
        const std::string& nspname);

} // namespace components::catalog

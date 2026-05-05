#pragma once

// Typed row structs and decode helpers for pg_catalog system tables.
// Column offsets track the layouts in system_table_schemas.cpp.
// All decode_* functions return a fully-populated struct; nullable fields
// get a zero/empty default when the cell is NULL.

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>

#include <cstdint>
#include <string>

namespace components::catalog {

    // -----------------------------------------------------------------------
    // pg_database  (col 0: oid, 1: datname)
    // -----------------------------------------------------------------------
    struct pg_database_row {
        oid_t       oid{INVALID_OID};
        std::string datname;
    };
    pg_database_row decode_pg_database(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_namespace  (col 0: oid, 1: nspname)
    // -----------------------------------------------------------------------
    struct pg_namespace_row {
        oid_t       oid{INVALID_OID};
        std::string nspname;
    };
    pg_namespace_row decode_pg_namespace(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_class  (col 0: oid, 1: relname, 2: relnamespace, 3: relkind, 4: relstoragemode)
    // -----------------------------------------------------------------------
    struct pg_class_row {
        oid_t       oid{INVALID_OID};
        std::string relname;
        oid_t       relnamespace{INVALID_OID};
        char        relkind{'\0'};
        char        relstoragemode{'\0'};
    };
    pg_class_row decode_pg_class(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_attribute  (col 0–9: attoid, attrelid, attname, atttypid, attnum,
    //                attnotnull, atthasdefault, attisdropped, atttypspec, attdefspec)
    // -----------------------------------------------------------------------
    struct pg_attribute_row {
        oid_t        attoid{INVALID_OID};
        oid_t        attrelid{INVALID_OID};
        std::string  attname;
        oid_t        atttypid{INVALID_OID};
        std::int32_t attnum{0};
        bool         attnotnull{false};
        bool         atthasdefault{false};
        bool         attisdropped{false};
        std::string  atttypspec;   // empty → scalar type reconstructed from atttypid
        std::string  attdefspec;   // empty → no default
    };
    pg_attribute_row decode_pg_attribute(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_type  (col 0: oid, 1: typname, 2: typnamespace, 3: typdefspec)
    // -----------------------------------------------------------------------
    struct pg_type_row {
        oid_t       oid{INVALID_OID};
        std::string typname;
        oid_t       typnamespace{INVALID_OID};
        std::string typdefspec;   // empty for built-in scalar types
    };
    pg_type_row decode_pg_type(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_proc  (col 0: oid, 1: proname, 2: pronamespace, 3: pronargs,
    //           4: prouid, 5: proargmatchers, 6: prorettype)
    // -----------------------------------------------------------------------
    struct pg_proc_row {
        oid_t        oid{INVALID_OID};
        std::string  proname;
        oid_t        pronamespace{INVALID_OID};
        std::int32_t pronargs{0};
        std::int64_t prouid{0};
        std::string  proargmatchers;
        std::string  prorettype;
    };
    pg_proc_row decode_pg_proc(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_depend  (col 0: classid, 1: objid, 2: refclassid, 3: refobjid,
    //             4: deptype, 5: objsubid, 6: refobjsubid)
    // -----------------------------------------------------------------------
    struct pg_depend_row {
        oid_t        classid{INVALID_OID};
        oid_t        objid{INVALID_OID};
        oid_t        refclassid{INVALID_OID};
        oid_t        refobjid{INVALID_OID};
        char         deptype{'\0'};   // 'n','a','i','p'
        std::int32_t objsubid{0};
        std::int32_t refobjsubid{0};
    };
    pg_depend_row decode_pg_depend(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_constraint  (col 0: oid, 1: conname, 2: conrelid, 3: contype,
    //                  4: confrelid, 5: conkey, 6: confkey, 7: confmatchtype,
    //                  8: confdeltype, 9: confupdtype, 10: conexpr)
    // -----------------------------------------------------------------------
    struct pg_constraint_row {
        oid_t       oid{INVALID_OID};
        std::string conname;
        oid_t       conrelid{INVALID_OID};
        char        contype{'\0'};          // 'p','f','u','c','n'
        oid_t       confrelid{INVALID_OID}; // 0 if not FK
        std::string conkey;                 // CSV of attoids on this side
        std::string confkey;                // CSV of attoids on referenced side
        char        matchtype{'s'};         // 's' SIMPLE, 'f' FULL, 'p' PARTIAL
        char        deltype{'a'};           // 'a' NO ACTION, 'r' RESTRICT, 'c' CASCADE, 'n' SET NULL, 'd' SET DEFAULT
        char        updtype{'a'};
        std::string conexpr;                // CHECK SQL text; empty if not CHECK
    };
    pg_constraint_row decode_pg_constraint(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_index  (col 0: indexrelid, 1: indrelid, 2: indkey, 3: indisvalid)
    // -----------------------------------------------------------------------
    struct pg_index_row {
        oid_t       indexrelid{INVALID_OID};
        oid_t       indrelid{INVALID_OID};
        std::string indkey;       // CSV of attoids
        bool        indisvalid{false};
    };
    pg_index_row decode_pg_index(const vector::data_chunk_t& chunk, std::uint64_t row);

    // -----------------------------------------------------------------------
    // pg_sequence  (col 0: seqrelid, 1: seqstart, 2: seqincrement, 3: seqmin,
    //               4: seqmax, 5: seqcycle, 6: seqlast)
    // -----------------------------------------------------------------------
    struct pg_sequence_row {
        oid_t        seqrelid{INVALID_OID};
        std::int64_t seqstart{1};
        std::int64_t seqincrement{1};
        std::int64_t seqmin{1};
        std::int64_t seqmax{std::int64_t{9223372036854775807LL}};
        bool         seqcycle{false};
        std::int64_t seqlast{0};
    };
    pg_sequence_row decode_pg_sequence(const vector::data_chunk_t& chunk, std::uint64_t row);

} // namespace components::catalog

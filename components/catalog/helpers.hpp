#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/types/logical_value.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::catalog {

    // Parse a comma-separated string of OID integers (e.g. pg_constraint.conkey / confkey).
    // Skips malformed tokens. Returns empty vector for empty input.
    std::vector<oid_t> parse_oid_csv(const std::string& s);

    // Encode a vector of OIDs as a comma-separated string — the inverse of parse_oid_csv.
    // Used when writing pg_constraint.conkey / confkey rows to pg_catalog.
    std::string encode_oid_csv(const std::vector<oid_t>& oids);

    // Column-index constants for system tables. Mirror the column order in
    // components/catalog/system_table_schemas.cpp (pg_*_columns() functions).
    // Centralised here so FK / CHECK readers don't redefine them per file.
    //
    // These are the identity of a system-table column: the keyed catalog reads take
    // them directly, so a name never crosses the mailbox. That is only safe because
    // catalog::system_schemas::column_order_is_pinned asserts (position, name) for
    // every table listed here — without that test, inserting a column in the middle
    // of a schema would silently shift every constant below it.
    namespace pg_constraint_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t conname = 1;
        constexpr std::uint64_t conrelid = 2;
        constexpr std::uint64_t contype = 3;
        constexpr std::uint64_t confrelid = 4;
        constexpr std::uint64_t conkey = 5;
        constexpr std::uint64_t confkey = 6;
        constexpr std::uint64_t confmatchtype = 7;
        constexpr std::uint64_t confdeltype = 8;
        constexpr std::uint64_t confupdtype = 9;
        constexpr std::uint64_t conexpr = 10;
    } // namespace pg_constraint_col
    namespace pg_attribute_col {
        constexpr std::uint64_t attoid = 0;
        constexpr std::uint64_t attrelid = 1;
        constexpr std::uint64_t attname = 2;
        constexpr std::uint64_t atttypid = 3;
        constexpr std::uint64_t attnum = 4;
        constexpr std::uint64_t attnotnull = 5;
        constexpr std::uint64_t atthasdefault = 6;
        constexpr std::uint64_t attisdropped = 7;
        constexpr std::uint64_t atttypspec = 8;
        constexpr std::uint64_t attdefspec = 9;
        constexpr std::uint64_t added_at_commit_id = 10;
        constexpr std::uint64_t dropped_at_commit_id = 11;
    } // namespace pg_attribute_col
    namespace pg_class_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t relname = 1;
        constexpr std::uint64_t relnamespace = 2;
        constexpr std::uint64_t relkind = 3;
        constexpr std::uint64_t relstoragemode = 4;
    } // namespace pg_class_col
    namespace pg_namespace_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t nspname = 1;
    } // namespace pg_namespace_col
    namespace pg_index_col {
        constexpr std::uint64_t indexrelid = 0;
        constexpr std::uint64_t indrelid = 1;
        constexpr std::uint64_t indkey = 2;
        constexpr std::uint64_t indisvalid = 3;
    } // namespace pg_index_col
    namespace pg_computed_column_col {
        constexpr std::uint64_t relid = 0;
        constexpr std::uint64_t attoid = 1;
        constexpr std::uint64_t attname = 2;
        constexpr std::uint64_t atttypid = 3;
        constexpr std::uint64_t atttypspec = 4;
        constexpr std::uint64_t attversion = 5;
        constexpr std::uint64_t attrefcount = 6;
    } // namespace pg_computed_column_col
    namespace pg_type_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t typname = 1;
        constexpr std::uint64_t typnamespace = 2;
        constexpr std::uint64_t typdefspec = 3;
    } // namespace pg_type_col
    namespace pg_database_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t datname = 1;
    } // namespace pg_database_col
    namespace pg_rewrite_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t rulename = 1;
        constexpr std::uint64_t ev_class = 2;
        constexpr std::uint64_t ev_type = 3;
        constexpr std::uint64_t ev_action = 4;
    } // namespace pg_rewrite_col
    namespace pg_depend_col {
        constexpr std::uint64_t classid = 0;
        constexpr std::uint64_t objid = 1;
        constexpr std::uint64_t refclassid = 2;
        constexpr std::uint64_t refobjid = 3;
        constexpr std::uint64_t deptype = 4;
    } // namespace pg_depend_col
    namespace pg_proc_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t proname = 1;
        constexpr std::uint64_t pronamespace = 2;
        constexpr std::uint64_t pronargs = 3;
        constexpr std::uint64_t prouid = 4;
    } // namespace pg_proc_col
    namespace pg_sequence_col {
        constexpr std::uint64_t seqrelid = 0;
        constexpr std::uint64_t seqstart = 1;
        constexpr std::uint64_t seqincrement = 2;
        constexpr std::uint64_t seqmin = 3;
        constexpr std::uint64_t seqmax = 4;
        constexpr std::uint64_t seqcycle = 5;
        constexpr std::uint64_t seqlast = 6;
    } // namespace pg_sequence_col

} // namespace components::catalog

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
    namespace pg_constraint_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t conname = 1;
        constexpr std::uint64_t conrelid = 2;
        constexpr std::uint64_t contype = 3;
        constexpr std::uint64_t confrelid = 4;
        constexpr std::uint64_t conkey = 5;
        constexpr std::uint64_t confkey = 6;
        constexpr std::uint64_t confmatch = 7;
        constexpr std::uint64_t confdeltype = 8;
        constexpr std::uint64_t confupdtype = 9;
        constexpr std::uint64_t conexpr = 10;
    } // namespace pg_constraint_col
    namespace pg_attribute_col {
        constexpr std::uint64_t attoid = 0;
        constexpr std::uint64_t attrelid = 1;
        constexpr std::uint64_t attname = 2;
    } // namespace pg_attribute_col
    namespace pg_class_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t relname = 1;
        constexpr std::uint64_t relnamespace = 2;
    } // namespace pg_class_col
    namespace pg_namespace_col {
        constexpr std::uint64_t oid = 0;
        constexpr std::uint64_t nspname = 1;
    } // namespace pg_namespace_col

} // namespace components::catalog

#pragma once

#include "catalog_oids.hpp"

#include <cstddef>
#include <limits>
#include <string>
#include <vector>

namespace components::catalog {

    // FK constraint descriptor resolved at plan-enrich time. Carries enough
    // information for operator_fk_check_t / operator_fk_cascade_t to enforce the
    // constraint at execution time without further catalog access.
    // Field order: vectors → strings → oid_t → char flags, to minimise padding.
    struct fk_info_t {
        // Column names in child table to extract from INSERT/UPDATE chunk.
        std::vector<std::string> child_col_names;
        // Corresponding column names in parent table for existence check.
        std::vector<std::string> parent_col_names;
        // Positions of the key columns among the table's live columns in attnum order
        std::vector<std::size_t> child_col_indices;
        std::vector<std::size_t> parent_col_indices;
        // attdefspec strings for each FK column (parallel to child_col_names).
        // Empty string = column has no default (NULL will be used for SET DEFAULT).
        std::vector<std::string> child_col_default_specs;

        // Resolved at enrich time: child table's {database, schema, collection}.
        // Used by operator_fk_cascade_t to call storage_delete_rows / storage_update
        // without a round-trip back to disk for the name resolution.
        std::string child_database;
        std::string child_schema;
        std::string child_collection_name;

        oid_t constraint_oid{INVALID_OID};
        oid_t child_table_oid{INVALID_OID};  // conrelid
        oid_t parent_table_oid{INVALID_OID}; // confrelid

        char matchtype{'s'};  // confmatchtype: 's' SIMPLE, 'f' FULL, 'p' PARTIAL
        char del_action{'a'}; // confdeltype: 'a' NO ACTION, 'r' RESTRICT, 'c' CASCADE, ...
        char upd_action{'a'}; // confupdtype: same alphabet as del_action
    };
    // Layout guard: libstdc++ (string==32) → 256; libc++ (string==24) → 232. Saves a cacheline.
    static_assert(sizeof(fk_info_t) <= 256, "fk_info_t layout regression");

} // namespace components::catalog
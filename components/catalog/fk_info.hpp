#pragma once

#include "catalog_oids.hpp"

#include <string>
#include <vector>

namespace components::catalog {

    // FK constraint descriptor resolved at plan-enrich time. Carries enough
    // information for operator_fk_check_t / operator_fk_cascade_t to enforce the
    // constraint at execution time without further catalog access.
    struct fk_info_t {
        oid_t constraint_oid{INVALID_OID};
        oid_t child_table_oid{INVALID_OID};   // conrelid
        oid_t parent_table_oid{INVALID_OID};  // confrelid
        // Column names in child table to extract from INSERT/UPDATE chunk.
        std::vector<std::string> child_col_names;
        // Corresponding column names in parent table for existence check.
        std::vector<std::string> parent_col_names;
        char matchtype{'s'};    // confmatchtype: 's' SIMPLE, 'f' FULL, 'p' PARTIAL
        char del_action{'a'};   // confdeltype: 'a' NO ACTION, 'r' RESTRICT, 'c' CASCADE, ...
        char upd_action{'a'};   // confupdtype: same alphabet as del_action
    };

} // namespace components::catalog
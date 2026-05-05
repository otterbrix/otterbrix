#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>

#include <string>
#include <vector>

namespace components::catalog {

    // FK constraint metadata resolved from pg_constraint at plan time.
    // Carried by node_fk_check (outgoing FK — INSERT/UPDATE enforcer)
    // and node_fk_cascade (referencing FK — DELETE/UPDATE cascade side).
    struct resolved_fk_t {
        oid_t       con_oid{INVALID_OID};    // pg_constraint.oid
        oid_t       conrelid{INVALID_OID};   // referencing table OID (this side)
        oid_t       confrelid{INVALID_OID};  // referenced table OID (parent side)
        std::vector<oid_t> conkey;            // referencing col attoids
        std::vector<oid_t> confkey;           // referenced col attoids
        char        matchtype{'s'};           // MATCH SIMPLE/FULL/PARTIAL
        char        deltype{'a'};             // ON DELETE action
        char        updtype{'a'};             // ON UPDATE action
        std::string conname;                  // for error messages
    };

} // namespace components::catalog

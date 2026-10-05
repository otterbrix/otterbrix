#pragma once

#include "catalog_oids.hpp"

#include <components/base/identifier_types.hpp>

#include <string>

namespace components::catalog {

    namespace view_refkind {
        inline constexpr char relation = 'r';  // a relation, by pg_class oid
        inline constexpr char host_name = 'h'; // a name the host resolved; no catalog oid
        inline constexpr char function = 'f';  // a function by pg_proc oid, its signature in refspec
    } // namespace view_refkind

    // One pg_rewrite_ref row: a view body name as written and what CREATE VIEW bound it to.
    struct view_binding_t {
        char refkind{view_refkind::relation};
        core::dbname_t dbname;
        core::schema_t schema;
        core::relname_t relname;
        oid_t refobjid{INVALID_OID};
        std::string refspec;
    };

    // A pg_depend 'n' edge from a view to (refclassid, refobjid).
    struct view_dependency_t {
        oid_t refclassid{INVALID_OID};
        oid_t refobjid{INVALID_OID};
    };

} // namespace components::catalog

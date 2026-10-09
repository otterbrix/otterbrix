#pragma once

#include "catalog_oids.hpp"

#include <components/base/identifier_types.hpp>

#include <string>

namespace components::catalog {

    // pg_rewrite_ref.refkind
    enum class view_refkind : char
    {
        relation = 'r',  // a relation, by pg_class oid
        host_name = 'h', // a name the host resolved; no catalog oid
        function = 'f'   // a function by pg_proc oid and the signature of that row
    };

    // One pg_rewrite_ref row: a view body name as written and what CREATE VIEW bound it to.
    struct view_binding_t {
        view_refkind refkind{view_refkind::relation};
        core::dbname_t dbname;
        core::schema_t schema;
        core::relname_t relname;
        oid_t refobjid{INVALID_OID};
        // 'f' rows only: the pg_proc row's signature the function was bound to.
        std::string proargmatchers;
        std::string prorettype;
    };

    // A pg_depend 'n' edge from a view to (refclassid, refobjid).
    struct view_dependency_t {
        oid_t refclassid{INVALID_OID};
        oid_t refobjid{INVALID_OID};
    };

} // namespace components::catalog

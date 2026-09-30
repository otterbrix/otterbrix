#pragma once

#include "catalog_oids.hpp"

#include <string>

namespace components::catalog {

    namespace view_refkind {
        inline constexpr char relation = 'r'; // a relation, by pg_class oid
    } // namespace view_refkind

    // One pg_rewrite_ref row: a view body name as written and what CREATE VIEW bound it to.
    struct view_binding_t {
        char refkind{view_refkind::relation};
        std::string dbname;
        std::string schema;
        std::string relname;
        oid_t refobjid{INVALID_OID};
        std::string refspec;
    };

    // A pg_depend 'n' edge from a view to (refclassid, refobjid).
    struct view_dependency_t {
        oid_t refclassid{INVALID_OID};
        oid_t refobjid{INVALID_OID};
    };

} // namespace components::catalog

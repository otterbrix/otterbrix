#include "catalog_oids.hpp"

// All declarations in catalog_oids.hpp are inline / constexpr / templated.
// This translation unit exists so the symbol table and well-known OIDs have a
// single point of definition for ODR purposes if the header is later extended
// with non-inline functions, and to give CMake a concrete object file to compile.

namespace components::catalog {

    // ODR anchor: forces the well-known OID constants to be addressable through this
    // translation unit. (constexpr inline already gives them external linkage; this is
    // a defensive no-op for tooling that scans for symbol presence.)
    static_assert(INVALID_OID == 0, "INVALID_OID must be 0 (PostgreSQL convention)");
    static_assert(FIRST_USER_OID > 0, "FIRST_USER_OID must be positive");

} // namespace components::catalog

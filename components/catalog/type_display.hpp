#pragma once

#include <components/types/types.hpp>

#include <string>

namespace components::catalog {

    // Type as it should read in a user-facing message: the declared name for a named
    // type (ENUM / STRUCT / UDT), the pg_type name for everything else.
    std::string describe_type(const types::complex_logical_type& type);

    // Type as PostgreSQL spells it (integer, bigint, double precision ...); numeric prints its (p,s) and a
    // fixed array its length like format_type_with_typemod, or bare with with_typmod=false. Types PostgreSQL does
    // not have keep their describe_type name.
    std::string format_type_be(const types::complex_logical_type& type, bool with_typmod = true);

} // namespace components::catalog

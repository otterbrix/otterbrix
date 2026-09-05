#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace otterbrix {

    // The one query that enumerates user tables from the engine catalog.
    // Kept next to its decoder so the two can never drift apart, and so the C++
    // test suite can pin the exact string the Python binding sends.
    inline constexpr std::string_view kListTablesQuery = "SELECT oid, relname, relkind FROM pg_class;";

    // Decode a `kListTablesQuery` cursor into the names of USER tables.
    //
    // pg_class layout: [0=oid, 1=relname, 2=relnamespace, 3=relkind, 4=relstoragemode].
    // Two filters pick user tables out of the projection: user objects have
    // oid >= FIRST_USER_OID (system catalog rows sit below that), and only regular
    // relations (relkind 'r') are tables.
    //
    // A failed query is NOT an empty database (rule 6): the error is returned as an
    // error, never flattened into an empty name list. An empty list therefore means
    // exactly one thing — the catalog was read and holds no user tables.
    core::result_wrapper_t<std::pmr::vector<std::pmr::string>>
    user_table_names_from_pg_class(std::pmr::memory_resource* resource,
                                   const components::cursor::cursor_t_ptr& cursor);

} // namespace otterbrix

#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace otterbrix {

    // Enumerates user tables from the engine catalog. Kept beside its decoder so
    // the two can't drift apart, and so the C++ suite can pin the exact string
    // the Python binding sends.
    inline constexpr std::string_view kListTablesQuery = "SELECT oid, relname, relkind FROM pg_class;";

    // Decode a `kListTablesQuery` cursor into the names of USER tables.
    // Filters: oid >= FIRST_USER_OID excludes system catalog rows, and only
    // relkind == 'r' (regular) rows are tables.
    //
    // A failed query is not an empty database: errors propagate as errors,
    // never flattened into an empty list — an empty list means the read succeeded
    // with zero user tables.
    core::result_wrapper_t<std::pmr::vector<std::pmr::string>>
    user_table_names_from_pg_class(std::pmr::memory_resource* resource,
                                   const components::cursor::cursor_t_ptr& cursor);

} // namespace otterbrix

#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>

namespace otterbrix {

    // Kept beside its decoder so the two cannot drift, and so the C++ suite can pin the exact query string.
    inline constexpr std::string_view kListTablesQuery = "SELECT oid, relname, relkind FROM pg_class;";

    // USER tables: oid >= FIRST_USER_OID and relkind == 'r'. A failed query is an error, never an empty list.
    core::result_wrapper_t<std::pmr::vector<std::pmr::string>>
    user_table_names_from_pg_class(std::pmr::memory_resource* resource,
                                   const components::cursor::cursor_t_ptr& cursor);

} // namespace otterbrix

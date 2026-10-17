#include "catalog_listing.hpp"

namespace otterbrix {

    core::result_wrapper_t<std::pmr::vector<std::pmr::string>>
    user_table_names_from_pg_class(std::pmr::memory_resource* resource,
                                   const components::cursor::cursor_t_ptr& cursor) {
        // No cursor means the dispatcher answered nothing at all — an engine fault,
        // not a catalog with no tables in it.
        if (!cursor) {
            return core::error_t{core::error_code_t::physical_plan_error,
                                 std::pmr::string{"listTables: the catalog query returned no cursor", resource}};
        }
        // Propagate the engine's own error; collapsing it to an empty list
        // would hide a broken read as an empty catalog.
        if (cursor->is_error()) {
            return cursor->get_error();
        }

        std::pmr::vector<std::pmr::string> names{resource};
        if (cursor->size() == 0) {
            return names; // read succeeded; the catalog holds no user tables
        }

        // Resolve the projected column positions by alias, falling back to the
        // SELECT order in kListTablesQuery if the cursor carries no aliases.
        const auto& types = cursor->type_data();
        uint64_t oid_col = 0;
        uint64_t relname_col = 1;
        uint64_t relkind_col = 2;
        for (std::size_t i = 0; i < types.size(); ++i) {
            if (!types[i].has_alias()) {
                continue;
            }
            const auto& alias = types[i].alias();
            if (alias == "oid") {
                oid_col = i;
            } else if (alias == "relname") {
                relname_col = i;
            } else if (alias == "relkind") {
                relkind_col = i;
            }
        }

        // Rows by index: the caller's cursor position stays where it was.
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            VALUE_OR_RETURN(const bool oid_is_null, cursor->is_null(oid_col, row));
            if (oid_is_null) {
                continue; // system catalog object
            }
            VALUE_OR_RETURN(const auto oid_value, cursor->get<std::uint32_t>(oid_col, row));
            if (oid_value < components::catalog::FIRST_USER_OID) {
                continue; // system catalog object
            }
            // relname and relkind are NOT NULL in the schema (system_table_schemas.cpp): a
            // NULL/empty value here is a corrupt row, not one to filter. Unchecked, a NULL
            // relkind reads as a regular table and a NULL relname silently drops a real one.
            // A catalog that cannot be trusted refuses.
            VALUE_OR_RETURN(const bool relkind_is_null, cursor->is_null(relkind_col, row));
            if (relkind_is_null) {
                return core::error_t{core::error_code_t::schema_error,
                                     std::pmr::string{"listTables: pg_class row oid=" + std::to_string(oid_value) +
                                                          " has NULL relkind (declared NOT NULL) — the catalog "
                                                          "cannot be trusted",
                                                      resource}};
            }
            VALUE_OR_RETURN(const auto relkind, cursor->get<std::string_view>(relkind_col, row));
            if (relkind.empty()) {
                return core::error_t{core::error_code_t::schema_error,
                                     std::pmr::string{"listTables: pg_class row oid=" + std::to_string(oid_value) +
                                                          " has an EMPTY relkind (declared NOT NULL) — the catalog "
                                                          "cannot be trusted",
                                                      resource}};
            }
            if (relkind.front() != components::catalog::relkind::regular) {
                continue; // not a regular table (view / matview / sequence / index / ...)
            }
            VALUE_OR_RETURN(const bool relname_is_null, cursor->is_null(relname_col, row));
            if (relname_is_null) {
                return core::error_t{core::error_code_t::schema_error,
                                     std::pmr::string{"listTables: pg_class row oid=" + std::to_string(oid_value) +
                                                          " has NULL relname (declared NOT NULL) — the catalog "
                                                          "cannot be trusted",
                                                      resource}};
            }
            VALUE_OR_RETURN(const auto relname, cursor->get<std::string_view>(relname_col, row));
            names.emplace_back(relname.data(), relname.size());
        }
        return names;
    }

} // namespace otterbrix

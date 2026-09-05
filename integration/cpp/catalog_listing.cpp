#include "catalog_listing.hpp"

namespace otterbrix {

    core::result_wrapper_t<std::pmr::vector<std::pmr::string>>
    user_table_names_from_pg_class(std::pmr::memory_resource* resource,
                                   const components::cursor::cursor_t_ptr& cursor) {
        // A cursor that never arrived means the dispatcher answered nothing at all.
        // That is an engine fault, not a catalog with no tables in it.
        if (!cursor) {
            return core::error_t{core::error_code_t::physical_plan_error,
                                 std::pmr::string{"listTables: the catalog query returned no cursor", resource}};
        }
        // Rule 6: hand the engine's own error back untouched. Collapsing it into an
        // empty list is what made a broken catalog read look like an empty database.
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
        components::cursor::index_t oid_col = 0;
        components::cursor::index_t relname_col = 1;
        components::cursor::index_t relkind_col = 2;
        for (std::size_t i = 0; i < types.size(); ++i) {
            if (!types[i].has_alias()) {
                continue;
            }
            const auto& alias = types[i].alias();
            if (alias == "oid") {
                oid_col = static_cast<components::cursor::index_t>(i);
            } else if (alias == "relname") {
                relname_col = static_cast<components::cursor::index_t>(i);
            } else if (alias == "relkind") {
                relkind_col = static_cast<components::cursor::index_t>(i);
            }
        }

        while (cursor->has_next()) {
            cursor->advance();
            auto oid_cell = cursor->value(static_cast<uint64_t>(oid_col));
            if (oid_cell.is_null() || oid_cell.value<std::uint32_t>() < components::catalog::FIRST_USER_OID) {
                continue; // system catalog object
            }
            // relname AND relkind ARE NOT NULL IN THE SCHEMA (system_table_schemas.cpp), so a
            // NULL — or empty relkind — here is a corrupt catalog row, not a row to filter.
            // Both used to pass in silence, each the wrong way around: a NULL-relkind row was
            // ACCEPTED as a regular table (an index with a corrupted kind byte showed up in
            // the listing) and a NULL-relname row was OMITTED (a table that exists silently
            // missing from the answer). Rule 6: a catalog that cannot be trusted refuses.
            const auto oid_value = oid_cell.value<std::uint32_t>();
            auto relkind_cell = cursor->value(static_cast<uint64_t>(relkind_col));
            if (relkind_cell.is_null()) {
                return core::error_t{core::error_code_t::schema_error,
                                     std::pmr::string{"listTables: pg_class row oid=" + std::to_string(oid_value) +
                                                          " has NULL relkind (declared NOT NULL) — the catalog "
                                                          "cannot be trusted",
                                                      resource}};
            }
            auto relkind = relkind_cell.value<std::string_view>();
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
            auto relname_cell = cursor->value(static_cast<uint64_t>(relname_col));
            if (relname_cell.is_null()) {
                return core::error_t{core::error_code_t::schema_error,
                                     std::pmr::string{"listTables: pg_class row oid=" + std::to_string(oid_value) +
                                                          " has NULL relname (declared NOT NULL) — the catalog "
                                                          "cannot be trusted",
                                                      resource}};
            }
            auto relname = relname_cell.value<std::string_view>();
            names.emplace_back(relname.data(), relname.size());
        }
        return names;
    }

} // namespace otterbrix

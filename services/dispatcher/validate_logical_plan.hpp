#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>

#include <span>
#include <string_view>

namespace components::catalog {
    class table_id;
}

// Real type lives in services::catalog_resolve; impl::plan_resolve_index_t
// below is an alias (see plan_resolve_index.hpp).
namespace services::catalog_resolve {
    struct plan_resolve_index_t;
} // namespace services::catalog_resolve

namespace services::dispatcher {

    namespace impl {
        using ::services::catalog_resolve::plan_resolve_index_t;
    }

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        // The SOURCE relation this column is visible under — a table name or its AS-alias.
        std::string result_alias;
        // The column's OWN name — carried here because a type cannot carry a column name
        // without being overloaded with an identity that is not part of it (M3-B5).
        // Empty means the column genuinely has no name (a projected constant, `SELECT 1`).
        std::string name;
        components::types::complex_logical_type type;
        components::expressions::side_t side = components::expressions::side_t::undefined;
        // Set when this column is a bare NULL literal (a scalar constant whose value is NULL, whose type was
        // defaulted to text). Lets a UNION reconcile the column to the other branch's type (PostgreSQL).
        bool from_null_literal = false;
    };
    struct type_path_t {
        column_path path;
        // Name of the thing this path lands on: the COLUMN's name at depth 1, a STRUCT
        // FIELD's name deeper down. Field names stay on the type (they are part of the
        // shape); a column name does not, hence this member.
        std::string name;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    // Existence checks — return no_error() on success, an error on failure.
    [[nodiscard]] core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                                       const impl::plan_resolve_index_t* idx,
                                                       const components::catalog::table_id& id);
    [[nodiscard]] core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                                        const impl::plan_resolve_index_t* idx,
                                                        const components::catalog::table_id& id);
    // Probe `alias` against the plan-tree idx (impl::type_md_for) for each
    // dbname in `search_dbnames` in order. Returns no_error() on first hit.
    // If `search_dbnames` is empty, falls back to {"public", "pg_catalog"}.
    [[nodiscard]] core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                                  const impl::plan_resolve_index_t* idx,
                                                  const std::string& alias,
                                                  std::span<const std::string> search_dbnames = {});

    // First validation pass: every node carrying a table_oid must resolve against the
    // plan-tree idx, and an INSERT write-set bound for a schemaless computing table
    // sheds its all-NULL (NA-typed) columns, which storage cannot append.
    //
    // It does NOT coerce write-set values to their columns' declared types: that is
    // enrich_insert_sync's job, and it runs after validate_schema. `session_tz` is
    // unused today; it is kept so a value-level check that needs a timezone can be
    // added without a signature change rippling through the executor.
    [[nodiscard]] core::error_t validate_types(std::pmr::memory_resource* resource,
                                               const impl::plan_resolve_index_t* idx,
                                               components::logical_plan::node_t* node,
                                               core::date::timezone_offset_t session_tz);

    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(std::pmr::memory_resource* resource,
                    const impl::plan_resolve_index_t* idx,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters);

} // namespace services::dispatcher

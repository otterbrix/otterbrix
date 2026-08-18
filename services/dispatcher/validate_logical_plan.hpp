#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>

#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace components::catalog {
    class table_id;
}

namespace components::casts {
    class cast_registry_t;
}

namespace services::dispatcher {

    using components::logical_plan::catalog_resolves_t;

    // A recursive CTE's anchor column schema. Written by validate_schema when it
    // processes node_recursive_cte_t, read back when it processes node_cte_scan_t
    // to type the working set. Purely validate-internal — unlike catalog_resolves_t
    // nothing outside this pass produces or consumes it.
    struct cte_schema_column_t {
        std::pmr::string name;
        components::types::complex_logical_type type;
    };
    using cte_schema_t = std::vector<cte_schema_column_t>;
    using cte_schemas_t = std::unordered_map<std::pmr::string, cte_schema_t>;

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
        components::expressions::side_t side = components::expressions::side_t::undefined;
        // Set when this column is a bare NULL literal (a scalar constant whose value is NULL, whose type was
        // defaulted to text). Lets a UNION reconcile the column to the other branch's type (PostgreSQL).
        bool from_null_literal = false;
    };
    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    // Existence checks — return no_error() on success, an error on failure.
    [[nodiscard]] core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                                       const catalog_resolves_t* resolves,
                                                       const components::catalog::table_id& id);
    [[nodiscard]] core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                                        const catalog_resolves_t* resolves,
                                                        const components::catalog::table_id& id);
    // Probe `alias` against the plan's resolved type entries for each dbname in
    // `search_dbnames` in order. Returns no_error() on first hit. If
    // `search_dbnames` is empty, falls back to {"public", "pg_catalog"}.
    [[nodiscard]] core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                                  const catalog_resolves_t* resolves,
                                                  const std::string& alias,
                                                  std::span<const std::string> search_dbnames = {});

    // Validate plan node types against the plan's resolved catalog entries.
    [[nodiscard]] core::error_t validate_types(std::pmr::memory_resource* resource,
                                               const catalog_resolves_t* resolves,
                                               components::logical_plan::node_t* node,
                                               const components::graph_execution_context& execution_context);

    [[nodiscard]] core::error_t convert_column_defaults(std::pmr::memory_resource* resource,
                                                        const components::casts::cast_registry_t* cast_registry,
                                                        const components::graph_execution_context& execution_context,
                                                        std::vector<components::table::column_definition_t>& columns);

    namespace impl {
        [[nodiscard]] core::error_t
        resolve_scalar_output_type(std::pmr::memory_resource* resource,
                                   const components::casts::cast_registry_t* cast_registry,
                                   components::expressions::scalar_expression_t* scalar_expr,
                                   const named_schema& schema,
                                   const components::logical_plan::storage_parameters& parameters,
                                   const named_schema* schema_right = nullptr,
                                   bool same_schema = true,
                                   bool* saw_reduction = nullptr);

        [[nodiscard]] core::error_t
        resolve_compare_output_type(std::pmr::memory_resource* resource,
                                    const components::casts::cast_registry_t* cast_registry,
                                    components::expressions::compare_expression_t* compare_expr,
                                    const named_schema& schema,
                                    const components::logical_plan::storage_parameters& parameters);
    } // namespace impl

    // `cast_registry` is the sole source of the casts INSERT/UPDATE column coercion is
    // stamped from. `cte_schemas` carries recursive-CTE anchor schemas between the
    // recursive_cte_t and cte_scan_t arms; callers pass a default-constructed map.
    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(std::pmr::memory_resource* resource,
                    const catalog_resolves_t* resolves,
                    const components::casts::cast_registry_t* cast_registry,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters,
                    cte_schemas_t* cte_schemas = nullptr);

} // namespace services::dispatcher

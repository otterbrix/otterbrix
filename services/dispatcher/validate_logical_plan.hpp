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
#include <services/dispatcher/validation/context.hpp>
#include <services/dispatcher/validation/schema.hpp>

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

    using validation::column_path;
    using validation::named_schema;
    using validation::type_from_t;
    using validation::type_path_t;
    using validation::type_paths;

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

    // Rule 6 gate for a TYPE the DDL is about to make durable — the type half of what
    // convert_column_defaults does for a DEFAULT value.
    //
    // The persistent form of a column type (components::types::encode_type_spec, which
    // writes the table checkpoint and every WAL chunk header) refuses a DECIMAL outside
    // the width/scale window and nesting past the format depth limit. Both are reachable
    // from ordinary SQL, and refused only by the READER they cost a checkpoint that
    // succeeds and a next startup that fails with data_corruption, permanently, with no
    // statement left to blame. Asking the real encoder here — not a copy of its rules —
    // is what keeps the DDL window and the durable window the same window.
    //
    // `subject` names what is being refused ("column 'c'", "type 'deep'").
    [[nodiscard]] core::error_t gate_persistable_type(std::pmr::memory_resource* resource,
                                                      const std::string& subject,
                                                      const components::types::complex_logical_type& type);

    // `cte_schemas` carries recursive-CTE anchor schemas between the recursive_cte_t and
    // cte_scan_t arms; callers pass nullptr and the map is owned internally.
    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(const validation::validation_context_t& context,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters,
                    cte_schemas_t* cte_schemas = nullptr);

} // namespace services::dispatcher

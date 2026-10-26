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

    // Existence checks — return no_error() on success, an error on failure: database_not_exists,
    // schema_not_exists or table_not_exists for the first slot of the name the catalog does not hold.
    // The database of `name` (its connection and database slots).
    [[nodiscard]] core::error_t check_database_exists(std::pmr::memory_resource* resource,
                                                      const catalog_resolves_t* resolves,
                                                      const qualified_name_t& name);
    [[nodiscard]] core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                                       const catalog_resolves_t* resolves,
                                                       const qualified_name_t& name);
    // A written [connection.][database.]schema above a relation the resolve did not find: the missing level, else
    // no_error (the relation itself is the caller's to report). check_read_levels names only a missing database of a
    // connection: a read of a missing local database or schema answers "relation ... does not exist".
    [[nodiscard]] core::error_t check_relation_levels(std::pmr::memory_resource* resource,
                                                      const catalog_resolves_t* resolves,
                                                      const qualified_name_t& name);
    [[nodiscard]] core::error_t check_read_levels(std::pmr::memory_resource* resource,
                                                  const catalog_resolves_t* resolves,
                                                  const qualified_name_t& name);
    [[nodiscard]] core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                                        const catalog_resolves_t* resolves,
                                                        const qualified_name_t& name);
    [[nodiscard]] core::error_t
    check_column_names_unique(std::pmr::memory_resource* resource,
                              const std::vector<components::table::column_definition_t>& columns);
    // A built-in (bare or pg_catalog) or a user type the resolve found as written.
    [[nodiscard]] core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                                  const components::logical_plan::catalog_resolves_t* resolves,
                                                  const qualified_name_t& written);

    // Validate plan node types against the plan's resolved catalog entries.
    [[nodiscard]] core::error_t validate_types(std::pmr::memory_resource* resource,
                                               const catalog_resolves_t* resolves,
                                               components::logical_plan::node_t* node,
                                               const components::graph_execution_context& execution_context);

    // A column's DEFAULT cast to the column's type on assignment, and persistable.
    [[nodiscard]] core::error_t convert_column_default(std::pmr::memory_resource* resource,
                                                       const components::casts::cast_registry_t* cast_registry,
                                                       const components::graph_execution_context& execution_context,
                                                       components::table::column_definition_t& column);

    // Gates a TYPE the DDL is about to make durable, using the real encoder
    // (components::types::encode_type_spec) rather than a copy of its rules: a DECIMAL outside
    // the width/scale window or nesting past the depth limit is reachable from ordinary SQL, and
    // refused only by the reader costs a checkpoint that succeeds and a next startup that fails
    // with data_corruption, permanently, with no statement left to blame.
    // `subject` names what is being refused ("column 'c'", "type 'deep'").
    [[nodiscard]] core::error_t gate_persistable_type(std::pmr::memory_resource* resource,
                                                      const std::string& subject,
                                                      const components::types::complex_logical_type& type);

    [[nodiscard]] core::error_t gate_storable_type(std::pmr::memory_resource* resource,
                                                   const std::string& subject,
                                                   const components::types::complex_logical_type& type);

    // Resolve every CHECK predicate the plan's DML nodes carry, against the table each one guards.
    // Called after enrich_plan, which is what parses the predicates out of the catalog.
    [[nodiscard]] core::error_t
    resolve_constraint_predicates(const validation::validation_context_t& context,
                                  components::logical_plan::node_t* root,
                                  const components::logical_plan::storage_parameters& parameters);

    // `cte_schemas` carries recursive-CTE anchor schemas between the recursive_cte_t and
    // cte_scan_t arms; callers pass nullptr and the map is owned internally.
    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(const validation::validation_context_t& context,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters,
                    cte_schemas_t* cte_schemas = nullptr);

} // namespace services::dispatcher

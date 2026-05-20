#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>
#include <components/types/user_type_walk.hpp>

#include <span>
#include <string_view>

namespace components::catalog {
    class table_id;
}

namespace services::dispatcher {

    namespace impl {
        struct plan_resolve_index_t;
    }

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
    };
    struct type_path_t {
        column_path path;
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
    [[nodiscard]] core::error_t
    check_type_exists(std::pmr::memory_resource* resource,
                      const impl::plan_resolve_index_t* idx,
                      const std::string& alias,
                      std::span<const std::string> search_dbnames = {});

    // Walk a complex_logical_type tree and visit every nested UDT reference.
    using ::components::types::walk_user_type_refs;

    // Validate plan node types against the plan-tree idx (populated by Pass 1).
    [[nodiscard]] core::error_t
    validate_types(std::pmr::memory_resource* resource,
                   const impl::plan_resolve_index_t* idx,
                   components::logical_plan::node_t* node);

    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(std::pmr::memory_resource* resource,
                    const impl::plan_resolve_index_t* idx,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters);

    // validate_drop_restrict: check that no 'n'-type pg_depend rows block a RESTRICT drop.
    // fetch_deps is a closure over the pg_depend snapshot (provided by caller from disk).
    // Returns nullptr on success, error cursor on RESTRICT violation.
    components::cursor::cursor_t_ptr
    validate_drop_restrict(std::pmr::memory_resource*                resource,
                           components::catalog::oid_t                seed_classid,
                           components::catalog::oid_t                seed_oid,
                           const components::catalog::fetch_deps_fn& fetch_deps);

    // validate_type_recursion: for CREATE TABLE / CREATE TYPE nodes, detect circular user-
    // defined type references (e.g. STRUCT A { b: B } and STRUCT B { a: A }).
    // Returns nullptr on success, error cursor if a cycle is detected.
    components::cursor::cursor_t_ptr
    validate_type_recursion(std::pmr::memory_resource*                     resource,
                            const impl::plan_resolve_index_t*              idx,
                            const components::types::complex_logical_type& root_type);

} // namespace services::dispatcher

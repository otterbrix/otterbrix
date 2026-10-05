#pragma once

#include <components/catalog/results/resolve_result.hpp>
#include <components/compute/function.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <core/result_wrapper.hpp>
#include <services/dispatcher/validation/schema.hpp>

#include <memory_resource>
#include <span>
#include <string>

namespace services::collection {

    // What a validated CREATE VIEW body was bound to, stamped onto the node for the planner to write:
    //  - the output columns (names must be unique and present, types persistable);
    //  - a pg_rewrite_ref row per table name of the body itself (its first `own_tables` resolve entries): 'r' with
    //    the relation's oid, 'h' for a name the host resolved;
    //  - a pg_depend edge per user relation, per column of it the body reads, per user type of its first
    //    `own_types` type entries (PostgreSQL 18 skips pinned objects, oid < FIRST_USER_OID).
    core::error_t describe_view_body(std::pmr::memory_resource* resource,
                                     components::logical_plan::node_create_view_t& view,
                                     const dispatcher::validation::named_schema& output,
                                     const components::logical_plan::catalog_resolves_t& resolves,
                                     std::size_t own_tables,
                                     std::size_t own_types,
                                     const dispatcher::validation::column_uses_t& uses);

    // CREATE OR REPLACE VIEW over `existing` (PostgreSQL 18 view.c): only a view is replaced, never by a body that
    // reads it (`read_views` are the views the new body expands to), and its columns may only be appended to
    // (checkViewColumns). Runs on the described view, whose columns are the new ones.
    core::error_t check_view_replacement(std::pmr::memory_resource* resource,
                                         const components::logical_plan::node_create_view_t& view,
                                         const components::logical_plan::resolved_table_metadata_t& existing,
                                         std::span<const components::catalog::oid_t> read_views);

    // The user functions the validated body calls, each with the kernel signature its call was resolved to.
    std::pmr::vector<components::compute::function_pin_t>
    view_body_user_functions(std::pmr::memory_resource* resource, const components::logical_plan::node_t* body);

    // PostgreSQL 18 records the function a view calls by oid (FuncExpr.funcid): an 'f' pg_rewrite_ref row per
    // function and signature the body calls (its name, the oid of the pg_proc row of that signature, the signature as
    // that row stores it) and a pg_depend edge to that row. `rows` are the pg_proc rows of the functions' names.
    core::error_t describe_view_functions(std::pmr::memory_resource* resource,
                                          components::logical_plan::node_create_view_t& view,
                                          const components::compute::function_registry_t& registry,
                                          std::span<const components::compute::function_pin_t> uses,
                                          std::span<const services::disk::resolve_function_result_t> rows);

    // The pg_proc oids of the view's 'f' rows, for the read that pins them.
    std::pmr::vector<components::catalog::oid_t>
    view_function_oids(std::pmr::memory_resource* resource,
                       const components::logical_plan::resolved_table_metadata_t& view);

    // A read of a view calls the functions CREATE VIEW bound its body to: each 'f' row's oid still is that name and
    // signature in pg_proc (`proc_chunks`, read by the view_function_oids; else the view is stale), and this process
    // holds it (else it is not registered). Every call of the body by that name then resolves among the pinned
    // signatures alone.
    core::error_t
    pin_view_functions(std::pmr::memory_resource* resource,
                       const components::logical_plan::resolved_table_metadata_t& view,
                       const std::pmr::vector<std::pmr::vector<components::vector::data_chunk_t>>& proc_chunks,
                       const components::compute::function_registry_t& registry,
                       components::logical_plan::node_t* body);

    // A read of a view is what CREATE VIEW recorded (Trino 483 checkViewStaleness): the validated body answers as
    // many columns as were stored, each named byte for byte the same and of exactly the stored type. A host
    // node's own columns are not compared: one the view does not read cannot make it stale.
    core::error_t check_expanded_view(std::pmr::memory_resource* resource,
                                      const components::logical_plan::resolved_table_metadata_t& view,
                                      const components::logical_plan::node_t& body);

} // namespace services::collection

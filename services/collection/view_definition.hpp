#pragma once

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
    //    the relation's oid, 'h' for a name the host resolved; an 'x' row per host node with its declared columns;
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

    // Names of the user functions the body calls (PostgreSQL 18 records them like any other object).
    std::pmr::vector<std::string> view_body_user_functions(std::pmr::memory_resource* resource,
                                                           const components::logical_plan::node_t* body,
                                                           const components::compute::function_registry_t& registry);

    // A host node's declared columns as one comparable text.
    std::string host_node_spec(std::pmr::memory_resource* resource,
                               const std::pmr::vector<components::types::complex_logical_type>& columns);

    // A read of a view is what CREATE VIEW recorded (Trino 483 checkViewStaleness): the validated body answers as
    // many columns as were stored, each named the same without regard to case and of exactly the stored type. A host
    // node's own columns are not compared: one the view does not read cannot make it stale.
    core::error_t check_expanded_view(std::pmr::memory_resource* resource,
                                      const components::logical_plan::resolved_table_metadata_t& view,
                                      const components::logical_plan::node_t& body);

} // namespace services::collection

#pragma once

// Dispatcher-side catalog enrichment pass.
// Called after validate_schema and before planner_t::create_plan. Fills
// logical plan node fields (outgoing_fks, not_null_cols, etc.) from the
// plan-tree resolve idx populated by operator_resolve_*_t. The planner then
// does pure structural rewrite reading those fields — no external context
// parameter needed.

#include <actor-zeta.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/context/execution_context.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <core/result_wrapper.hpp>
#include <memory_resource>
#include <services/collection/context_storage.hpp>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace services::dispatcher {

    // STEP 1 of the two-step plan preparation: resolve everything, replace unknowns
    // with knowns.
    //
    // Afterwards the plan is SELF-CONTAINED: STEP 2 (validate_schema) reads only the
    // plan, so a query that needed no resolves at all validates through exactly the
    // same code path.
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    enrich_plan(std::pmr::memory_resource* resource,
                components::logical_plan::node_ptr root,
                components::execution_context_t ctx,
                const components::logical_plan::catalog_resolves_t* resolves,
                actor_zeta::address_t index_address = actor_zeta::address_t::empty_address(),
                services::context_storage_t* collections_ctx = nullptr);

} // namespace services::dispatcher

// catalog-resolve helpers shared by the dispatcher and executor pipelines.
// Pure functions over plan trees / lookup paths — no member-state access.
namespace services::catalog_resolve {

    // Copy resolved catalog data onto the consumer nodes that named it. Binding is
    // by name, so it depends on no node order and no sibling. Idempotent.
    void bind_catalog_data(components::logical_plan::node_t* root,
                           const components::logical_plan::catalog_resolves_t& resolves);

    // Register a lookup for every target the plan tree names
    void register_plan_targets(std::pmr::memory_resource* resource,
                               const components::logical_plan::node_t* root,
                               components::logical_plan::catalog_resolves_t* resolves);

    // Append every entry of `src` into `dest`. Entries dedupe, so a table both
    // plans reference stays one lookup.
    void merge_catalog_resolves(std::pmr::memory_resource* resource,
                                components::logical_plan::catalog_resolves_t& dest,
                                const components::logical_plan::catalog_resolves_t& src);

    // True when any entry still carries no resolved result
    bool has_unresolved_entries(const components::logical_plan::catalog_resolves_t& resolves);

    // Probe `name` across the dbname search path. The transformer registers a type
    // entry for every (dbname, name) tuple we expect to find here (CREATE TABLE
    // column UDT, CREATE TYPE collision check, DROP TYPE existence check).
    // search_dbnames carries dbname strings ordered by precedence.
    const components::logical_plan::resolved_type_metadata_t*
    probe_type_in_path(const components::logical_plan::catalog_resolves_t& resolves,
                       std::string_view name,
                       std::span<const std::string> search_dbnames);

    // Type-name search path. Deduplicates entries (when target_dbname is already
    // "public" / "pg_catalog").
    std::vector<std::string> build_type_search_path_str(std::string_view target_dbname);

} // namespace services::catalog_resolve

// Lets dispatcher code call the plan-preparation helpers unqualified.
namespace services::dispatcher {
    using catalog_resolve::bind_catalog_data;
    using catalog_resolve::merge_catalog_resolves;
    using catalog_resolve::register_plan_targets;
} // namespace services::dispatcher

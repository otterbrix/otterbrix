#pragma once

// Fills logical plan node fields (outgoing_fks, not_null_cols, etc.) from the resolve idx
// operator_resolve_*_t populated, so the planner's rewrite needs no external context parameter.

#include <actor-zeta.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>
#include <components/context/execution_context.hpp>
#include <components/cursor/cursor.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/planner/host_hooks.hpp>
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

    // Resolves everything and replaces unknowns with knowns, so the plan is self-contained
    // afterwards -- validate_schema then reads only the plan, nothing external.
    [[nodiscard]] actor_zeta::unique_future<core::error_t>
    enrich_plan(std::pmr::memory_resource* resource,
                components::logical_plan::node_ptr root,
                components::execution_context_t ctx,
                const components::logical_plan::catalog_resolves_t* resolves,
                actor_zeta::address_t index_address,
                services::context_storage_t* collections_ctx = nullptr);

} // namespace services::dispatcher

// catalog-resolve helpers shared by the dispatcher and executor pipelines.
namespace services::catalog_resolve {

    // Binds by name, so it depends on no node order and no sibling; idempotent.
    void bind_catalog_data(components::logical_plan::node_t* root,
                           const components::logical_plan::catalog_resolves_t& resolves);

    void register_plan_targets(std::pmr::memory_resource* resource,
                               const components::logical_plan::node_t* root,
                               components::logical_plan::catalog_resolves_t* resolves);

    // A view body name pinned to a relation the catalog no longer holds: the view is stale. Runs before any
    // connector is asked for a storage.
    core::error_t refuse_stale_pins(std::pmr::memory_resource* resource,
                                    const components::logical_plan::catalog_resolves_t& resolves);

    // A REFERENCES target in another database than the table owning the key is refused.
    core::error_t refuse_cross_database_references(std::pmr::memory_resource* resource,
                                                   const components::logical_plan::catalog_resolves_t& resolves);

    core::error_t refuse_storage_write_in_savepoint(std::pmr::memory_resource* resource,
                                                    const components::logical_plan::catalog_resolves_t& resolves,
                                                    const components::logical_plan::node_t& write);

} // namespace services::catalog_resolve

namespace services::dispatcher {
    using catalog_resolve::bind_catalog_data;
    using catalog_resolve::register_plan_targets;
} // namespace services::dispatcher

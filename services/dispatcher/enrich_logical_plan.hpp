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

    // Entries dedupe, so a table both plans reference stays one lookup.
    void merge_catalog_resolves(std::pmr::memory_resource* resource,
                                components::logical_plan::catalog_resolves_t& dest,
                                const components::logical_plan::catalog_resolves_t& src);

    bool has_unresolved_entries(const components::logical_plan::catalog_resolves_t& resolves);

    // After the one classification of names (resolve probed which first parts are servers): uid = server and the
    // remote path in the remaining slots for a remote name; no uid for a local one (a uid that is no server keeps
    // its old meaning, database.name). Resolve records and the FROM nodes that carry the written name move
    // together; from here on a non-empty uid alone means remote.
    void reslot_classified_names(components::logical_plan::node_t* root,
                                 components::logical_plan::catalog_resolves_t& resolves);

    // A REFERENCES target with a uid or schema segment (its first part no server) is refused, not dropped.
    core::error_t refuse_referenced_segments(std::pmr::memory_resource* resource,
                                             const components::logical_plan::catalog_resolves_t& resolves);

    // A read never drops part of a name: a schema segment on a local relation (database.schema.name, first part
    // not a server) is refused. The uid form keeps its meaning for the host's swap hooks.
    core::error_t refuse_local_schema_segments(std::pmr::memory_resource* resource,
                                               const components::logical_plan::catalog_resolves_t& resolves);

    // The first part of the name a statement writes to: the uid of a 4-part target, else its database.
    std::string_view statement_target_first_part(const components::logical_plan::node_t* root,
                                                 const components::logical_plan::catalog_resolves_t& resolves);

    // search_dbnames is ordered by precedence over the type-name search path.
    const components::logical_plan::resolved_type_metadata_t*
    probe_type_in_path(const components::logical_plan::catalog_resolves_t& resolves,
                       std::string_view name,
                       std::span<const std::string> search_dbnames);

    // Deduplicates entries when target_dbname is already "public" or "pg_catalog".
    std::vector<std::string> build_type_search_path_str(std::string_view target_dbname);

} // namespace services::catalog_resolve

namespace services::dispatcher {
    using catalog_resolve::bind_catalog_data;
    using catalog_resolve::merge_catalog_resolves;
    using catalog_resolve::register_plan_targets;
} // namespace services::dispatcher

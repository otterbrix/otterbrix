#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/place.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/index_facts.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <services/dispatcher/txn_messages.hpp>
#include <unordered_map>
#include <unordered_set>

namespace components::compute {
    class function_registry_t;
} // namespace components::compute

namespace services {

    struct commit_input_t {
        dispatcher::txn_commit_drain_t drain;
        core::error_t resolve_error{core::error_t::no_error()};
    };

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        components::graph_execution_context execution_context;
        // The statement's place; the resolve operators complete short names with it.
        components::catalog::place_t place;
        // The disk's address: the storage of the catalog tables, which have no resolved metadata.
        actor_zeta::actor::address_t disk_address{actor_zeta::actor::address_t::empty_address()};
        // oid-only routing. Plan generators ask "do we know about this table?"
        // via the resolved table_oid stamped on the logical_plan node.
        // Wrapper / parser-window paths fall back to the empty set.
        std::unordered_set<components::catalog::oid_t> known_oids;
        // Filled by enrich before the optimizer runs; the optimizer chooses the indexes from them.
        components::logical_plan::index_facts_t indexes;
        const components::logical_plan::storage_parameters* parameters = nullptr;
        // Every catalog lookup the plan depends on, owned by the execution_plan_t
        const components::logical_plan::catalog_resolves_t* catalog_resolves = nullptr;
        // The functions the statement sees, which the executor sets as it lowers the plan.
        const components::compute::function_registry_t* functions = nullptr;
        // Set for a COMMIT plan only;
        commit_input_t* commit_input = nullptr;
        // oid -> resolved_table_metadata_t* stamped by Pass 1's
        // operator_resolve_table_t. Plan generators (transfer_scan in
        // create_plan_match / create_plan_aggregate) use it to forward live
        // column names + relkind.
        std::unordered_map<components::catalog::oid_t, const components::logical_plan::resolved_table_metadata_t*>
            table_metadata;
        // Slot pointers for recursive CTE working sets. Keyed by CTE name.
        // Each entry points into the owning operator_recursive_cte_t's working_set_ field.
        std::pmr::unordered_map<std::pmr::string, components::operators::operator_data_ptr*> cte_working_sets;
        // Live per-table row counts (physical appended count from
        // manager_disk_t::storage_total_rows), keyed by resolved table_oid.
        // execute_plan_full fetches these for the child tables of every INNER
        // hash join BEFORE lowering; create_plan_join reads them to put the
        // smaller side on the hash build. Empty when the executor was given no
        // disk-manager address -> the build-side swap no-ops.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> row_counts;

        context_storage_t(std::pmr::memory_resource* resource,
                          log_t log,
                          const components::catalog::session_catalog_t& settings)
            : resource(resource)
            , log(std::move(log))
            , execution_context{.timezone_offset = settings.timezone_offset}
            , indexes(resource)
            , cte_working_sets(resource)
            , row_counts(resource) {}

        context_storage_t(std::pmr::memory_resource* resource,
                          log_t log,
                          const components::graph_execution_context& inherited)
            : resource(resource)
            , log(std::move(log))
            , execution_context(inherited)
            , indexes(resource)
            , cte_working_sets(resource)
            , row_counts(resource) {}

        bool has_table_oid(components::catalog::oid_t oid) const noexcept {
            return oid != components::catalog::INVALID_OID && known_oids.count(oid) > 0;
        }

        const components::logical_plan::resolved_table_metadata_t*
        table_metadata_for(components::catalog::oid_t oid) const noexcept {
            auto it = table_metadata.find(oid);
            return it != table_metadata.end() ? it->second : nullptr;
        }
    };

} //namespace services

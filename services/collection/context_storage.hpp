#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/execution_context/graph_execution_context.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/index/forward.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <unordered_map>
#include <unordered_set>

namespace components::compute {
    class function_registry_t;
}
namespace components::operators {
    class operator_t;
}

namespace services {

    struct context_storage_t;

    namespace planner {
        // Host-injected physical-plan rule: lowers a host-custom logical node
        // (node_extension) the engine cannot lower itself, into the host's own
        // operator. Delivered through the constructor chain (base_spaces ->
        // dispatcher -> executor) and stamped onto context_storage per query; the
        // physical-plan generator reads it at the `case node_type::extension_t` arm.
        // Plain fn-ptr (no std::function). NEVER null — defaults to no_custom_lowering
        // (a Null Object returning {}); the returned operator MAY be null, meaning
        // "no host lowering for this node".
        using create_plan_rule_t =
            boost::intrusive_ptr<components::operators::operator_t> (*)(const context_storage_t&,
                                                                        const components::compute::function_registry_t&,
                                                                        const components::logical_plan::node_ptr&);

        boost::intrusive_ptr<components::operators::operator_t>
        no_custom_lowering(const context_storage_t&,
                           const components::compute::function_registry_t&,
                           const components::logical_plan::node_ptr&);
    } // namespace planner

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        components::graph_execution_context execution_context;
        core::date::timezone_offset_t session_timezone;
        // Host-injected create_plan rule (see planner::create_plan_rule_t). Stamped
        // per query by the executor; read at create_plan's extension arm. Never null.
        planner::create_plan_rule_t create_plan_rule = &planner::no_custom_lowering;
        // oid-only routing. Plan generators ask "do we know about this table?"
        // via the resolved table_oid stamped on the logical_plan node.
        // Wrapper / parser-window paths fall back to the empty set.
        std::unordered_set<components::catalog::oid_t> known_oids;
        // Per-table index info fetched by enrich_logical_plan (get_indexed_keys +
        // get_indexed_descriptions), keyed by resolved table_oid — one entry per
        // queried table of the statement, read through the oid-taking accessors
        // below (mirrors table_metadata / table_metadata_for). An oid without an
        // entry has, definitively, no usable index: a table without indexes, a
        // statement enriched without an index service, and an unresolved oid all
        // land there. NEVER flatten this across tables — a multi-table statement
        // would judge one table's predicate by another table's index set (the
        // "last table wins" defect: a bogus index_scan silently returns zero rows).
        struct table_index_info_t {
            std::pmr::vector<components::index::keys_base_storage_t> keys;
            std::pmr::vector<components::index::index_description_t> descriptions;

            explicit table_index_info_t(std::pmr::memory_resource* resource)
                : keys(resource)
                , descriptions(resource) {}
        };
        std::pmr::unordered_map<components::catalog::oid_t, table_index_info_t> table_indexes;
        const components::logical_plan::storage_parameters* parameters = nullptr;
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
        // smaller side on the hash build. Empty in in-memory mode (no owning
        // disk agent) -> the build-side swap no-ops.
        std::pmr::unordered_map<components::catalog::oid_t, uint64_t> row_counts;

        context_storage_t(std::pmr::memory_resource* resource,
                          log_t log,
                          core::date::timezone_offset_t session_timezone)
            : resource(resource)
            , log(std::move(log))
            , execution_context{.timezone_offset = session_timezone}
            , table_indexes(resource)
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

        const table_index_info_t* index_info_for(components::catalog::oid_t oid) const noexcept {
            auto it = table_indexes.find(oid);
            return it != table_indexes.end() ? &it->second : nullptr;
        }

        // The ONE mutation point (enrich_logical_plan + planner tests): the
        // table_indexes entry for `oid`, created empty on `resource` if absent.
        table_index_info_t& index_info_slot(components::catalog::oid_t oid) {
            auto it = table_indexes.find(oid);
            if (it == table_indexes.end()) {
                it = table_indexes.emplace(oid, table_index_info_t{resource}).first;
            }
            return it->second;
        }

        bool has_index_on(components::catalog::oid_t oid, const components::expressions::key_t& key) const {
            const auto* info = index_info_for(oid);
            if (info == nullptr) {
                return false;
            }
            for (const auto& keys : info->keys) {
                if (keys.size() == 1 && keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        bool has_index_on(components::catalog::oid_t oid,
                          const components::expressions::key_t& key,
                          components::logical_plan::index_type type) const {
            const auto* info = index_info_for(oid);
            if (info == nullptr) {
                return false;
            }
            for (const auto& desc : info->descriptions) {
                if (desc.type != type) {
                    continue;
                }
                if (desc.keys.size() == 1 && desc.keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        bool has_index_on_with_other_type(components::catalog::oid_t oid,
                                          const components::expressions::key_t& key,
                                          components::logical_plan::index_type type) const {
            const auto* info = index_info_for(oid);
            if (info == nullptr) {
                return false;
            }
            for (const auto& desc : info->descriptions) {
                if (desc.type == type) {
                    continue;
                }
                if (desc.keys.size() == 1 && desc.keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        components::logical_plan::index_type
        preferred_index_type_for_compare(components::catalog::oid_t oid,
                                         const components::expressions::key_t& key,
                                         components::expressions::compare_type compare) const {
            const bool is_range = compare == components::expressions::compare_type::lt ||
                                  compare == components::expressions::compare_type::lte ||
                                  compare == components::expressions::compare_type::gt ||
                                  compare == components::expressions::compare_type::gte;

            if (!is_range && has_index_on(oid, key, components::logical_plan::index_type::hashed)) {
                return components::logical_plan::index_type::hashed;
            }
            if (is_range && has_index_on(oid, key, components::logical_plan::index_type::single)) {
                return components::logical_plan::index_type::single;
            }
            return components::logical_plan::index_type::no_valid;
        }
    };

} //namespace services

#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/index/forward.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <unordered_map>
#include <unordered_set>

namespace components::logical_plan {
    class node_extension_t;
}
namespace components::operators {
    class operator_t;
}

namespace services {

    struct context_storage_t;

    // Host-injected factory that lowers a node_extension_t to the host's own
    // operator_t. Set at engine construction (base_spaces -> dispatcher -> executor)
    // and stamped onto context_storage per query; the physical-plan generator's
    // `case extension_t` calls it. Plain fn-ptr (no std::function). NEVER null —
    // defaults to no_extension_operator (a Null Object), so callers invoke it
    // unconditionally without a nullptr guard.
    using extension_operator_factory_t = boost::intrusive_ptr<components::operators::operator_t> (*)(
        const context_storage_t&,
        const components::logical_plan::node_extension_t&);

    // Default factory: the embedding host registered no extension operators, so an
    // extension node has no host operator to build (its plan errors downstream).
    boost::intrusive_ptr<components::operators::operator_t>
    no_extension_operator(const context_storage_t&, const components::logical_plan::node_extension_t&);

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        core::date::timezone_offset_t session_timezone;
        // See extension_operator_factory_t. Never null (Null Object default).
        extension_operator_factory_t extension_factory = &no_extension_operator;
        // oid-only routing. Plan generators ask "do we know about this table?"
        // via the resolved table_oid stamped on the logical_plan node.
        // Wrapper / parser-window paths fall back to the empty set.
        std::unordered_set<components::catalog::oid_t> known_oids;
        std::pmr::vector<components::index::keys_base_storage_t> indexed_keys;
        std::pmr::vector<components::index::index_description_t> indexed_descriptions;
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
            , session_timezone(session_timezone)
            , indexed_keys(resource)
            , indexed_descriptions(resource)
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

        bool has_index_on(const components::expressions::key_t& key) const {
            for (const auto& keys : indexed_keys) {
                if (keys.size() == 1 && keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        bool has_index_on(const components::expressions::key_t& key, components::logical_plan::index_type type) const {
            for (const auto& desc : indexed_descriptions) {
                if (desc.type != type) {
                    continue;
                }
                if (desc.keys.size() == 1 && desc.keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }

        bool has_index_on_with_other_type(const components::expressions::key_t& key,
                                          components::logical_plan::index_type type) const {
            for (const auto& desc : indexed_descriptions) {
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
        preferred_index_type_for_compare(const components::expressions::key_t& key,
                                         components::expressions::compare_type compare) const {
            const bool is_range = compare == components::expressions::compare_type::lt ||
                                  compare == components::expressions::compare_type::lte ||
                                  compare == components::expressions::compare_type::gt ||
                                  compare == components::expressions::compare_type::gte;

            if (!is_range && has_index_on(key, components::logical_plan::index_type::hashed)) {
                return components::logical_plan::index_type::hashed;
            }
            if (is_range && has_index_on(key, components::logical_plan::index_type::single)) {
                return components::logical_plan::index_type::single;
            }
            return components::logical_plan::index_type::no_valid;
        }
    };

} //namespace services

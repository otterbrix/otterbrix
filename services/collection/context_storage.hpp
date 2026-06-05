#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/key.hpp>
#include <components/index/forward.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <unordered_map>
#include <unordered_set>

namespace services {

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        core::date::timezone_offset_t session_timezone;
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

        context_storage_t(std::pmr::memory_resource* resource,
                          log_t log,
                          core::date::timezone_offset_t session_timezone)
            : resource(resource)
            , log(std::move(log))
            , session_timezone(session_timezone)
            , indexed_keys(resource)
            , indexed_descriptions(resource)
            , cte_working_sets(resource) {}

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

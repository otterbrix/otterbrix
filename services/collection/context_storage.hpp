#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/key.hpp>
#include <components/index/forward.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <unordered_map>
#include <unordered_set>

namespace services {

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        // oid-only routing. Plan generators ask "do we know about this table?"
        // via the resolved table_oid stamped on the logical_plan node.
        // Wrapper / parser-window paths fall back to the empty set.
        std::unordered_set<components::catalog::oid_t> known_oids;
        std::pmr::vector<components::index::keys_base_storage_t> indexed_keys;
        std::pmr::vector<components::index::keys_base_storage_t> hashed_indexed_keys;
        const components::logical_plan::storage_parameters* parameters = nullptr;
        // oid -> resolved_table_metadata_t* stamped by Pass 1's
        // operator_resolve_table_t. Plan generators (transfer_scan in
        // create_plan_match / create_plan_aggregate) use it to forward live
        // column names + relkind.
        std::unordered_map<components::catalog::oid_t, const components::logical_plan::resolved_table_metadata_t*>
            table_metadata;

        context_storage_t(std::pmr::memory_resource* resource, log_t log)
            : resource(resource)
            , log(std::move(log))
            , indexed_keys(resource)
            , hashed_indexed_keys(resource) {}

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

        bool has_hashed_index_on(const components::expressions::key_t& key) const {
            for (const auto& keys : hashed_indexed_keys) {
                if (keys.size() == 1 && keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }
    };

} //namespace services

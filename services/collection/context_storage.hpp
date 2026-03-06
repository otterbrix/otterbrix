#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/expressions/key.hpp>
#include <components/index/forward.hpp>
#include <components/log/log.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <unordered_set>

namespace services {

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        std::unordered_set<collection_full_name_t, collection_name_hash> known_collections;
        std::pmr::vector<components::index::keys_base_storage_t> indexed_keys;
        const components::logical_plan::storage_parameters* parameters = nullptr;

        context_storage_t(std::pmr::memory_resource* resource, log_t log)
            : resource(resource)
            , log(std::move(log))
            , indexed_keys(resource) {}

        bool has_collection(const collection_full_name_t& name) const { return known_collections.count(name) > 0; }

        bool has_index_on(const components::expressions::key_t& key) const {
            for (const auto& keys : indexed_keys) {
                if (keys.size() == 1 && keys[0].as_string() == key.as_string()) {
                    return true;
                }
            }
            return false;
        }
    };

} //namespace services

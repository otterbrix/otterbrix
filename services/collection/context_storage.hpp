#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/log/log.hpp>
#include <unordered_set>

namespace services {

    struct context_storage_t {
        std::pmr::memory_resource* resource;
        log_t log;
        std::unordered_set<collection_full_name_t, collection_name_hash> known_collections;

        context_storage_t(std::pmr::memory_resource* resource, log_t log)
            : resource(resource)
            , log(std::move(log)) {}

        bool has_collection(const collection_full_name_t& name) const {
            return known_collections.count(name) > 0;
        }
    };

} //namespace services

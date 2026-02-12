#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/log/log.hpp>
#include <unordered_set>

namespace services {

    struct context_storage_t {
        std::pmr::memory_resource* resource = nullptr;
        log_t* log = nullptr;
        std::unordered_set<collection_full_name_t, collection_name_hash> known_collections;

        bool has_collection(const collection_full_name_t& name) const {
            return known_collections.count(name) > 0;
        }
    };

} //namespace services

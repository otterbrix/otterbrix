#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

#include <memory_resource>
#include <set>

namespace services::loader {

    using collection_set_t = std::pmr::set<collection_full_name_t>;

    struct loaded_state_t {
        std::pmr::set<database_name_t> databases;
        collection_set_t collections;
        std::pmr::vector<components::logical_plan::node_create_index_ptr> index_definitions;
        std::vector<wal::record_t> wal_records;
        wal::id_t last_wal_id{0};

        explicit loaded_state_t(std::pmr::memory_resource* resource)
            : databases(resource)
            , collections(resource)
            , index_definitions(resource)
            , wal_records()
            , last_wal_id(0) {}
    };

} // namespace services::loader

#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/document/document.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

#include <memory_resource>
#include <set>
#include <vector>

namespace services::loader {

    using components::document::document_ptr;

    using document_map_t = std::pmr::unordered_map<
        collection_full_name_t,
        std::pmr::vector<document_ptr>,
        collection_name_hash>;

    using schema_map_t = std::pmr::unordered_map<
        collection_full_name_t,
        std::pmr::vector<std::string>,
        collection_name_hash>;

    struct loaded_state_t {
        std::pmr::set<database_name_t> databases;
        document_map_t documents;
        schema_map_t schemas;
        std::pmr::vector<components::logical_plan::node_create_index_ptr> index_definitions;
        std::vector<wal::record_t> wal_records;
        wal::id_t last_wal_id{0};

        explicit loaded_state_t(std::pmr::memory_resource* resource)
            : databases(resource)
            , documents(resource)
            , schemas(resource)
            , index_definitions(resource)
            , wal_records()
            , last_wal_id(0) {}
    };

} // namespace services::loader

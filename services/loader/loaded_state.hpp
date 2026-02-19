#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <services/disk/catalog_storage.hpp>
#include <services/wal/base.hpp>
#include <services/wal/record.hpp>

#include <memory_resource>
#include <set>

namespace services::loader {

    using collection_set_t = std::pmr::set<collection_full_name_t>;

    struct collection_load_info_t {
        collection_full_name_t name;
        services::disk::table_storage_mode_t storage_mode{services::disk::table_storage_mode_t::IN_MEMORY};
        std::vector<services::disk::catalog_column_entry_t> columns;
    };

    struct loaded_state_t {
        std::pmr::set<database_name_t> databases;
        collection_set_t collections;
        std::vector<collection_load_info_t> collection_infos;
        std::pmr::vector<components::logical_plan::node_create_index_ptr> index_definitions;
        std::vector<wal::record_t> wal_records;
        wal::id_t last_wal_id{0};

        // Catalog DDL objects (per-database)
        std::vector<std::pair<database_name_t, services::disk::catalog_sequence_entry_t>> sequences;
        std::vector<std::pair<database_name_t, services::disk::catalog_view_entry_t>> views;
        std::vector<std::pair<database_name_t, services::disk::catalog_macro_entry_t>> macros;

        explicit loaded_state_t(std::pmr::memory_resource* resource)
            : databases(resource)
            , collections(resource)
            , index_definitions(resource)
            , wal_records()
            , last_wal_id(0) {}
    };

} // namespace services::loader

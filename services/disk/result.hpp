#pragma once

#include "catalog_storage.hpp"
#include <components/base/collection_full_name.hpp>
#include <memory_resource>
#include <services/wal/base.hpp>
#include <vector>

namespace services::disk {

    struct result_database_t {
        database_name_t name;
        std::pmr::vector<collection_name_t> collections;
        std::vector<catalog_table_entry_t> table_entries_; // enriched per-collection info
        std::vector<catalog_sequence_entry_t> sequence_entries_;
        std::vector<catalog_view_entry_t> view_entries_;
        std::vector<catalog_macro_entry_t> macro_entries_;

        result_database_t(std::pmr::memory_resource* resource, database_name_t name)
            : name(std::move(name))
            , collections(resource) {}

        const std::pmr::vector<collection_name_t>& name_collections() const { return collections; }
        void set_collection(const std::vector<collection_name_t>& names) {
            collections.assign(names.begin(), names.end());
        }
        void set_table_entries(std::vector<catalog_table_entry_t> entries) { table_entries_ = std::move(entries); }
        void set_sequence_entries(std::vector<catalog_sequence_entry_t> entries) {
            sequence_entries_ = std::move(entries);
        }
        void set_view_entries(std::vector<catalog_view_entry_t> entries) { view_entries_ = std::move(entries); }
        void set_macro_entries(std::vector<catalog_macro_entry_t> entries) { macro_entries_ = std::move(entries); }
    };

    class result_load_t {
        using result_t = std::vector<result_database_t>;

    public:
        result_load_t() = default;
        result_load_t(std::pmr::memory_resource* resource,
                      const std::vector<database_name_t>& databases,
                      wal::id_t wal_id);
        const result_t& operator*() const;
        result_t& operator*();
        std::vector<database_name_t> name_databases() const;
        std::size_t count_collections() const;
        void clear();

        wal::id_t wal_id() const;

        static result_load_t empty();

    private:
        std::pmr::memory_resource* resource_{nullptr};
        result_t databases_;
        wal::id_t wal_id_{0};
    };

} // namespace services::disk

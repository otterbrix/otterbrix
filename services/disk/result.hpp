#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/document/document.hpp>
#include <services/wal/base.hpp>
#include <vector>

namespace services::disk {

    struct result_collection_t {
        collection_name_t name;
        std::pmr::vector<components::document::document_ptr> documents;

        result_collection_t() = default;
        explicit result_collection_t(std::pmr::memory_resource* resource)
            : documents(resource) {}
        result_collection_t(std::pmr::memory_resource* resource, collection_name_t n)
            : name(std::move(n)), documents(resource) {}
    };

    struct result_database_t {
        database_name_t name;
        std::vector<result_collection_t> collections;

        std::vector<collection_name_t> name_collections() const;
        void set_collection(std::pmr::memory_resource* resource, const std::vector<collection_name_t>& names);
    };

    class result_load_t {
        using result_t = std::vector<result_database_t>;

    public:
        result_load_t() = default;
        result_load_t(const std::vector<database_name_t>& databases, wal::id_t wal_id);
        const result_t& operator*() const;
        result_t& operator*();
        std::vector<database_name_t> name_databases() const;
        std::size_t count_collections() const;
        void clear();

        wal::id_t wal_id() const;

        static result_load_t empty();

    private:
        result_t databases_;
        wal::id_t wal_id_{0};
    };

} // namespace services::disk

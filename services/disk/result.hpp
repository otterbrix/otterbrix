#pragma once

#include <components/base/collection_full_name.hpp>
#include <services/wal/base.hpp>
#include <memory_resource>
#include <vector>

namespace services::disk {

    struct result_database_t {
        database_name_t name;
        std::pmr::vector<collection_name_t> collections;

        result_database_t(std::pmr::memory_resource* resource, database_name_t name)
            : name(std::move(name))
            , collections(resource) {}

        const std::pmr::vector<collection_name_t>& name_collections() const { return collections; }
        void set_collection(const std::vector<collection_name_t>& names) {
            collections.assign(names.begin(), names.end());
        }
    };

    class result_load_t {
        using result_t = std::vector<result_database_t>;

    public:
        result_load_t() = default;
        result_load_t(std::pmr::memory_resource* resource, const std::vector<database_name_t>& databases, wal::id_t wal_id);
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

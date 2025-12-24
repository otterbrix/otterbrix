#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <core/file/local_file_system.hpp>

#include <components/base/collection_full_name.hpp>

namespace services::disk {

    class metadata_t {
        using path_t = std::filesystem::path;
        using collections_t = std::vector<collection_name_t>;
        using databases_t = std::vector<database_name_t>;
        using data_t = std::unordered_map<database_name_t, collections_t>;
        using metadata_ptr = std::unique_ptr<metadata_t>;

    public:
        static metadata_ptr open(core::filesystem::local_file_system_t& fs, const path_t& file_name);

        databases_t databases() const;
        const collections_t& collections(const database_name_t& database) const;

        bool is_exists_database(const database_name_t& database) const;
        bool append_database(const database_name_t& database, bool is_flush = true);
        bool remove_database(const database_name_t& database, bool is_flush = true);

        bool is_exists_collection(const database_name_t& database, const collection_name_t& collection) const;
        bool
        append_collection(const database_name_t& database, const collection_name_t& collection, bool is_flush = true);
        bool
        remove_collection(const database_name_t& database, const collection_name_t& collection, bool is_flush = true);

        metadata_t() = delete;
        metadata_t(const metadata_t&) = delete;
        metadata_t(metadata_t&&) = delete;
        metadata_t& operator=(metadata_t const&) = delete;
        metadata_t& operator=(metadata_t&&) = delete;

    private:
        data_t data_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;

        explicit metadata_t(core::filesystem::local_file_system_t& fs, const path_t& file_name);
        void flush_();
    };

} //namespace services::disk
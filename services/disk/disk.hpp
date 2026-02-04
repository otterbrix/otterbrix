#pragma once
#include <core/b_plus_tree/b_plus_tree.hpp>
#include <filesystem>
#include <services/wal/base.hpp>

#include "metadata.hpp"

#include <components/base/collection_full_name.hpp>

namespace services::disk {

    using path_t = std::filesystem::path;
    using metadata_ptr = std::unique_ptr<metadata_t>;
    using file_ptr = std::unique_ptr<core::filesystem::file_handle_t>;
    using btree_ptr = std::unique_ptr<core::b_plus_tree::btree_t>;

    // TODO: add checkpoints to avoid flushing b+tree after each call
    class disk_t {
    public:
        explicit disk_t(const path_t& storage_directory, std::pmr::memory_resource* resource);
        disk_t(const disk_t&) = delete;
        disk_t& operator=(disk_t const&) = delete;

        [[nodiscard]] std::vector<database_name_t> databases() const;
        bool append_database(const database_name_t& database);
        bool remove_database(const database_name_t& database);

        [[nodiscard]] std::vector<collection_name_t> collections(const database_name_t& database) const;
        bool append_collection(const database_name_t& database, const collection_name_t& collection);
        bool remove_collection(const database_name_t& database, const collection_name_t& collection);

        void fix_wal_id(wal::id_t wal_id);
        wal::id_t wal_id() const;

    private:
        path_t path_;
        std::pmr::memory_resource* resource_;
        core::filesystem::local_file_system_t fs_;
        std::pmr::unordered_map<collection_full_name_t, btree_ptr, collection_name_hash> db_;
        metadata_ptr metadata_;
        file_ptr file_wal_id_;
    };

} //namespace services::disk

#include "disk.hpp"

namespace services::disk {

    using namespace core::filesystem;

    disk_t::disk_t(const path_t& storage_directory, std::pmr::memory_resource* resource)
        : path_(storage_directory)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , catalog_(fs_, storage_directory / "catalog.otbx")
        , file_wal_id_(nullptr) {
        create_directories(storage_directory);

        if (std::filesystem::exists(storage_directory / "catalog.otbx")) {
            catalog_.load();
        }

        file_wal_id_ = open_file(fs_,
                                 storage_directory / "WAL_ID",
                                 file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                 file_lock_type::NO_LOCK);
    }

    std::vector<database_name_t> disk_t::databases() const {
        auto names = catalog_.databases();
        return std::vector<database_name_t>(names.begin(), names.end());
    }

    bool disk_t::append_database(const database_name_t& database) {
        if (catalog_.database_exists(database)) {
            return false;
        }
        catalog_.append_database(database);
        return true;
    }

    bool disk_t::remove_database(const database_name_t& database) {
        if (!catalog_.database_exists(database)) {
            return false;
        }
        for (const auto& collection : catalog_.collection_names(database)) {
            remove_collection(database, collection);
        }
        catalog_.remove_database(database);
        return true;
    }

    std::vector<collection_name_t> disk_t::collections(const database_name_t& database) const {
        auto names = catalog_.collection_names(database);
        return std::vector<collection_name_t>(names.begin(), names.end());
    }

    bool disk_t::append_collection(const database_name_t& database, const collection_name_t& collection) {
        if (catalog_.find_table(database, collection) != nullptr) {
            return false;
        }
        if (!catalog_.database_exists(database)) {
            return false;
        }
        // Default: IN_MEMORY with no columns (computing table)
        catalog_table_entry_t entry;
        entry.name = collection;
        entry.storage_mode = table_storage_mode_t::IN_MEMORY;
        catalog_.append_table(database, entry);
        return true;
    }

    bool disk_t::append_collection(const database_name_t& database,
                                   const collection_name_t& collection,
                                   table_storage_mode_t mode,
                                   const std::vector<catalog_column_entry_t>& columns) {
        if (catalog_.find_table(database, collection) != nullptr) {
            return false;
        }
        if (!catalog_.database_exists(database)) {
            return false;
        }
        catalog_table_entry_t entry;
        entry.name = collection;
        entry.storage_mode = mode;
        entry.columns = columns;
        catalog_.append_table(database, entry);
        return true;
    }

    bool disk_t::remove_collection(const database_name_t& database, const collection_name_t& collection) {
        if (catalog_.find_table(database, collection) == nullptr) {
            return false;
        }
        core::filesystem::remove_directory(fs_, path_ / database / collection);
        catalog_.remove_table(database, collection);
        return true;
    }

    std::vector<catalog_table_entry_t> disk_t::table_entries(const database_name_t& database) const {
        return catalog_.tables(database);
    }

    void disk_t::fix_wal_id(wal::id_t wal_id) {
        auto id = std::to_string(wal_id);
        file_wal_id_->write(id.data(), id.size(), 0);
        file_wal_id_->truncate(static_cast<int64_t>(id.size()));
    }

    wal::id_t disk_t::wal_id() const { return wal::id_from_string(file_wal_id_->read_line()); }

} //namespace services::disk
#include "disk.hpp"

#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>

namespace services::disk {

    using namespace core::filesystem;

    constexpr static std::string_view base_index_name = "base_index";

    auto key_getter = [](const core::b_plus_tree::btree_t::item_data& item) -> core::b_plus_tree::btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) { return true; });
        return core::b_plus_tree::get_field(msg.get(), "/_id");
    };

    disk_t::disk_t(const path_t& storage_directory, std::pmr::memory_resource* resource)
        : path_(storage_directory)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , db_(resource_)
        , catalog_(fs_, storage_directory / "catalog.otbx")
        , file_wal_id_(nullptr) {
        create_directories(storage_directory);

        // Try to load catalog.otbx; if it doesn't exist, try migrating from legacy METADATA
        if (std::filesystem::exists(storage_directory / "catalog.otbx")) {
            catalog_.load();
        } else if (std::filesystem::exists(storage_directory / "METADATA")) {
            // Migrate from legacy text format
            migrate_from_legacy_(storage_directory / "METADATA");
        }

        file_wal_id_ = open_file(fs_,
                                 storage_directory / "WAL_ID",
                                 file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                 file_lock_type::NO_LOCK);

        for (const auto& database : catalog_.databases()) {
            for (const auto& collection : catalog_.collection_names(database)) {
                path_t p = storage_directory / database / collection / base_index_name;
                if (std::filesystem::exists(p) || std::filesystem::exists(p.parent_path())) {
                    db_.emplace(collection_full_name_t{database, collection},
                                new core::b_plus_tree::btree_t(resource_, fs_, p, key_getter));
                    db_[{database, collection}]->load();
                }
            }
        }
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

        path_t storage_directory = path_ / database / collection / base_index_name;
        create_directories(storage_directory);
        db_.emplace(collection_full_name_t{database, collection},
                    new core::b_plus_tree::btree_t(resource_, fs_, storage_directory, key_getter));
        return true;
    }

    bool disk_t::append_collection(const database_name_t& database, const collection_name_t& collection,
                                   table_storage_mode_t mode, const std::vector<catalog_column_entry_t>& columns) {
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

        path_t storage_directory = path_ / database / collection / base_index_name;
        create_directories(storage_directory);
        db_.emplace(collection_full_name_t{database, collection},
                    new core::b_plus_tree::btree_t(resource_, fs_, storage_directory, key_getter));
        return true;
    }

    bool disk_t::remove_collection(const database_name_t& database, const collection_name_t& collection) {
        if (catalog_.find_table(database, collection) == nullptr) {
            return false;
        }
        db_.erase({database, collection});
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

    void disk_t::migrate_from_legacy_(const path_t& metadata_path) {
        // Read old text format: "database:coll1;coll2;\n"
        auto handle = open_file(fs_, metadata_path, file_flags::READ, file_lock_type::NO_LOCK);
        auto fsize = handle->file_size();
        if (fsize == 0) return;

        std::unique_ptr<char[]> buffer(new char[fsize]);
        handle->read(buffer.get(), fsize);
        std::string data(buffer.get(), fsize);

        std::size_t pos_new_line = 0;
        auto pos_db = data.find(':', pos_new_line);
        while (pos_db != std::string::npos) {
            auto database = data.substr(pos_new_line, pos_db - pos_new_line);
            catalog_.append_database(database);
            auto pos = pos_db + 1;
            auto pos_col = data.find(';', pos);
            auto pos_end_line = data.find('\n', pos);
            while (pos_col != std::string::npos && pos_col < pos_end_line) {
                auto collection = data.substr(pos, pos_col - pos);
                catalog_table_entry_t entry;
                entry.name = collection;
                entry.storage_mode = table_storage_mode_t::IN_MEMORY;
                catalog_.append_table(database, entry);
                pos = pos_col + 1;
                pos_col = data.find(';', pos);
            }
            if (pos_end_line != std::string::npos) {
                pos_new_line = pos_end_line + 1;
                pos_db = data.find(':', pos_new_line);
            } else {
                break;
            }
        }
    }

} //namespace services::disk

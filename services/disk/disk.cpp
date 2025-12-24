#include "disk.hpp"
#include <components/document/msgpack/msgpack_encoder.hpp>

#include <core/b_plus_tree/msgpack_reader/msgpack_reader.hpp>

namespace services::disk {

    using namespace core::filesystem;

    constexpr static std::string_view base_index_name = "base_index";

    auto key_getter = [](const core::b_plus_tree::btree_t::item_data& item) -> core::b_plus_tree::btree_t::index_t {
        msgpack::unpacked msg;
        msgpack::unpack(msg, (char*) item.data, item.size, [](msgpack::type::object_type, std::size_t, void*) {
            return true;
        });
        return core::b_plus_tree::get_field(msg.get(), "/_id");
    };

    disk_t::disk_t(const path_t& storage_directory, std::pmr::memory_resource* resource)
        : path_(storage_directory)
        , resource_(resource)
        , fs_(core::filesystem::local_file_system_t())
        , db_(resource_)
        , metadata_(nullptr)
        , file_wal_id_(nullptr) {
        create_directories(storage_directory);
        metadata_ = metadata_t::open(fs_, storage_directory / "METADATA");
        file_wal_id_ = open_file(fs_,
                                 storage_directory / "WAL_ID",
                                 file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                                 file_lock_type::NO_LOCK);

        for (const auto& database : metadata_->databases()) {
            for (const auto& collection : metadata_->collections(database)) {
                path_t p = storage_directory / database / collection / base_index_name;
                db_.emplace(collection_full_name_t{database, collection},
                            new core::b_plus_tree::btree_t(resource_, fs_, p, key_getter));
                db_[{database, collection}]->load();
            }
        }
    }

    void disk_t::save_document(const database_name_t& database,
                               const collection_name_t& collection,
                               const document_ptr& document) {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, document);
        if (db_.find({database, collection}) == db_.end()) {
            metadata_->append_database(database);
            metadata_->append_collection(database, collection);
            db_[{database, collection}] =
                std::make_unique<core::b_plus_tree::btree_t>(resource_,
                                                             fs_,
                                                             path_ / database / collection / base_index_name,
                                                             key_getter);
        }
        db_[{database, collection}]->append(core::b_plus_tree::data_ptr_t(sbuf.data()), sbuf.size());
        db_[{database, collection}]->flush();
    }

    document_ptr disk_t::load_document(const database_name_t& database,
                                       const collection_name_t& collection,
                                       const document_id_t& id) const {
        auto item = db_.at({database, collection})->get_item(core::b_plus_tree::btree_t::index_t(id.to_string()), 0);

        if (item.data) {
            msgpack::unpacked msg;
            msgpack::unpack(msg, (char*) item.data, item.size);
            msgpack::object obj = msg.get();
            return components::document::to_document(obj, resource_);
        }
        return nullptr;
    }

    void disk_t::remove_document(const database_name_t& database,
                                 const collection_name_t& collection,
                                 const document_id_t& id) {
        db_.at({database, collection})->remove_index(core::b_plus_tree::btree_t::index_t(id.to_string()));
        db_.at({database, collection})->flush();
    }

    std::pmr::vector<document_id_t> disk_t::load_list_documents(const database_name_t& database,
                                                                const collection_name_t& collection) const {
        std::pmr::vector<document_id_t> id_documents;
        db_.at({database, collection})->full_scan(&id_documents, [](void* data, size_t size) {
            auto key = key_getter(core::b_plus_tree::btree_t::item_data{core::b_plus_tree::data_ptr_t(data), size});
            return document_id_t{key.value<components::types::physical_type::STRING>()};
        });

        return id_documents;
    }

    void disk_t::load_documents(const database_name_t& database,
                                const collection_name_t& collection,
                                std::pmr::vector<document_ptr>& result) const {
        result.reserve(db_.at({database, collection})->size());
        db_.at({database, collection})->full_scan(&result, [&](void* data, size_t size) {
            msgpack::unpacked msg;
            msgpack::unpack(msg, (char*) data, size);
            msgpack::object obj = msg.get();
            return components::document::to_document(obj, resource_);
        });
    }

    std::vector<database_name_t> disk_t::databases() const { return metadata_->databases(); }

    bool disk_t::append_database(const database_name_t& database) { return metadata_->append_database(database); }

    bool disk_t::remove_database(const database_name_t& database) {
        for (const auto& collection : metadata_->collections(database)) {
            remove_collection(database, collection);
        }
        return metadata_->remove_database(database);
    }

    std::vector<collection_name_t> disk_t::collections(const database_name_t& database) const {
        return metadata_->collections(database);
    }

    bool disk_t::append_collection(const database_name_t& database, const collection_name_t& collection) {
        if (db_.find({database, collection}) == db_.end()) {
            path_t storage_directory = path_ / database / collection / base_index_name;
            create_directories(storage_directory);
            db_.emplace(collection_full_name_t{database, collection},
                        new core::b_plus_tree::btree_t(resource_, fs_, storage_directory, key_getter));
        }
        return metadata_->append_collection(database, collection);
    }

    bool disk_t::remove_collection(const database_name_t& database, const collection_name_t& collection) {
        db_.erase({database, collection});
        core::filesystem::remove_directory(fs_, path_ / database / collection);
        return metadata_->remove_collection(database, collection);
    }

    void disk_t::fix_wal_id(wal::id_t wal_id) {
        auto id = std::to_string(wal_id);
        file_wal_id_->write(id.data(), id.size(), 0);
        file_wal_id_->truncate(id.size());
    }

    wal::id_t disk_t::wal_id() const { return wal::id_from_string(file_wal_id_->read_line()); }

} //namespace services::disk
#include "metadata.hpp"
#include <algorithm>

namespace services::disk {

    using namespace core::filesystem;
    metadata_t::metadata_ptr metadata_t::open(local_file_system_t& fs, const path_t& file_name) {
        return std::unique_ptr<metadata_t>(new metadata_t(fs, file_name));
    }

    metadata_t::databases_t metadata_t::databases() const {
        databases_t names;
        names.reserve(data_.size());
        for (const auto& it : data_) {
            names.emplace_back(it.first);
        }
        return names;
    }

    const metadata_t::collections_t& metadata_t::collections(const database_name_t& database) const {
        auto it = data_.find(database);
        if (it != data_.end()) {
            return it->second;
        }
        static const collections_t empty;
        return empty;
    }

    bool metadata_t::is_exists_database(const database_name_t& database) const {
        return data_.find(database) != data_.end();
    }

    bool metadata_t::append_database(const database_name_t& database, bool is_flush) {
        if (!is_exists_database(database)) {
            data_.insert_or_assign(database, collections_t());
            if (is_flush) {
                flush_();
            }
            return true;
        }
        return false;
    }

    bool metadata_t::remove_database(const database_name_t& database, bool is_flush) {
        bool result = data_.erase(database) > 0;
        if (is_flush && result) {
            flush_();
        }
        return result;
    }

    bool metadata_t::is_exists_collection(const database_name_t& database, const collection_name_t& collection) const {
        auto it = data_.find(database);
        if (it != data_.end()) {
            return std::find(it->second.begin(), it->second.end(), collection) != it->second.end();
        }
        return false;
    }

    bool
    metadata_t::append_collection(const database_name_t& database, const collection_name_t& collection, bool is_flush) {
        if (!is_exists_collection(database, collection)) {
            auto it = data_.find(database);
            if (it != data_.end()) {
                it->second.push_back(collection);
                if (is_flush) {
                    flush_();
                }
                return true;
            }
        }
        return false;
    }

    bool
    metadata_t::remove_collection(const database_name_t& database, const collection_name_t& collection, bool is_flush) {
        auto it = data_.find(database);
        if (it != data_.end()) {
            auto it_collection = std::remove(it->second.begin(), it->second.end(), collection);
            if (it_collection != it->second.end()) {
                it->second.erase(it_collection, it->second.end());
                if (is_flush) {
                    flush_();
                }
                return true;
            }
        }
        return false;
    }

    metadata_t::metadata_t(local_file_system_t& fs, const path_t& file_name)
        : file_(open_file(fs,
                          file_name,
                          file_flags::WRITE | file_flags::READ | file_flags::FILE_CREATE,
                          file_lock_type::NO_LOCK)) {
        std::unique_ptr<char[]> buffer(new char[file_->file_size()]);
        file_->read(buffer.get(), file_->file_size());
        std::string data(buffer.get(), file_->file_size());
        std::size_t pos_new_line = 0;
        auto pos_db = data.find(':', pos_new_line);
        while (pos_db != std::string::npos) {
            auto database = data.substr(pos_new_line, pos_db - pos_new_line);
            append_database(database, false);
            auto pos = pos_db + 1;
            auto pos_col = data.find(';', pos);
            auto pos_end_line = data.find('\n', pos);
            while (pos_col != std::string::npos && pos_col < pos_end_line) {
                auto collection = data.substr(pos, pos_col - pos);
                append_collection(database, collection, false);
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

    void metadata_t::flush_() {
        std::string data;
        for (auto& it : data_) {
            data += it.first + ":";
            for (const auto& collection : it.second) {
                data += collection + ";";
            }
            data += "\n";
        }
        file_->write(data.data(), data.size(), 0);
        file_->truncate(static_cast<int64_t>(data.size()));
    }

} //namespace services::disk
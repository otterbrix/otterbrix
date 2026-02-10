#include "result.hpp"

namespace services::disk {

    result_load_t::result_load_t(const std::vector<database_name_t>& databases, wal::id_t wal_id)
        : wal_id_(wal_id) {
        databases_.resize(databases.size());
        std::size_t i = 0;
        for (const auto& database : databases) {
            databases_[i++] = {database, {}};
        }
    }

    const result_load_t::result_t& result_load_t::operator*() const { return databases_; }

    std::vector<database_name_t> result_load_t::name_databases() const {
        std::vector<database_name_t> names(databases_.size());
        std::size_t i = 0;
        for (const auto& database : databases_) {
            names[i++] = database.name;
        }
        return names;
    }

    std::size_t result_load_t::count_collections() const {
        std::size_t count = 0;
        for (const auto& database : databases_) {
            count += database.collections.size();
        }
        return count;
    }

    void result_load_t::clear() {
        databases_.clear();
        wal_id_ = 0;
    }

    wal::id_t result_load_t::wal_id() const { return wal_id_; }

    result_load_t::result_t& result_load_t::operator*() { return databases_; }

    result_load_t result_load_t::empty() {
        result_load_t result;
        result.wal_id_ = 0;
        return result;
    }

} // namespace services::disk
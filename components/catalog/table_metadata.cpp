#include "table_metadata.hpp"

#include <sstream>
#include <stdexcept>

namespace components::catalog {
    table_metadata::table_metadata(std::pmr::memory_resource* resource,
                                   schema schema,
                                   const std::pmr::string& description)
        : schema_struct_(std::move(schema))
        , table_description_(description, resource)
        , last_updated_ms_(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()))
        , next_column_id_(schema_struct_.highest_field_id() + 1) {}

    const std::pmr::string& table_metadata::description() const { return table_description_; }

    field_id_t table_metadata::next_column_id() const { return next_column_id_; }

    timestamp table_metadata::last_updated_ms() const { return last_updated_ms_; }

    const schema& table_metadata::current_schema() const { return schema_struct_; }

    void table_metadata::set_table_oid(oid_t oid) {
        if (table_oid_ != INVALID_OID && table_oid_ != oid) {
            std::ostringstream oss;
            oss << "table_metadata::set_table_oid: OID is immutable after assignment (current="
                << table_oid_ << ", attempted=" << oid << ")";
            throw std::logic_error(oss.str());
        }
        table_oid_ = oid;
    }

    void table_metadata::set_next_column_oid(oid_t oid) {
        if (next_column_oid_ != INVALID_OID && next_column_oid_ != oid) {
            std::ostringstream oss;
            oss << "table_metadata::set_next_column_oid: counter seed is immutable after first set"
                << " (current=" << next_column_oid_ << ", attempted=" << oid << ")";
            throw std::logic_error(oss.str());
        }
        next_column_oid_ = oid;
    }
} // namespace components::catalog

#include "table_id.hpp"

#include <cassert>

namespace components::catalog {
    table_id::table_id(std::pmr::memory_resource* resource, const qualified_name_t& full_name)
        : namespace_parts_(resource)
        , name_(full_name.collection)
        , resource_(resource) {
        // Storage order is database-first (see header); empty parts omitted.
        if (!full_name.database.empty()) {
            namespace_parts_.emplace_back(full_name.database.c_str());
        }
        if (!full_name.schema.empty()) {
            namespace_parts_.emplace_back(full_name.schema.c_str());
        }
        if (!full_name.unique_identifier.empty()) {
            namespace_parts_.emplace_back(full_name.unique_identifier.c_str());
        }
    }

    const table_namespace_t& table_id::get_namespace() const { return namespace_parts_; }

    const std::pmr::string& table_id::table_name() const { return name_; }

    void table_id::set_oid(oid_t oid) {
        // OID is immutable after first assignment — programmer-error precondition.
        // Assert in debug, no-op in release if someone tries to reassign.
        assert((oid_ == INVALID_OID || oid_ == oid) && "table_id::set_oid: OID is immutable after assignment");
        if (oid_ != INVALID_OID && oid_ != oid) {
            return;
        }
        oid_ = oid;
    }
} // namespace components::catalog

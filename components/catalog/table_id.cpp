#include "table_id.hpp"

#include <cassert>
#include <cstdlib>

namespace components::catalog {
    table_id::table_id(std::pmr::memory_resource* resource, const qualified_name_t& full_name)
        : namespace_parts_(resource)
        // .c_str() + resource: constructing the pmr::string straight from the
        // std::string would silently land the copy on the DEFAULT resource.
        , name_(full_name.collection.c_str(), resource) {
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
        // OID is immutable after first assignment. Re-stamping the SAME value is an
        // idempotent no-op; a DIFFERENT value is a programmer error and dies loudly in
        // EVERY build (same pattern as oid_generator::allocate). An assert in debug that
        // silently keeps the first value under NDEBUG would let two identities of one
        // table diverge unseen, and there is no exception to throw either (rule 2).
        if (oid_ != INVALID_OID && oid_ != oid) [[unlikely]] {
            assert(false && "table_id::set_oid: OID is immutable after assignment");
            std::abort();
        }
        oid_ = oid;
    }
} // namespace components::catalog

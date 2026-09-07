#pragma once

#include "catalog_oids.hpp"
#include <components/base/collection_full_name.hpp>

#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace components::catalog {
    using table_namespace_t = std::pmr::vector<std::pmr::string>;

    class table_id {
    public:
        table_id(std::pmr::memory_resource* resource, const qualified_name_t& full_name);

        // Storage order is database-first: [database, schema?, uid?], empty parts omitted.
        [[nodiscard]] const table_namespace_t& get_namespace() const;
        [[nodiscard]] std::string_view database() const noexcept {
            return namespace_parts_.empty() ? std::string_view{} : std::string_view(namespace_parts_.front());
        }
        [[nodiscard]] const std::pmr::string& table_name() const;

        // pg_class.oid; INVALID_OID until the CREATE TABLE pipeline assigns it. Equality/hashing
        // is by name, not oid — the oid is purely a join tag (pg_attribute.attrelid, pg_depend.refobjid).
        [[nodiscard]] oid_t oid() const noexcept { return oid_; }
        // Immutable after first non-INVALID assignment; changing to a different value ABORTS in
        // every build (no exception to throw) rather than silently diverging two identities of the same table.
        void set_oid(oid_t oid);

    private:
        table_namespace_t namespace_parts_;
        std::pmr::string name_;
        oid_t oid_{INVALID_OID};
    };
} // namespace components::catalog

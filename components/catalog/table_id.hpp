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

        // Namespace STORAGE order is database-first: [database, schema?, uid?],
        // empty parts omitted — so database() is front() whenever a database
        // was given. Consumers (check_namespace_exists / check_collection_exists
        // / the type-search-path builder) read the database through database().
        [[nodiscard]] const table_namespace_t& get_namespace() const;
        [[nodiscard]] std::string_view database() const noexcept {
            return namespace_parts_.empty() ? std::string_view{} : std::string_view(namespace_parts_.front());
        }
        [[nodiscard]] const std::pmr::string& table_name() const;

        // pg_class.oid for this table. INVALID_OID until assigned by the CREATE TABLE
        // pipeline (build_create_table_writes / operator_create_collection) —
        // pre-existing in-memory table_id values stay INVALID_OID, which is fine:
        // hashing/equality is by name, the OID is purely an identity tag for catalog
        // joins (pg_attribute.attrelid, pg_depend.refobjid, etc).
        [[nodiscard]] oid_t oid() const noexcept { return oid_; }
        // Immutable after first non-INVALID assignment: re-stamping the same value is a
        // no-op; changing to a different value is a programmer error and ABORTS — loud
        // in every build, debug and NDEBUG alike (rule 2: no exceptions to throw, and a
        // silent no-op would let two identities of the same table diverge unseen).
        void set_oid(oid_t oid);

    private:
        table_namespace_t namespace_parts_;
        std::pmr::string name_;
        oid_t oid_{INVALID_OID};
    };
} // namespace components::catalog

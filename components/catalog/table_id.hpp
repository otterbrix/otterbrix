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
        table_id(std::pmr::memory_resource* resource, std::pmr::vector<std::pmr::string> full_name);
        table_id(std::pmr::memory_resource* resource, table_namespace_t ns, std::pmr::string name);
        table_id(std::pmr::memory_resource* resource, const qualified_name_t& full_name);

        [[nodiscard]] const table_namespace_t& get_namespace() const;
        [[nodiscard]] const std::pmr::string& table_name() const;
        [[nodiscard]] std::pmr::string to_pmr_string() const;

        // pg_class.oid for this table. INVALID_OID until assigned by the CREATE TABLE
        // pipeline (build_create_table_writes / operator_create_collection) —
        // pre-existing in-memory table_id values stay INVALID_OID, which is fine:
        // hashing/equality is by name, the OID is purely an identity tag for catalog
        // joins (pg_attribute.attrelid, pg_depend.refobjid, etc).
        [[nodiscard]] oid_t oid() const noexcept { return oid_; }
        // Immutable after first non-INVALID assignment: re-stamping the same value is a no-op,
        // changing to a different value throws std::logic_error.
        void set_oid(oid_t oid);

    private:
        table_namespace_t namespace_parts_;
        std::pmr::string name_;
        std::pmr::memory_resource* resource_;
        oid_t oid_{INVALID_OID};
    };
} // namespace components::catalog

namespace std {
    template<>
    struct hash<components::catalog::table_id> {
        std::size_t operator()(const components::catalog::table_id& id) const noexcept {
            return std::hash<std::pmr::string>{}(id.to_pmr_string());
        }
    };
} // namespace std

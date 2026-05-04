#pragma once

#include "catalog_oids.hpp"
#include "schema.hpp"

#include <chrono>

namespace components::catalog {
    struct table_metadata {
        table_metadata(std::pmr::memory_resource* resource, schema schema, const std::pmr::string& description = "");

        [[nodiscard]] const std::pmr::string& description() const;
        [[nodiscard]] field_id_t next_column_id() const;
        [[nodiscard]] timestamp last_updated_ms() const;
        [[nodiscard]] const schema& current_schema() const;

        [[nodiscard]] oid_t table_oid() const noexcept { return table_oid_; }
        // Immutable after first non-INVALID assignment: re-stamping the same value is a no-op,
        // changing to a different value throws std::logic_error.
        void set_table_oid(oid_t oid);

        // Monotonic counter for pg_attribute.attoid. Never reused on DROP COLUMN.
        // The setter seeds the counter once at restore; subsequent calls with the same value
        // are idempotent, but seeding to a different value throws std::logic_error. Use
        // advance_next_column_oid() to bump the counter after seeding.
        [[nodiscard]] oid_t next_column_oid() const noexcept { return next_column_oid_; }
        void set_next_column_oid(oid_t oid);
        oid_t advance_next_column_oid() noexcept { return next_column_oid_++; }

    private:
        schema schema_struct_;
        std::pmr::string table_description_;
        timestamp last_updated_ms_;
        field_id_t next_column_id_;
        oid_t table_oid_{INVALID_OID};
        oid_t next_column_oid_{INVALID_OID};
    };

    enum class used_format_t : uint8_t
    {
        documents = 0,
        columns = 1,
        undefined = 2
    };
} // namespace components::catalog

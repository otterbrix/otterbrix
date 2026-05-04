#pragma once

#include "catalog_error.hpp"
#include "catalog_oids.hpp"

#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>

#include <memory_resource>
#include <optional>
#include <unordered_map>

namespace components::catalog {
    using schema_version_t = uint64_t;
    using field_id_t = uint64_t;

    class schema {
    public:
        using field_description_cref = std::reference_wrapper<const types::field_description>;

        explicit schema(std::pmr::memory_resource* resource,
                        const std::vector<table::column_definition_t>& columns,
                        const std::vector<types::field_description>& descriptions,
                        const std::pmr::vector<field_id_t>& primary_key = {});

        cursor::cursor_t_ptr find_field(field_id_t id) const;
        cursor::cursor_t_ptr find_field(const std::pmr::string& name) const;

        // Lookup by pg_attribute.attoid (column-level OID assigned by ddl_create_table /
        // ddl_add_column). Returns nullopt if no column carries this OID. INVALID_OID
        // never matches even if a column has not yet been stamped.
        [[nodiscard]] std::optional<std::reference_wrapper<const table::column_definition_t>>
        find_field_by_oid(oid_t oid) const noexcept;

        // Snapshot of current attoids in column order. Useful for callers that want to
        // round-trip OIDs (e.g. dispatcher cache → pg_attribute scan). Entries equal to
        // INVALID_OID indicate columns that haven't been stamped yet.
        [[nodiscard]] std::vector<oid_t> column_oids() const;

        [[nodiscard]] std::optional<field_description_cref> get_field_description(field_id_t id) const;
        [[nodiscard]] std::optional<field_description_cref> get_field_description(const std::pmr::string& name) const;

        [[nodiscard]] const std::pmr::vector<field_id_t>& primary_key() const;
        [[nodiscard]] const std::vector<table::column_definition_t>& columns() const;
        [[nodiscard]] const std::vector<types::field_description>& descriptions() const;
        [[nodiscard]] field_id_t highest_field_id() const;

        [[nodiscard]] const catalog_error& error() const;
        [[nodiscard]] std::vector<types::complex_logical_type> types() const;

        // pg_namespace.oid for the schema this struct belongs to. INVALID_OID until set
        // by ddl_create_namespace (M3); the value is an attribute, not part of identity.
        // Immutable after first non-INVALID assignment: re-stamping the same value is a no-op,
        // changing to a different value throws std::logic_error.
        [[nodiscard]] oid_t schema_oid() const noexcept { return schema_oid_; }
        void set_schema_oid(oid_t oid);

    private:
        size_t find_idx_by_id(field_id_t id) const;
        size_t find_idx_by_name(const std::pmr::string& name) const;

        std::vector<components::table::column_definition_t> columns_;
        std::vector<types::field_description> descriptions_;
        std::pmr::vector<field_id_t> primary_key_field_ids_;
        std::pmr::unordered_map<field_id_t, size_t> id_to_struct_idx_;
        field_id_t highest_ = 0;
        mutable catalog_error error_;
        std::pmr::memory_resource* resource_;
        oid_t schema_oid_{INVALID_OID};
    };
} // namespace components::catalog

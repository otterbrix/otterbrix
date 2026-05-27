#pragma once
#include <components/types/logical_value.hpp>
#include <cstdint>
#include <unordered_map>

#include "storage/file_buffer.hpp"

#include <optional>

namespace components::table {

    class column_definition_t {
    public:
        column_definition_t(std::string name, types::complex_logical_type type);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            std::optional<types::logical_value_t> default_value);
        column_definition_t(std::string name, types::complex_logical_type type, bool not_null);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            bool not_null,
                            std::optional<types::logical_value_t> default_value);
        column_definition_t(const column_definition_t&) = default;
        column_definition_t& operator=(const column_definition_t&) = default;
        column_definition_t(column_definition_t&&) = default;
        column_definition_t& operator=(column_definition_t&&) = default;

        const types::logical_value_t& default_value() const;
        const std::optional<types::logical_value_t>& default_value_opt() const;
        bool has_default_value() const;
        void set_default_value(std::optional<types::logical_value_t> default_value);

        bool is_not_null() const;
        void set_not_null(bool v);

        const types::complex_logical_type& type() const;
        types::complex_logical_type& type();

        const std::string& name() const;
        void set_name(const std::string& name);

        uint64_t storage_oid() const;
        void set_storage_oid(uint64_t storage_oid);

        uint64_t logical() const;
        uint64_t physical() const;

        uint64_t oid() const;
        void set_oid(uint64_t oid);

        // pg_attribute.attoid (uint32_t, 0 = unset). Distinct from oid()/storage_oid().
        // Immutable after first non-zero assignment: re-stamping the same value is a no-op,
        // changing to a different value throws std::logic_error.
        std::uint32_t attoid() const noexcept { return attoid_; }
        void set_attoid(std::uint32_t v);

        // Column→type pg_depend: pg_type.oid for this column's type.
        // 0 = unresolved (built-ins fall back to well-known OIDs in the writer).
        std::uint32_t atttypid() const noexcept { return atttypid_; }
        void set_atttypid(std::uint32_t v) noexcept { atttypid_ = v; }

    private:
        std::string name_;
        types::complex_logical_type type_;
        uint64_t storage_oid_ = storage::INVALID_INDEX;
        uint64_t oid_ = storage::INVALID_INDEX;
        std::uint32_t attoid_{0};   // catalog::INVALID_OID; not included via header to avoid cycle.
        std::uint32_t atttypid_{0}; // catalog::INVALID_OID; resolved by the CREATE TABLE pipeline.
        bool not_null_{false};
        std::optional<types::logical_value_t> default_value_;
        std::unordered_map<std::string, std::string> tags_;
    };

} // namespace components::table
#pragma once
#include <components/types/types.hpp>
#include <components/vector/vector.hpp>
#include <cstdint>
#include <unordered_map>

#include "row_version_manager.hpp"
#include "storage/file_buffer.hpp"

#include <optional>

namespace components::table {

    class column_definition_t {
    public:
        column_definition_t(std::string name, types::complex_logical_type type);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            std::optional<vector::vector_t>&& default_value);
        column_definition_t(std::string name, types::complex_logical_type type, bool not_null);
        column_definition_t(std::string name,
                            types::complex_logical_type type,
                            bool not_null,
                            std::optional<vector::vector_t>&& default_value);
        // vector_t copies are shallow and a column_definition_t crosses actors, so the copy owns its own default.
        column_definition_t(const column_definition_t& other);
        column_definition_t& operator=(const column_definition_t& other);
        column_definition_t(column_definition_t&&) = default;
        column_definition_t& operator=(column_definition_t&&) = default;

        // One-row vector; a DEFAULT NULL is a null row of the column's type, not an absent default.
        const vector::vector_t& default_value() const;
        const std::optional<vector::vector_t>& default_value_opt() const;
        bool has_default_value() const;
        void set_default_value(std::optional<vector::vector_t>&& default_value);

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
        // Immutable after first non-zero assignment: re-stamping the same value is a no-op;
        // a DIFFERENT value is refused (the first stamp stays) and reported on stderr, in
        // every build. It does NOT throw: there is no exception channel here.
        std::uint32_t attoid() const noexcept { return attoid_; }
        void set_attoid(std::uint32_t v);

        // Column→type pg_depend: pg_type.oid for this column's type.
        // 0 = unresolved (built-ins fall back to well-known OIDs in the writer).
        std::uint32_t atttypid() const noexcept { return atttypid_; }
        void set_atttypid(std::uint32_t v) noexcept { atttypid_ = v; }

        [[nodiscard]] uint64_t added_at() const noexcept { return added_at_; }
        void set_added_at(uint64_t stamp) noexcept { added_at_ = stamp; }
        [[nodiscard]] uint64_t dropped_at() const noexcept { return dropped_at_; }
        void set_dropped_at(uint64_t stamp) noexcept { dropped_at_ = stamp; }
        [[nodiscard]] uint64_t added_sub_id() const noexcept { return added_sub_id_; }
        void set_added_sub_id(uint64_t sub_id) noexcept { added_sub_id_ = sub_id; }
        [[nodiscard]] uint64_t dropped_sub_id() const noexcept { return dropped_sub_id_; }
        void set_dropped_sub_id(uint64_t sub_id) noexcept { dropped_sub_id_ = sub_id; }

        [[nodiscard]] bool visible_to(const transaction_data& txn) const noexcept {
            return version_visible(txn, added_at_) && !version_visible(txn, dropped_at_);
        }

        // NOT NULL a PRIMARY KEY added by ALTER brings: stamped with its transaction until COMMIT makes it
        // is_not_null().
        [[nodiscard]] uint64_t not_null_at() const noexcept { return not_null_at_; }
        [[nodiscard]] uint64_t not_null_sub_id() const noexcept { return not_null_sub_id_; }
        void stamp_not_null(uint64_t stamp, uint64_t sub_id) noexcept {
            not_null_at_ = stamp;
            not_null_sub_id_ = sub_id;
        }
        [[nodiscard]] bool not_null_for(const transaction_data& txn) const noexcept {
            return not_null_ || version_visible(txn, not_null_at_);
        }

    private:
        std::string name_;
        types::complex_logical_type type_;
        uint64_t storage_oid_ = storage::INVALID_INDEX;
        uint64_t oid_ = storage::INVALID_INDEX;
        std::uint32_t attoid_{0};   // catalog::INVALID_OID; not included via header to avoid cycle.
        std::uint32_t atttypid_{0}; // catalog::INVALID_OID; resolved by the CREATE TABLE pipeline.
        uint64_t added_at_{0};
        uint64_t dropped_at_{NOT_DELETED_ID};
        uint64_t added_sub_id_{0};
        uint64_t dropped_sub_id_{0};
        uint64_t not_null_at_{NOT_DELETED_ID};
        uint64_t not_null_sub_id_{0};
        bool not_null_{false};
        std::optional<vector::vector_t> default_value_;
        std::unordered_map<std::string, std::string> tags_;
    };

} // namespace components::table
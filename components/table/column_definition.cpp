#include "column_definition.hpp"
#include "table_state.hpp"

#include <cassert>
#include <sstream>
#include <stdexcept>

namespace components::table {

    column_definition_t::column_definition_t(std::string name, types::complex_logical_type type)
        : name_(std::move(name))
        , type_(std::move(type)) {}

    column_definition_t::column_definition_t(std::string name,
                                             types::complex_logical_type type,
                                             std::optional<types::logical_value_t> default_value)
        : name_(std::move(name))
        , type_(std::move(type))
        , default_value_(std::move(default_value)) {}

    column_definition_t::column_definition_t(std::string name, types::complex_logical_type type, bool not_null)
        : name_(std::move(name))
        , type_(std::move(type))
        , not_null_(not_null) {}

    column_definition_t::column_definition_t(std::string name,
                                             types::complex_logical_type type,
                                             bool not_null,
                                             std::optional<types::logical_value_t> default_value)
        : name_(std::move(name))
        , type_(std::move(type))
        , not_null_(not_null)
        , default_value_(std::move(default_value)) {}

    const types::logical_value_t& column_definition_t::default_value() const {
        assert(has_default_value() && "default_value() called on a column without a default value");
        return *default_value_;
    }

    const std::optional<types::logical_value_t>& column_definition_t::default_value_opt() const {
        return default_value_;
    }

    bool column_definition_t::has_default_value() const { return default_value_.has_value(); }

    void column_definition_t::set_default_value(std::optional<types::logical_value_t> default_value) {
        default_value_ = std::move(default_value);
    }

    bool column_definition_t::is_not_null() const { return not_null_; }
    void column_definition_t::set_not_null(bool v) { not_null_ = v; }

    const types::complex_logical_type& column_definition_t::type() const { return type_; }

    types::complex_logical_type& column_definition_t::type() { return type_; }

    const std::string& column_definition_t::name() const { return name_; }
    void column_definition_t::set_name(const std::string& name) { name_ = name; }

    uint64_t column_definition_t::storage_oid() const { return storage_oid_; }

    uint64_t column_definition_t::logical() const { return oid_; }

    uint64_t column_definition_t::physical() const { return storage_oid_; }

    void column_definition_t::set_storage_oid(uint64_t storage_oid) { storage_oid_ = storage_oid; }

    uint64_t column_definition_t::oid() const { return oid_; }

    void column_definition_t::set_oid(uint64_t oid) { oid_ = oid; }

    void column_definition_t::set_attoid(std::uint32_t v) {
        // attoid is immutable after first assignment — programmer-error precondition.
        // Hot DDL/resolve path: drop the throw, assert in debug, no-op in release if
        // someone tries to reassign with a different value.
        assert((attoid_ == 0 || attoid_ == v) &&
               "column_definition_t::set_attoid: attoid is immutable after assignment");
        if (attoid_ != 0 && attoid_ != v) {
            return;
        }
        attoid_ = v;
    }

    types::logical_value_t reconcile_to_fixed_array(std::pmr::memory_resource* resource,
                                                    const types::logical_value_t& value,
                                                    const column_definition_t& column,
                                                    core::date::timezone_offset_t session_tz) {
        using namespace components::types;
        assert(column.type().type() == logical_type::ARRAY &&
               "reconcile_to_fixed_array: column is not an ARRAY column");

        const auto& elem_type = column.type().child_type();
        const auto target_size = static_cast<const array_logical_type_extension*>(column.type().extension())->size();
        const auto& src = value.children();

        // The column DEFAULT, when present, supplies the per-position pad values; it is an
        // ARRAY value whose children() line up with the column's element slots.
        const std::vector<logical_value_t>* default_elems = nullptr;
        if (column.has_default_value() && column.default_value().type().type() == logical_type::ARRAY) {
            default_elems = &column.default_value().children();
        }

        std::vector<logical_value_t> elems;
        elems.reserve(target_size);
        for (uint64_t i = 0; i < target_size; ++i) {
            if (i < src.size()) {
                // Within the provided value: cast the element to the target type. Over-long
                // values are truncated implicitly (the loop stops at target_size).
                elems.emplace_back(src[i].cast_as(elem_type, session_tz));
            } else if (default_elems && i < default_elems->size()) {
                // Short value: pad slot i from the column DEFAULT at the same position.
                elems.emplace_back((*default_elems)[i].cast_as(elem_type, session_tz));
            } else if (!column.is_not_null()) {
                // Nullable column with no usable default: pad with NULL.
                elems.emplace_back(logical_value_t{resource, complex_logical_type{logical_type::NA}});
            } else {
                // NOT NULL column with no default to pad from: the short value cannot fill
                // the fixed array. Signal the caller with an NA value.
                return logical_value_t{resource, complex_logical_type{logical_type::NA}};
            }
        }
        return logical_value_t::create_array(resource, elem_type, elems);
    }

} // namespace components::table
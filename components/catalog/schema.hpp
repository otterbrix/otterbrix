#pragma once

#include "catalog_error.hpp"
#include "catalog_types.hpp"

#include <components/cursor/cursor.hpp>

#include <algorithm>
#include <memory_resource>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace components::catalog {
    class schema {
    public:
        using field_description_cref = std::reference_wrapper<const types::field_description>;

        explicit schema(std::pmr::memory_resource* resource,
                        const components::types::complex_logical_type& schema_struct,
                        const std::pmr::vector<field_id_t>& primary_key = {});

        cursor::cursor_t_ptr find_field(field_id_t id) const;
        cursor::cursor_t_ptr find_field(const std::pmr::string& name) const;

        [[nodiscard]] std::optional<field_description_cref> get_field_description(field_id_t id) const;
        [[nodiscard]] std::optional<field_description_cref> get_field_description(const std::pmr::string& name) const;

        [[nodiscard]] const std::pmr::vector<field_id_t>& primary_key() const;
        [[nodiscard]] const std::vector<types::complex_logical_type>& columns() const;
        [[nodiscard]] const std::vector<types::field_description>& descriptions() const;
        [[nodiscard]] field_id_t highest_field_id() const;

        [[nodiscard]] const catalog_error& error() const;
        [[nodiscard]] types::complex_logical_type schema_struct() const;

    private:
        size_t find_idx_by_id(field_id_t id) const;
        size_t find_idx_by_name(const std::pmr::string& name) const;

        types::complex_logical_type schema_struct_;
        std::pmr::vector<field_id_t> primary_key_field_ids_;
        std::pmr::unordered_map<field_id_t, size_t> id_to_struct_idx_;
        field_id_t highest_ = 0;
        mutable catalog_error error_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog

#pragma once

#include "catalog_types.hpp"

#include <components/cursor/cursor.hpp>
#include <components/table/column_definition.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <optional>
#include <unordered_map>

namespace components::catalog {
    class schema {
    public:
        using field_description_cref = std::reference_wrapper<const types::field_description>;

        explicit schema(std::pmr::memory_resource* resource,
                        const std::vector<table::column_definition_t>& columns,
                        const std::vector<types::field_description>& descriptions,
                        const std::pmr::vector<field_id_t>& primary_key = {});

        [[nodiscard]] core::result_wrapper_t<types::complex_logical_type> find_field(field_id_t id) const;
        [[nodiscard]] core::result_wrapper_t<types::complex_logical_type>
        find_field(const std::pmr::string& name) const;

        [[nodiscard]] core::result_wrapper_t<field_description_cref> get_field_description(field_id_t id) const;
        [[nodiscard]] core::result_wrapper_t<field_description_cref>
        get_field_description(const std::pmr::string& name) const;

        [[nodiscard]] const std::pmr::vector<field_id_t>& primary_key() const;
        [[nodiscard]] const std::vector<table::column_definition_t>& columns() const;
        [[nodiscard]] const std::vector<types::field_description>& descriptions() const;
        [[nodiscard]] field_id_t highest_field_id() const;

        [[nodiscard]] const core::error_t& error() const;
        [[nodiscard]] std::vector<types::complex_logical_type> types() const;

    private:
        size_t find_idx_by_id(field_id_t id) const;
        size_t find_idx_by_name(const std::pmr::string& name) const;

        std::vector<components::table::column_definition_t> columns_;
        std::vector<types::field_description> descriptions_;
        std::pmr::vector<field_id_t> primary_key_field_ids_;
        std::pmr::unordered_map<field_id_t, size_t> id_to_struct_idx_;
        field_id_t highest_ = 0;
        mutable core::error_t error_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog

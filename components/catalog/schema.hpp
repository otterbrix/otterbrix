#pragma once

#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <cstdint>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::catalog {

    using field_id_t = std::uint32_t;

    class schema {
    public:
        schema() = default;

        explicit schema(std::pmr::memory_resource* resource,
                        const std::vector<table::column_definition_t>& columns,
                        const std::vector<types::field_description>& descriptions,
                        const std::pmr::vector<field_id_t>& primary_key = {})
            : columns_(columns)
            , descriptions_(descriptions)
            , primary_key_(primary_key, resource) {}

        [[nodiscard]] const std::vector<table::column_definition_t>& columns() const { return columns_; }
        [[nodiscard]] const std::vector<types::field_description>& descriptions() const { return descriptions_; }
        [[nodiscard]] const std::pmr::vector<field_id_t>& primary_key() const { return primary_key_; }

        [[nodiscard]] std::vector<types::complex_logical_type> types() const {
            std::vector<types::complex_logical_type> out;
            out.reserve(columns_.size());
            for (const auto& column : columns_) {
                out.emplace_back(column.type());
            }
            return out;
        }

    private:
        std::vector<table::column_definition_t> columns_;
        std::vector<types::field_description> descriptions_;
        std::pmr::vector<field_id_t> primary_key_;
    };

} // namespace components::catalog

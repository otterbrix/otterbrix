#pragma once

#include <components/types/types.hpp>
#include <unordered_map>
#include <vector>

namespace components::catalog {
    class computed_schema {
    public:
        explicit computed_schema(std::pmr::memory_resource* resource);

        // Add a (field_name, type) pair. No-op if already present.
        void append(std::pmr::string field_name, const types::complex_logical_type& type);
        void append_n(std::pmr::string field_name, const types::complex_logical_type& type, size_t n);

        // Remove a (field_name, type) pair.
        void drop(std::pmr::string field_name, const types::complex_logical_type& type);
        void drop_n(std::pmr::string field_name, const types::complex_logical_type& type, size_t n);

        [[nodiscard]] std::vector<types::complex_logical_type>
        find_field_versions(const std::pmr::string& field_name) const;

        [[nodiscard]] types::complex_logical_type latest_types_struct() const;

        // Physical storage column name: "field_name__TYPENAME"
        [[nodiscard]] static std::string storage_column_name(const std::string& field_name,
                                                             const types::complex_logical_type& type);

        [[nodiscard]] bool has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const;

    private:
        // field_name -> list of types currently present
        std::pmr::unordered_map<std::pmr::string,
                                std::pmr::vector<types::complex_logical_type>>
            fields_;

        // Preserves insertion order of (field_name, type) pairs for physical column ordering.
        std::pmr::vector<std::pair<std::pmr::string, types::complex_logical_type>> column_order_;
    };
} // namespace components::catalog

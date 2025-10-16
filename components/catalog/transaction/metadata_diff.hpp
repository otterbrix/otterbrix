#pragma once

#include "schema_diff.hpp"
#include <components/catalog/table_metadata.hpp>

namespace components::catalog {
    struct metadata_diff {
        explicit metadata_diff(std::pmr::memory_resource* resource);

        void update_description(const std::pmr::string& desc);
        void use_schema_diff(components::catalog::schema_diff&& diff);

        [[nodiscard]] bool has_changes() const;
        [[nodiscard]] table_metadata apply(const table_metadata& base_metadata) const;

    private:
        schema_diff schema_diff_;
        std::optional<std::pmr::string> new_desc_;
        std::pmr::memory_resource* resource_;
    };
} // namespace components::catalog

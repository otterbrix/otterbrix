#pragma once

#include <components/catalog/schema.hpp>

namespace components::catalog {

    class table_metadata {
    public:
        table_metadata() = default;

        table_metadata(std::pmr::memory_resource* /*resource*/, schema table_schema)
            : schema_(std::move(table_schema)) {}

        [[nodiscard]] const schema& get_schema() const { return schema_; }

    private:
        schema schema_;
    };

} // namespace components::catalog

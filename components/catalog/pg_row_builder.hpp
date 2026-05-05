#pragma once

// Helpers for building single-row data_chunk_t values for pg_catalog writes.
// Mirrors the lv_* / make_row helpers in manager_disk_impl.hpp so that
// ddl_metadata_builder (a pure catalog library) can build pg_class / pg_attribute
// rows without depending on disk-private headers.

#include <components/table/column_definition.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace components::catalog {

    inline types::logical_value_t lv_oid(std::pmr::memory_resource* r, oid_t v) {
        return types::logical_value_t(r, v);
    }
    inline types::logical_value_t lv_str(std::pmr::memory_resource* r, std::string v) {
        return types::logical_value_t(r, std::move(v));
    }
    inline types::logical_value_t lv_i32(std::pmr::memory_resource* r, std::int32_t v) {
        return types::logical_value_t(r, v);
    }
    inline types::logical_value_t lv_bool(std::pmr::memory_resource* r, bool v) {
        return types::logical_value_t(r, v);
    }

    // Build a single-row data_chunk_t whose schema is derived from `columns`.
    // `fill` receives (chunk, resource) and must call chunk.set_value(col, 0, lv_*(...)).
    template<typename FillFn>
    vector::data_chunk_t make_pg_row(
        std::pmr::memory_resource*                      resource,
        const std::vector<table::column_definition_t>&  columns,
        FillFn&&                                         fill)
    {
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(columns.size());
        for (const auto& col : columns) {
            types.push_back(col.type());
        }
        vector::data_chunk_t chunk(resource, types, 1);
        chunk.set_cardinality(1);
        fill(chunk, resource);
        return chunk;
    }

} // namespace components::catalog
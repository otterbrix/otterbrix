#include "ddl_metadata_builder.hpp"

#include <components/catalog/system_table_schemas.hpp>
#include <components/types/logical_value.hpp>

#include <stdexcept>

namespace components::catalog {

namespace {

    inline types::logical_value_t lv_oid(std::pmr::memory_resource* r, oid_t v) {
        return types::logical_value_t(r, v);
    }
    inline types::logical_value_t lv_str(std::pmr::memory_resource* r, std::string v) {
        return types::logical_value_t(r, std::move(v));
    }
    inline types::logical_value_t lv_bool(std::pmr::memory_resource* r, bool v) {
        return types::logical_value_t(r, v);
    }
    inline types::logical_value_t lv_i32(std::pmr::memory_resource* r, std::int32_t v) {
        return types::logical_value_t(r, v);
    }
    inline types::logical_value_t lv_i32null(std::pmr::memory_resource* r) {
        return types::logical_value_t(r, types::logical_type::INTEGER);  // null integer
    }

    // Build a one-row chunk using the given column definitions.
    vector::data_chunk_t one_row_chunk(
        std::pmr::memory_resource*                     resource,
        const std::vector<table::column_definition_t>& columns)
    {
        std::pmr::vector<types::complex_logical_type> col_types(resource);
        col_types.reserve(columns.size());
        for (const auto& col : columns) {
            col_types.push_back(col.type());
        }
        vector::data_chunk_t chunk(resource, col_types, 1);
        chunk.set_cardinality(1);
        return chunk;
    }

} // namespace

vector::data_chunk_t build_pg_class_row(
    std::pmr::memory_resource* resource,
    const pg_class_build_t&    p)
{
    const auto* def = find_system_table("pg_class");
    if (!def) return vector::data_chunk_t{};

    auto chunk = one_row_chunk(resource, def->columns);
    chunk.set_value(0, 0, lv_oid(resource, p.table_oid));
    chunk.set_value(1, 0, lv_str(resource, p.relname));
    chunk.set_value(2, 0, lv_oid(resource, p.namespace_oid));
    chunk.set_value(3, 0, lv_str(resource, std::string(1, p.relkind)));
    chunk.set_value(4, 0, lv_str(resource, std::string(1, p.relstoragemode)));
    return chunk;
}

std::vector<vector::data_chunk_t> build_pg_attribute_rows(
    std::pmr::memory_resource*               resource,
    const std::vector<pg_attribute_build_t>& attrs)
{
    const auto* def = find_system_table("pg_attribute");
    if (!def) return {};

    std::vector<vector::data_chunk_t> rows;
    rows.reserve(attrs.size());
    for (const auto& a : attrs) {
        auto chunk = one_row_chunk(resource, def->columns);
        chunk.set_value(0, 0, lv_oid (resource, a.attoid));
        chunk.set_value(1, 0, lv_oid (resource, a.attrelid));
        chunk.set_value(2, 0, lv_str (resource, a.attname));
        chunk.set_value(3, 0, lv_oid (resource, a.atttypid));
        chunk.set_value(4, 0, lv_i32 (resource, a.attnum));
        chunk.set_value(5, 0, lv_bool(resource, a.attnotnull));
        chunk.set_value(6, 0, lv_bool(resource, a.atthasdefault));
        chunk.set_value(7, 0, lv_bool(resource, false));    // attisdropped
        chunk.set_value(8, 0, lv_str (resource, a.atttypspec));
        chunk.set_value(9, 0, lv_str (resource, a.attdefspec));
        rows.push_back(std::move(chunk));
    }
    return rows;
}

vector::data_chunk_t build_pg_constraint_row(
    std::pmr::memory_resource*    resource,
    const pg_constraint_build_t&  p)
{
    const auto* def = find_system_table("pg_constraint");
    if (!def) return vector::data_chunk_t{};

    auto chunk = one_row_chunk(resource, def->columns);
    chunk.set_value( 0, 0, lv_oid(resource, p.con_oid));
    chunk.set_value( 1, 0, lv_str(resource, p.conname));
    chunk.set_value( 2, 0, lv_oid(resource, p.conrelid));
    chunk.set_value( 3, 0, lv_str(resource, std::string(1, p.contype)));
    chunk.set_value( 4, 0, lv_oid(resource, p.confrelid));
    chunk.set_value( 5, 0, lv_str(resource, p.conkey));
    chunk.set_value( 6, 0, lv_str(resource, p.confkey));
    chunk.set_value( 7, 0, lv_str(resource, std::string(1, p.matchtype)));
    chunk.set_value( 8, 0, lv_str(resource, std::string(1, p.deltype)));
    chunk.set_value( 9, 0, lv_str(resource, std::string(1, p.updtype)));
    chunk.set_value(10, 0, lv_str(resource, p.conexpr));
    return chunk;
}

vector::data_chunk_t build_pg_depend_row(
    std::pmr::memory_resource* resource,
    oid_t classid,    oid_t objid,
    oid_t refclassid, oid_t refobjid,
    char  deptype,
    std::int32_t objsubid,
    std::int32_t refobjsubid)
{
    const auto* def = find_system_table("pg_depend");
    if (!def) return vector::data_chunk_t{};

    auto chunk = one_row_chunk(resource, def->columns);
    chunk.set_value(0, 0, lv_oid(resource, classid));
    chunk.set_value(1, 0, lv_oid(resource, objid));
    chunk.set_value(2, 0, lv_oid(resource, refclassid));
    chunk.set_value(3, 0, lv_oid(resource, refobjid));
    chunk.set_value(4, 0, lv_str(resource, std::string(1, deptype)));
    chunk.set_value(5, 0, lv_i32(resource, objsubid));
    chunk.set_value(6, 0, lv_i32(resource, refobjsubid));
    return chunk;
}

vector::data_chunk_t build_pg_namespace_row(
    std::pmr::memory_resource* resource,
    oid_t                      ns_oid,
    const std::string&         nspname)
{
    const auto* def = find_system_table("pg_namespace");
    if (!def) return vector::data_chunk_t{};

    auto chunk = one_row_chunk(resource, def->columns);
    chunk.set_value(0, 0, lv_oid(resource, ns_oid));
    chunk.set_value(1, 0, lv_str(resource, nspname));
    return chunk;
}

} // namespace components::catalog

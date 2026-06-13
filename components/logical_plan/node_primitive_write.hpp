#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    // Planner-emitted DDL leaf: write a single pre-built row into a pg_catalog table.
    // Fields are set at plan time; the operator calls disk.append_pg_catalog_row at runtime.
    class node_primitive_write_t final : public node_t {
    public:
        node_primitive_write_t(std::pmr::memory_resource* resource,
                               catalog::oid_t catalog_table_oid,
                               vector::data_chunk_t row);

        catalog::oid_t catalog_table_oid() const noexcept { return catalog_table_oid_; }
        vector::data_chunk_t& row() noexcept { return row_; }
        const vector::data_chunk_t& row() const noexcept { return row_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        catalog::oid_t catalog_table_oid_;
        vector::data_chunk_t row_;
    };

    using node_primitive_write_ptr = boost::intrusive_ptr<node_primitive_write_t>;

} // namespace components::logical_plan

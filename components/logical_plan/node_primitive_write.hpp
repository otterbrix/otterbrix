#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    // Inserts one pre-built row into a pg_catalog system table.
    // Emitted by the planner for DDL CREATE operations (one per catalog table row).
    class node_primitive_write_t final : public node_t {
    public:
        explicit node_primitive_write_t(std::pmr::memory_resource*          resource,
                                         const collection_full_name_t&       collection,
                                         components::catalog::oid_t           table_oid,
                                         components::vector::data_chunk_t     row);

        components::catalog::oid_t               table_oid() const { return table_oid_; }
        components::vector::data_chunk_t&        row()             { return row_; }
        const components::vector::data_chunk_t&  row()       const { return row_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t          table_oid_;
        components::vector::data_chunk_t    row_;
    };

    using node_primitive_write_ptr = boost::intrusive_ptr<node_primitive_write_t>;

} // namespace components::logical_plan

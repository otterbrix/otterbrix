#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <cstddef>

namespace components::logical_plan {

    // Deletes a catalog row identified by a matching OID value in a specific column.
    // Emitted by the planner for DDL DROP operations (one per catalog table/row).
    class node_primitive_delete_t final : public node_t {
    public:
        explicit node_primitive_delete_t(std::pmr::memory_resource*    resource,
                                          const collection_full_name_t& collection,
                                          components::catalog::oid_t    table_oid,
                                          std::size_t                   oid_col_index,
                                          components::catalog::oid_t    match_oid);

        components::catalog::oid_t table_oid()     const { return table_oid_; }
        std::size_t                oid_col_index() const { return oid_col_index_; }
        components::catalog::oid_t match_oid()     const { return match_oid_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t table_oid_;
        std::size_t                oid_col_index_;
        components::catalog::oid_t match_oid_;
    };

    using node_primitive_delete_ptr = boost::intrusive_ptr<node_primitive_delete_t>;

} // namespace components::logical_plan

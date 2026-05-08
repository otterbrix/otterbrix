#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    // Planner-emitted DDL leaf: delete all rows in a pg_catalog table where
    // column[oid_col_idx] == target_oid. The operator calls disk.drop_catalog_rows at runtime.
    class node_primitive_delete_t final : public node_t {
    public:
        node_primitive_delete_t(std::pmr::memory_resource*  resource,
                                 collection_full_name_t       catalog_table,
                                 std::int64_t                 oid_col_idx,
                                 components::catalog::oid_t   target_oid);

        const collection_full_name_t&      catalog_table() const noexcept { return catalog_table_; }
        std::int64_t                       oid_col_idx()   const noexcept { return oid_col_idx_; }
        components::catalog::oid_t         target_oid()    const noexcept { return target_oid_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        collection_full_name_t     catalog_table_;
        std::int64_t               oid_col_idx_;
        components::catalog::oid_t target_oid_;
    };

    using node_primitive_delete_ptr = boost::intrusive_ptr<node_primitive_delete_t>;

} // namespace components::logical_plan
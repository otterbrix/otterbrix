#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    // Phase 7.1 — planner-emitted leaf that marks one column on a relkind='g'
    // (generated / computing / Mongo-style dynamic-schema) table as dropped by
    // appending a new pg_computed_column row with attrefcount=0 (preserves the
    // history; the resolver in manager_disk_resolve.cpp filters refcount<=0).
    //
    // Carries the parent table's OID and the column name. The matching
    // operator (operator_computed_field_unregister_t) does:
    //
    //   1. read_rows_by_key on pg_computed_column where
    //        relid == table_oid AND attname == column_name
    //   2. find the latest live version (max(attversion) AND attrefcount > 0).
    //   3. append a tombstone row with attversion = max+1 and attrefcount = 0
    //      so the next resolve hides this column.
    //
    // We append rather than tombstone-via-delete to keep an audit trail; the
    // resolver already gates on attrefcount > 0, so the column disappears
    // without losing history.
    class node_computed_field_unregister_t final : public node_t {
    public:
        node_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                          collection_full_name_t      collection,
                                          components::catalog::oid_t  table_oid,
                                          std::string                 column_name);

        components::catalog::oid_t  table_oid()   const noexcept { return table_oid_; }
        const std::string&          column_name() const noexcept { return column_name_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t  table_oid_;
        std::string                 column_name_;
    };

    using node_computed_field_unregister_ptr = boost::intrusive_ptr<node_computed_field_unregister_t>;

} // namespace components::logical_plan

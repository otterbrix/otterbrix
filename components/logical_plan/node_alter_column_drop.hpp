#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <string>

namespace components::logical_plan {

    // Planner-emitted DDL leaf for `ALTER TABLE ... DROP COLUMN`.
    //
    // Built by planner::rewrite_alter_table from a drop_column subcommand of
    // node_alter_table_t. The operator (operator_alter_column_drop_t) walks
    // pg_depend at execution time, scrubs dependent indexes/constraints, then
    // soft-deletes the pg_attribute row (replaces it with attisdropped=true
    // tombstone). Mirrors the legacy ddl.cpp drop_column path that this node
    // replaces (Phase 2 #50).
    class node_alter_column_drop_t final : public node_t {
    public:
        node_alter_column_drop_t(std::pmr::memory_resource*       resource,
                                  collection_full_name_t           collection,
                                  components::catalog::oid_t       table_oid,
                                  components::catalog::oid_t       namespace_oid,
                                  std::string                      column_name,
                                  components::catalog::drop_behavior_t behavior =
                                      components::catalog::drop_behavior_t::cascade_);

        components::catalog::oid_t            table_oid()     const noexcept { return table_oid_; }
        components::catalog::oid_t            namespace_oid() const noexcept { return namespace_oid_; }
        const std::string&                    column_name()   const noexcept { return column_name_; }
        components::catalog::drop_behavior_t  behavior()      const noexcept { return behavior_; }

        // Resolved by enrich_logical_plan after looking up pg_attribute by
        // (table_oid, column_name). INVALID_OID means the column does not exist
        // (or was already dropped); the operator no-ops in that case.
        components::catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(components::catalog::oid_t a) noexcept { attoid_ = a; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t            table_oid_;
        components::catalog::oid_t            namespace_oid_;
        std::string                           column_name_;
        components::catalog::drop_behavior_t  behavior_;
        components::catalog::oid_t            attoid_{components::catalog::INVALID_OID};
    };

    using node_alter_column_drop_ptr = boost::intrusive_ptr<node_alter_column_drop_t>;

} // namespace components::logical_plan

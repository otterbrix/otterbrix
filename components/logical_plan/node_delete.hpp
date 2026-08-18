#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    class node_delete_t final : public node_t {
    public:
        explicit node_delete_t(std::pmr::memory_resource* resource,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit);

        // The delete target, as written. Kept on the node so enrich binds it to a
        // resolved entry by name rather than inferring it from a child.
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }
        const std::string& relname() const noexcept { return relname_; }
        void set_relname(std::string relname) { relname_ = std::move(relname); }

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // Catalog-delete spec (DDL pg_catalog row scrub). When oid_col_idx() >= 0
        // this node deletes every row in table_oid() whose column[oid_col_idx]
        // equals target_oid() via the WAL-first delete_pg_catalog_rows path —
        // create_plan_delete / create_plan_sequence lower it to operator_delete's
        // catalog branch instead of a predicate scan. Default oid_col_idx_ == -1
        // ("not a catalog delete").
        std::int64_t oid_col_idx() const noexcept { return oid_col_idx_; }
        components::catalog::oid_t target_oid() const noexcept { return target_oid_; }
        void set_catalog_delete(std::int64_t oid_col_idx, components::catalog::oid_t target_oid) noexcept {
            oid_col_idx_ = oid_col_idx;
            target_oid_ = target_oid;
        }

        // FK referencing metadata: FKs where this table is the parent.
        // Populated by enrich_plan when the table has referencing FK constraints.
        void set_referencing_fks(std::vector<catalog::fk_info_t> v) { referencing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& referencing_fks() const { return referencing_fks_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::vector<catalog::fk_info_t> referencing_fks_;
        std::pmr::vector<expressions::expression_ptr> returning_;
        std::int64_t oid_col_idx_{-1};
        components::catalog::oid_t target_oid_{components::catalog::INVALID_OID};
    };

    using node_delete_ptr = boost::intrusive_ptr<node_delete_t>;

    node_delete_ptr
    make_node_delete(std::pmr::memory_resource* resource, const node_match_ptr& match, const node_limit_ptr& limit);

    // Catalog-delete leaf (DDL pg_catalog row scrub): delete all rows in
    // `catalog_table_oid` where column[oid_col_idx] == target_oid. The empty
    // match/unlimited limit children exist only to satisfy the base shape; the
    // catalog branch of operator_delete ignores them.
    node_delete_ptr make_node_catalog_delete(std::pmr::memory_resource* resource,
                                             components::catalog::oid_t catalog_table_oid,
                                             std::int64_t oid_col_idx,
                                             components::catalog::oid_t target_oid);

} // namespace components::logical_plan

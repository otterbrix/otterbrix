#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    class node_delete_t final : public node_t {
    public:
        explicit node_delete_t(std::pmr::memory_resource* resource,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit);

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // table_oid for the USING-clause table (DELETE FROM tableA USING tableB).
        // enrich_logical_plan stamps this from the sibling resolve_table for
        // the USING target. create_plan_delete passes it to the USING-side
        // full_scan operator. Default INVALID_OID — caller must check before
        // using.
        components::catalog::oid_t table_oid_from() const noexcept { return table_oid_from_; }
        void set_table_oid_from(components::catalog::oid_t oid) noexcept { table_oid_from_ = oid; }

        // FK referencing metadata: FKs where this table is the parent.
        // Populated by enrich_plan when the table has referencing FK constraints.
        void set_referencing_fks(std::vector<catalog::fk_info_t> v) { referencing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& referencing_fks() const { return referencing_fks_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t table_oid_from_{components::catalog::INVALID_OID};
        std::vector<catalog::fk_info_t> referencing_fks_;
        std::pmr::vector<expressions::expression_ptr> returning_;
    };

    using node_delete_ptr = boost::intrusive_ptr<node_delete_t>;

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource, const node_match_ptr& match);

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource, const node_match_ptr& match);

    node_delete_ptr
    make_node_delete(std::pmr::memory_resource* resource, const node_match_ptr& match, const node_limit_ptr& limit);

} // namespace components::logical_plan

#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

namespace components::logical_plan {

    // Explicit projection node introduced by main's PR #479. Sits as a child
    // of node_aggregate_t and holds the SELECT-clause column expressions.
    // Adapted to our HEAD's strong-typed identifiers (core::dbname_t /
    // core::relname_t) and our node_t base ctor (resource, type) — no
    // collection_full_name_t parameter, since the aggregate parent already
    // carries the table identity.
    class node_select_t final : public node_t {
    public:
        explicit node_select_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        const core::dbname_t& dbname() const noexcept { return dbname_; }
        const core::relname_t& relname() const noexcept { return relname_; }

        // Number of hidden aggregate expressions appended at the tail of expressions_
        // (used for HAVING internal aggregates when there is no GROUP BY).
        // Visible SELECT column count = expressions_.size() - internal_aggregate_count.
        //
        // ALWAYS 0 TODAY. Nothing anywhere writes it — the transformer's HAVING lowering
        // stamps the equivalent counter on node_group_t instead (node_group.hpp), and its
        // only readers are the three `visible = exprs.size() - hidden` computations in
        // pushdown_filter's identity-select tests, which therefore always see the full
        // expression list. Kept as the declared home for the concept should a
        // group-less HAVING ever need hidden tail columns on the projection itself;
        // treat any code reading it as if the value were the literal 0.
        size_t internal_aggregate_count{0};

    private:
        core::dbname_t dbname_;
        core::relname_t relname_;
        std::string to_string_impl() const override;
    };

    using node_select_ptr = boost::intrusive_ptr<node_select_t>;

    node_select_ptr
    make_node_select(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

} // namespace components::logical_plan
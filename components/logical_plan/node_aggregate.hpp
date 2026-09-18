#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/expressions/key.hpp>

#include <vector>

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        explicit node_aggregate_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);
        explicit node_aggregate_t(std::pmr::memory_resource* resource,
                                  core::uid_t uid,
                                  core::dbname_t dbname,
                                  core::relname_t relname);

        void set_distinct(bool d) { distinct_ = d; }
        bool is_distinct() const { return distinct_; }

        // A non-empty list splices the distinct operator below the projection so the ON columns
        // survive for dedup.
        const std::pmr::vector<expressions::key_t>& distinct_on_keys() const { return distinct_on_keys_; }
        // Non-const: validate_logical_plan resolves each key's numeric path() in place via find_types.
        std::pmr::vector<expressions::key_t>& distinct_on_keys() { return distinct_on_keys_; }
        void set_distinct_on_keys(std::pmr::vector<expressions::key_t> keys) { distinct_on_keys_ = std::move(keys); }

        // Carries table identity through the parser-window; resolved code routes via table_oid() instead.
        const core::dbname_t& dbname() const noexcept { return dbname_; }
        const core::relname_t& relname() const noexcept { return relname_; }
        // Mirrors node_match_t::source(); a spliced view/CTE body clears relname_, so this reads as `none` too.
        match_source source() const noexcept { return relname_.t.empty() ? match_source::none : match_source::table; }
        // External identifier from a SQL `<uid>.<db>.<schema>.<rel>` form; empty when omitted (see swap_externals).
        const core::uid_t& uid() const noexcept { return uid_; }

        // Populated by the post-validate column_pruning pass; empty means no projection (scan all columns).
        const std::vector<size_t>& projected_cols() const { return projected_cols_; }
        void set_projected_cols(std::vector<size_t> cols) { projected_cols_ = std::move(cols); }

        // Set by pushdown_limit: a pure COUNT read-cap (offset always 0), advisory only.
        // EXCLUDED from hash_impl()/operator== — see node_match_t::read_cap_ for the rationale.
        void set_read_cap(const limit_t& read_cap) noexcept { read_cap_ = read_cap; }
        const limit_t& read_cap() const noexcept { return read_cap_; }

        // Must run once the view body is spliced in as child[0], or bind_catalog_data re-stamps
        // table_oid and collect_view_references re-expands it.
        void clear_source_identity() {
            uid_.t.clear();
            dbname_.t.clear();
            relname_.t.clear();
            set_table_oid(components::catalog::INVALID_OID);
            set_table_metadata(nullptr);
        }

    private:
        core::uid_t uid_;
        core::dbname_t dbname_;
        core::relname_t relname_;
        bool distinct_{false};
        std::pmr::vector<expressions::key_t> distinct_on_keys_;
        std::vector<size_t> projected_cols_;
        limit_t read_cap_{};
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_aggregate_ptr = boost::intrusive_ptr<node_aggregate_t>;

    node_aggregate_ptr
    make_node_aggregate(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);
    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           core::uid_t uid,
                                           core::dbname_t dbname,
                                           core::relname_t relname);

} // namespace components::logical_plan

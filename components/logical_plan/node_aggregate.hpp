#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include "node_limit.hpp"

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

        // Role-named accessors. The aggregate node carries the source table
        // identity through the parser-window for downstream operator dispatch;
        // routing in resolved-stage code uses table_oid().
        const core::dbname_t& dbname() const noexcept { return dbname_; }
        const core::relname_t& relname() const noexcept { return relname_; }
        // Parser-supplied external identifier from a SQL fully-qualified
        // `<uid>.<db>.<schema>.<rel>` form. Carries through the parser-window
        // for client-side externals (e.g. raw-chunk injection in JOIN tests
        // via swap_externals). Empty when SQL omits the uid prefix.
        const core::uid_t& uid() const noexcept { return uid_; }

        // Column projection metadata, populated by the post-validate column_pruning pass.
        // When non-empty, downstream scan operators read only these column indices from
        // the source table instead of scanning every column. Empty = "no projection"
        // (scan all columns) — the default.
        const std::vector<size_t>& projected_cols() const { return projected_cols_; }
        void set_projected_cols(std::vector<size_t> cols) { projected_cols_ = std::move(cols); }

        // Optimizer annotation set by the pushdown_limit rule: a pure COUNT read-cap
        // (offset always 0) the terminal transfer_scan may cap its base-table read
        // at, for the plain-scan shape (no WHERE, no sort/group/non-scan source, NOT
        // is_distinct()) whose scan create_plan_aggregate builds directly. The
        // authoritative operator_limit above still windows [offset, offset+limit).
        // unlimit() = no cap. Advisory only; EXCLUDED from hash_impl()/operator== —
        // see node_match_t::read_cap_ for the rationale.
        void set_read_cap(const limit_t& read_cap) noexcept { read_cap_ = read_cap; }
        const limit_t& read_cap() const noexcept { return read_cap_; }

    private:
        core::uid_t uid_;
        core::dbname_t dbname_;
        core::relname_t relname_;
        bool distinct_{false};
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

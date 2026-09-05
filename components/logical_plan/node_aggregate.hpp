#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include "node_limit.hpp"

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

        // SELECT DISTINCT ON (...) keys. Empty for plain DISTINCT (whole-row dedup) and for
        // non-DISTINCT. Name-based after transform; validate_logical_plan resolves each key's
        // numeric path(). A non-empty list forces the coordinator path (pushdown barrier) and
        // makes create_plan splice the distinct operator BELOW the projection, so the ON columns
        // are still present for subset dedup.
        const std::pmr::vector<expressions::key_t>& distinct_on_keys() const { return distinct_on_keys_; }
        // Non-const: validate_logical_plan resolves each key's numeric path() in place via find_types.
        std::pmr::vector<expressions::key_t>& distinct_on_keys() { return distinct_on_keys_; }
        void set_distinct_on_keys(std::pmr::vector<expressions::key_t> keys) { distinct_on_keys_ = std::move(keys); }

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

        // Stop being a source. After a view body has been spliced in as child[0],
        // this node is a CONSUMER of that child and no longer names a base relation —
        // exactly the shape an inlined CTE reference has (see transform_from_element:
        // make_node_aggregate(resource, {}, {}) + append_child(body)).
        //
        // Clearing the names is what TERMINATES the expansion: bind_catalog_data binds
        // by name, so a node that still said "v" would be re-stamped with the view's
        // table_oid on the next bind and collect_view_references would find it again.
        // Clearing the stamped oid/metadata as well is the other half: create_plan_match
        // hands back a bare full_scan whenever has_table_oid() holds, which would drop
        // the spliced child on the floor.
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

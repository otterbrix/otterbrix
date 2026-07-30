#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/expressions/key.hpp>

#include <optional>

namespace components::logical_plan {

    enum class join_type : uint8_t
    {
        invalid,
        inner,
        full,
        left,
        right,
        cross,
        // Semi- / anti-join: emit each LEFT (outer) row AT MOST ONCE. `semi` keeps an
        // outer row iff the right (inner) side has >=1 matching row; `anti` keeps it
        // iff the right side has NO matching row. The output schema is the LEFT schema
        // only (no right columns). Produced by the transformer for a correlated
        // EXISTS (semi) / NOT EXISTS (anti) in WHERE, lowered as a LATERAL join.
        semi,
        anti
    };

    class node_join_t final : public node_t {
    public:
        // Physical algorithm chosen for this logical join. Stamped by the optimizer
        // rule rewrite_hash_joins (via set_equi_columns) and read by create_plan_join.
        // This is an ANNOTATION only — it does NOT change the logical semantics.
        enum class join_algo : uint8_t
        {
            nested,
            hash
        };

        explicit node_join_t(std::pmr::memory_resource* resource,
                             core::dbname_t dbname,
                             core::relname_t relname,
                             join_type type);

        join_type type() const;

        join_algo algo() const noexcept;
        void set_algo(join_algo algo) noexcept;
        std::size_t left_col() const noexcept;
        std::size_t right_col() const noexcept;
        // Records the detected equi-key column indices (into each side's input chunk)
        // and switches algo() to hash. Called by rewrite_hash_joins.
        void set_equi_columns(std::size_t left, std::size_t right) noexcept;

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

        // A join is binary: children() is [left, right] by construction. These stay
        // total on a partially built node (a null node_ptr), because optimizer rules
        // do walk half-assembled shapes mid-rewrite.
        const node_ptr& left() const noexcept { return child_or_null(0); }
        const node_ptr& right() const noexcept { return child_or_null(1); }

        // Column count each input contributes to this join's MERGED output.
        // left_width() is the merged ordinal at which the right input's columns begin:
        // a right-side column's merged path()[0] is left_width() + its right-local index.
        //
        // Read from that child's validator-stamped output_schema(). nullopt means the
        // child carries NO stamp — which is not the same as a width of zero, and is
        // exactly the case the callers degrade DIFFERENTLY on (promote_cross_join: "not
        // a promotable boundary, leave it CROSS"; eager_aggregation: bail outright;
        // pushdown_filter: keep the conjunct in the residual rather than push an
        // out-of-range path). This supplies the number and takes no position on it.
        [[nodiscard]] std::optional<std::size_t> left_width() const;
        [[nodiscard]] std::optional<std::size_t> right_width() const;

        enum class merged_side : uint8_t
        {
            left_input,
            right_input,
            out_of_range
        };

        // Which input a MERGED column ordinal addresses, against an EXPLICITLY supplied
        // boundary. Deliberately not a live read of the children: a rewrite captures the
        // boundary from the intact stamped children first and then classifies against
        // that captured value while it splices the children underneath.
        [[nodiscard]] static merged_side
        side_of(std::size_t merged_ordinal, std::size_t left_width, std::size_t right_width) noexcept;

        using correlation_t = std::pair<core::parameter_id_t, expressions::key_t>;

        bool is_lateral() const noexcept { return lateral_; }
        void set_lateral(bool lateral) noexcept { lateral_ = lateral; }
        const std::pmr::vector<correlation_t>& correlations() const noexcept { return correlations_; }
        void add_correlation(core::parameter_id_t id, expressions::key_t key) {
            correlations_.emplace_back(id, std::move(key));
        }

    private:
        std::string dbname_;
        std::string relname_;
        join_type type_;
        join_algo algo_{join_algo::nested};
        std::size_t left_col_{0};
        std::size_t right_col_{0};
        bool lateral_{false};
        std::pmr::vector<correlation_t> correlations_{resource()};

        std::string to_string_impl() const override;
    };

    using node_join_ptr = boost::intrusive_ptr<node_join_t>;

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname, join_type type);

} // namespace components::logical_plan

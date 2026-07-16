#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/expressions/key.hpp>

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

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_join_ptr = boost::intrusive_ptr<node_join_t>;

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname, join_type type);

} // namespace components::logical_plan

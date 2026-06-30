#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

namespace components::logical_plan {

    enum class join_type : uint8_t
    {
        invalid,
        inner,
        full,
        left,
        right,
        cross
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

        // Memory budget for this logical join. Stamped by the optimizer rule
        // spill_strategy from context_storage_t::disk_config (R10) and read by
        // create_plan_join. Separate from join_algo: join_algo is nested-vs-hash
        // (algorithm choice), exec_strategy is in-memory-vs-spill (memory budget).
        // This is an ANNOTATION only — it does NOT change the logical semantics.
        enum class exec_strategy : uint8_t
        {
            in_memory,
            spill
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

        exec_strategy strategy() const noexcept;
        // Stamped by the optimizer spill_strategy rule when
        // disk_config->spill_enabled is set. Read by create_plan_join to lower to
        // the grace/disk join operator.
        void set_strategy(exec_strategy s) noexcept;

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        join_type type_;
        join_algo algo_{join_algo::nested};
        exec_strategy strategy_{exec_strategy::in_memory};
        std::size_t left_col_{0};
        std::size_t right_col_{0};

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_join_ptr = boost::intrusive_ptr<node_join_t>;

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname, join_type type);

} // namespace components::logical_plan

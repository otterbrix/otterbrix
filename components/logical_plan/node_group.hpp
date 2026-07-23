#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

        // Optimizer annotation: this aggregate sub-plan targets a SINGLE owning
        // agent and every aggregate is fragment-mergeable, so the reduce can be
        // pushed to that agent instead of the coordinator. Stamped by the
        // pushdown_aggregate rule; consumed by the physical-plan lowering
        // (create_plan_aggregate). ANNOTATION only — it does NOT change logical
        // semantics. Like node_aggregate_t::projected_cols_ (and UNLIKE
        // node_join_t::algo_), it is deliberately EXCLUDED from hash_impl()
        // (which stays 0). That exclusion is safe ONLY while the flag is a
        // STATIC, per-process rollout gate: a pushed and a non-pushed plan for
        // the same query then never coexist in one process. A DYNAMIC
        // (runtime-toggled) flag would let both variants hash-collide in the
        // plan cache — if this ever becomes runtime-dynamic, fold pushdown_
        // into hash_impl() the way node_join_t does with algo_.
        void set_pushdown(bool pushdown) noexcept;
        [[nodiscard]] bool pushdown() const noexcept;

        size_t internal_aggregate_count{0};
        // Number of visible SELECT-clause columns recorded BEFORE the
        // transformer appends hidden internal aggregates for HAVING etc.
        // PR #479-style projection lineage uses this to know where the
        // visible SELECT list ends.
        size_t visible_select_count{0};

    private:
        std::string dbname_;
        std::string relname_;
        // See set_pushdown()/pushdown() above. Default false = coordinator-side
        // reduce. Intentionally NOT folded into hash_impl().
        bool pushdown_{false};

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_group_ptr = boost::intrusive_ptr<node_group_t>;

    node_group_ptr make_node_group(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const std::vector<expression_ptr>& expressions);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const std::pmr::vector<expression_ptr>& expressions);

} // namespace components::logical_plan

#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include "node_limit.hpp"

namespace components::logical_plan {

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

        // Optimizer annotation (pushdown_limit rule): a pure COUNT read-cap
        // (offset always 0) = limit+offset rows the disk scan for this WHERE may
        // cap its POST-filter output at, so the authoritative operator_limit above
        // can still window [offset, offset+limit). unlimit() = no cap. ANNOTATION
        // only (no logical-semantics change); like node_group_t::pushdown_ it is
        // deliberately EXCLUDED from hash_impl() (stays 0) and operator== (which
        // ignores scalar members) — safe only while no logical-plan-hash-keyed
        // plan cache exists; if one is introduced, fold read_cap_ into hash_impl().
        void set_read_cap(const limit_t& read_cap) noexcept { read_cap_ = read_cap; }
        const limit_t& read_cap() const noexcept { return read_cap_; }

    private:
        std::string dbname_;
        std::string relname_;
        limit_t read_cap_{};
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_match_ptr = boost::intrusive_ptr<node_match_t>;

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const expressions::expression_ptr& match);

} // namespace components::logical_plan

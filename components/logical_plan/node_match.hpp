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

        // Optimizer annotation set by the pushdown_limit rule: a pure COUNT read-cap
        // (offset always 0) capping this WHERE scan's POST-filter output at
        // limit+offset rows, so the authoritative operator_limit above can still
        // window [offset, offset+limit). unlimit() = no cap. Advisory only, no
        // semantics change. Deliberately EXCLUDED from hash_impl()
        // (like node_group_t::pushdown_): safe only while no logical-plan-hash-keyed
        // plan cache exists — fold it into hash_impl() if one is introduced.
        void set_read_cap(const limit_t& read_cap) noexcept { read_cap_ = read_cap; }
        const limit_t& read_cap() const noexcept { return read_cap_; }

    private:
        std::string dbname_;
        std::string relname_;
        limit_t read_cap_{};
        std::string to_string_impl() const override;
    };

    using node_match_ptr = boost::intrusive_ptr<node_match_t>;

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const expressions::expression_ptr& match);

} // namespace components::logical_plan

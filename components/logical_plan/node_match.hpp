#pragma once

#include "identifier_types.hpp"
#include "node.hpp"
#include "node_limit.hpp"

namespace components::logical_plan {

    // What a match node reads from, stated by the node itself. The base class's
    // table_oid() cannot say it: INVALID_OID is what BOTH "the statement has no FROM"
    // and "the named table never resolved" look like there, and the two demand opposite
    // plans (a one-row synthetic source vs a refusal).
    enum class match_source : uint8_t
    {
        none, // no FROM: the plan's source is the single synthetic placeholder row
        table // a named table; it must arrive at plan generation resolved
    };

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }
        match_source source() const noexcept { return relname_.empty() ? match_source::none : match_source::table; }

        // Optimizer annotation set by the pushdown_limit rule: a pure COUNT read-cap
        // (offset always 0) capping this WHERE scan's POST-filter output at
        // limit+offset rows, so the authoritative operator_limit above can still
        // window [offset, offset+limit). unlimit() = no cap. Advisory only, no
        // semantics change. Deliberately EXCLUDED from hash_impl() and operator==
        // (like node_group_t::pushdown_): safe only while no logical-plan-hash-keyed
        // plan cache exists — fold it into hash_impl() if one is introduced.
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

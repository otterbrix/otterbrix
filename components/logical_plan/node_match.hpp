#pragma once

#include "node.hpp"
#include "node_create_index.hpp"
#include "node_limit.hpp"

#include <components/expressions/compare_expression.hpp>

#include <optional>

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

    // The index the optimizer chose to answer a WHERE (rule index_selection): the WHERE's single compare reads
    // `key OP value` with the key on the left or the right, `compare` as the index reads it (key on the left).
    struct index_choice_t {
        bool key_on_left{true};
        expressions::compare_type compare{expressions::compare_type::eq};
        index_type type{index_type::no_valid};
    };

    class node_match_t final : public node_t {
    public:
        node_match_t(std::pmr::memory_resource* resource, qualified_name_t target);

        match_source source() const noexcept {
            return target_.collection.t.empty() ? match_source::none : match_source::table;
        }

        // Optimizer annotation set by the pushdown_limit rule: a pure COUNT read-cap
        // (offset always 0) capping this WHERE scan's POST-filter output at
        // limit+offset rows, so the authoritative operator_limit above can still
        // window [offset, offset+limit). row_cap_t::uncapped() = no cap. Advisory only, no
        // semantics change. Deliberately EXCLUDED from hash_impl() and operator==
        // (like node_aggregate_t::projected_cols_): safe only while no logical-plan-hash-keyed
        // plan cache exists — fold it into hash_impl() if one is introduced.
        void set_read_cap(const row_cap_t& read_cap) noexcept { read_cap_ = read_cap; }
        const row_cap_t& read_cap() const noexcept { return read_cap_; }

        // Set by the index_selection rule; plan generation reads this WHERE through the index.
        void set_index(const index_choice_t& choice) noexcept { index_ = choice; }
        const std::optional<index_choice_t>& index() const noexcept { return index_; }

    private:
        row_cap_t read_cap_{};
        std::optional<index_choice_t> index_;
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_match_ptr = boost::intrusive_ptr<node_match_t>;

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   qualified_name_t target,
                                   const expressions::expression_ptr& match);

} // namespace components::logical_plan

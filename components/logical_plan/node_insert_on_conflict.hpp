#pragma once

#include "node.hpp"
#include "node_insert.hpp"

#include <components/expressions/expression.hpp>

#include <string>
#include <vector>

namespace components::logical_plan {

    enum class on_conflict_action_t : uint8_t
    {
        do_nothing,
        do_update
    };

    struct insert_on_conflict_t {
        explicit insert_on_conflict_t(std::pmr::memory_resource* resource);

        on_conflict_action_t action{on_conflict_action_t::do_nothing};
        std::pmr::vector<std::pmr::string> target_columns;
        std::pmr::string constraint_name;
        expressions::expression_ptr target_where;
        std::vector<std::vector<std::string>> arbiter_groups;
    };

    class node_insert_on_conflict_t final : public node_t {
    public:
        node_insert_on_conflict_t(std::pmr::memory_resource* resource, node_insert_ptr insert);

        insert_on_conflict_t& on_conflict() { return on_conflict_; }
        const insert_on_conflict_t& on_conflict() const { return on_conflict_; }

        node_insert_t* insert() const;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        insert_on_conflict_t on_conflict_;
    };

    using node_insert_on_conflict_ptr = boost::intrusive_ptr<node_insert_on_conflict_t>;

} // namespace components::logical_plan

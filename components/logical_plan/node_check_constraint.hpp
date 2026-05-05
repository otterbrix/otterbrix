#pragma once

#include "node.hpp"

#include <components/catalog/constraint_evaluator.hpp>

#include <string>
#include <vector>

namespace components::logical_plan {

    // Checks NOT NULL columns and evaluates compiled CHECK constraint predicates
    // over the incoming chunk. Emitted by the planner above node_insert /
    // node_update when the target table has NOT NULL or CHECK constraints.
    class node_check_constraint_t final : public node_t {
    public:
        struct check_entry_t {
            components::catalog::row_predicate_fn predicate;
            std::string                           conexpr;
        };

        explicit node_check_constraint_t(std::pmr::memory_resource*          resource,
                                          const collection_full_name_t&       collection,
                                          std::vector<std::string>            not_null_columns,
                                          std::vector<check_entry_t>          checks);

        const std::vector<std::string>&    not_null_columns() const { return not_null_columns_; }
        const std::vector<check_entry_t>&  checks()           const { return checks_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        std::vector<std::string>   not_null_columns_;
        std::vector<check_entry_t> checks_;
    };

    using node_check_constraint_ptr = boost::intrusive_ptr<node_check_constraint_t>;

} // namespace components::logical_plan

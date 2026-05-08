#pragma once

#include "node.hpp"

#include <string>
#include <utility>
#include <vector>

namespace components::logical_plan {

    // Checks NOT NULL constraints and CHECK expressions over the incoming chunk.
    // Emitted by the planner above node_insert / node_update when the target
    // table has NOT NULL columns or CHECK constraints.
    class node_check_constraint_t final : public node_t {
    public:
        // check_exprs: vector of (constraint_name, expression_string) pairs loaded from pg_constraint.
        explicit node_check_constraint_t(std::pmr::memory_resource*                        resource,
                                          const collection_full_name_t&                     collection,
                                          std::vector<std::string>                          not_null_columns,
                                          std::vector<std::pair<std::string, std::string>>  check_exprs = {});

        const std::vector<std::string>& not_null_columns() const { return not_null_columns_; }
        const std::vector<std::pair<std::string, std::string>>& check_exprs() const { return check_exprs_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        std::vector<std::string>                         not_null_columns_;
        std::vector<std::pair<std::string, std::string>> check_exprs_; // (name, expr)
    };

    using node_check_constraint_ptr = boost::intrusive_ptr<node_check_constraint_t>;

} // namespace components::logical_plan
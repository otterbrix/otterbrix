#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <string>
#include <utility>
#include <vector>

namespace components::logical_plan {

    class node_check_constraint_t final : public node_t {
    public:
        explicit node_check_constraint_t(std::pmr::memory_resource* resource,
                                         core::dbname_t dbname,
                                         core::relname_t relname,
                                         std::vector<std::string> not_null_columns,
                                         std::vector<std::pair<std::string, std::string>> check_exprs = {});

        const std::vector<std::string>& not_null_columns() const { return not_null_columns_; }
        const std::vector<std::pair<std::string, std::string>>& check_exprs() const { return check_exprs_; }

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::vector<std::string> not_null_columns_;
        std::vector<std::pair<std::string, std::string>> check_exprs_; // (name, expr)
    };

    using node_check_constraint_ptr = boost::intrusive_ptr<node_check_constraint_t>;

} // namespace components::logical_plan

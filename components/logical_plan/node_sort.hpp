#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/expressions/compare_expression.hpp>

namespace components::logical_plan {

    class node_sort_t final : public node_t {
    public:
        explicit node_sort_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_sort_ptr = boost::intrusive_ptr<node_sort_t>;

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 const std::vector<expressions::expression_ptr>& expressions);

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions);

} // namespace components::logical_plan

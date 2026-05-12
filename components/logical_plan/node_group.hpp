#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource,
                              std::string dbname,
                              std::string relname,
                              expression_ptr having = nullptr);

        const expression_ptr& having() const { return having_; }

        // Phase 9.W/10.D: role-named accessors. Carries source table identity for parser-window.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

        size_t internal_aggregate_count{0};
        size_t visible_select_count{0};

    private:
        std::string dbname_;
        std::string relname_;
        expression_ptr having_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_group_ptr = boost::intrusive_ptr<node_group_t>;

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   std::string dbname,
                                   std::string relname,
                                   expression_ptr having = nullptr);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   std::string dbname,
                                   std::string relname,
                                   const std::vector<expression_ptr>& expressions,
                                   expression_ptr having = nullptr);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   std::string dbname,
                                   std::string relname,
                                   const std::pmr::vector<expression_ptr>& expressions,
                                   expression_ptr having = nullptr);

} // namespace components::logical_plan

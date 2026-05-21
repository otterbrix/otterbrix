#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource,
                              core::dbname_t dbname,
                              core::relname_t relname,
                              expression_ptr having = nullptr);

        const expression_ptr& having() const { return having_; }

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

        size_t internal_aggregate_count{0};
        // Number of visible SELECT-clause columns recorded BEFORE the
        // transformer appends hidden internal aggregates for HAVING etc.
        // PR #479-style projection lineage uses this to know where the
        // visible SELECT list ends.
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
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   expression_ptr having = nullptr);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const std::vector<expression_ptr>& expressions,
                                   expression_ptr having = nullptr);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const std::pmr::vector<expression_ptr>& expressions,
                                   expression_ptr having = nullptr);

} // namespace components::logical_plan

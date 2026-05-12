#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_having_t final : public node_t {
    public:
        explicit node_having_t(std::pmr::memory_resource* resource, std::string dbname, std::string relname);

        // Phase 9.W/10.D: role-named accessors. Carries source table identity for parser-window.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_having_ptr = boost::intrusive_ptr<node_having_t>;

    node_having_ptr make_node_having(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     const expressions::expression_ptr& expr);

} // namespace components::logical_plan

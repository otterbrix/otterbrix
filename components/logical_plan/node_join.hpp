#pragma once

#include "node.hpp"

namespace components::logical_plan {

    enum class join_type : uint8_t
    {
        inner,
        full,
        left,
        right,
        cross
    };

    class node_join_t final : public node_t {
    public:
        explicit node_join_t(std::pmr::memory_resource* resource,
                             std::string dbname,
                             std::string relname,
                             join_type type);

        join_type type() const;

        // Phase 9.W/10.D: role-named accessors. Carries left-side join target identity.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        join_type type_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_join_ptr = boost::intrusive_ptr<node_join_t>;

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, std::string dbname, std::string relname, join_type type);

} // namespace components::logical_plan

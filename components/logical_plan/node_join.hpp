#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

namespace components::logical_plan {

    enum class join_type : uint8_t
    {
        invalid,
        inner,
        full,
        left,
        right,
        cross
    };

    class node_join_t final : public node_t {
    public:
        explicit node_join_t(std::pmr::memory_resource* resource,
                             core::dbname_t dbname,
                             core::relname_t relname,
                             join_type type);

        join_type type() const;

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
    make_node_join(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname, join_type type);

    // Optimizer-produced equi-join node (see node_type::hash_join_t). Same shape as
    // node_join_t (children = left/right inputs, expressions = the ON condition) but
    // additionally carries the equi-key column indices into each side's input chunk,
    // detected by rewrite_hash_joins. The planner lowers it directly into
    // operator_hash_join_t without re-inspecting the condition.
    class node_hash_join_t final : public node_t {
    public:
        node_hash_join_t(std::pmr::memory_resource* resource,
                         core::dbname_t dbname,
                         core::relname_t relname,
                         join_type type,
                         std::size_t left_col,
                         std::size_t right_col);

        join_type type() const noexcept { return type_; }
        std::size_t left_col() const noexcept { return left_col_; }
        std::size_t right_col() const noexcept { return right_col_; }

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        join_type type_;
        std::size_t left_col_;
        std::size_t right_col_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_hash_join_ptr = boost::intrusive_ptr<node_hash_join_t>;

    node_hash_join_ptr make_node_hash_join(std::pmr::memory_resource* resource,
                                           core::dbname_t dbname,
                                           core::relname_t relname,
                                           join_type type,
                                           std::size_t left_col,
                                           std::size_t right_col);

} // namespace components::logical_plan

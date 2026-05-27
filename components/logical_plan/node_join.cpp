#include "node_join.hpp"

#include <sstream>

namespace components::logical_plan {

    std::string to_string(join_type type) {
        switch (type) {
            case join_type::inner:
                return "inner";
            case join_type::full:
                return "full";
            case join_type::left:
                return "left";
            case join_type::right:
                return "right";
            case join_type::cross:
                return "cross";
            default:
                return "invalid";
        }
    }

    node_join_t::node_join_t(std::pmr::memory_resource* resource,
                             core::dbname_t dbname,
                             core::relname_t relname,
                             join_type type)
        : node_t(resource, node_type::join_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , type_(type) {}

    join_type node_join_t::type() const { return type_; }

    hash_t node_join_t::hash_impl() const { return 0; }

    std::string node_join_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$join: {";
        stream << "$type: " << logical_plan::to_string(type_);
        for (const auto& child : children_) {
            stream << ", " << child->to_string();
        }
        for (const auto& expr : expressions()) {
            stream << ", " << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_join_ptr make_node_join(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 join_type type) {
        return {new node_join_t{resource, std::move(dbname), std::move(relname), type}};
    }

    node_hash_join_t::node_hash_join_t(std::pmr::memory_resource* resource,
                                       core::dbname_t dbname,
                                       core::relname_t relname,
                                       join_type type,
                                       std::size_t left_col,
                                       std::size_t right_col)
        : node_t(resource, node_type::hash_join_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname)))
        , type_(type)
        , left_col_(left_col)
        , right_col_(right_col) {}

    hash_t node_hash_join_t::hash_impl() const { return 0; }

    std::string node_hash_join_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$hash_join: {";
        stream << "$type: " << logical_plan::to_string(type_);
        stream << ", $left_col: " << left_col_ << ", $right_col: " << right_col_;
        for (const auto& child : children_) {
            stream << ", " << child->to_string();
        }
        for (const auto& expr : expressions()) {
            stream << ", " << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_hash_join_ptr make_node_hash_join(std::pmr::memory_resource* resource,
                                           core::dbname_t dbname,
                                           core::relname_t relname,
                                           join_type type,
                                           std::size_t left_col,
                                           std::size_t right_col) {
        return {new node_hash_join_t{resource, std::move(dbname), std::move(relname), type, left_col, right_col}};
    }

} // namespace components::logical_plan

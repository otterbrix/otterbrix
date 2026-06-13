#include "node_match.hpp"

#include <sstream>

namespace components::logical_plan {

    node_match_t::node_match_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname)
        : node_t(resource, node_type::match_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname))) {}

    hash_t node_match_t::hash_impl() const { return 0; }

    std::string node_match_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$match: {";
        bool is_first = true;
        for (const auto& expr : expressions_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   core::dbname_t dbname,
                                   core::relname_t relname,
                                   const expressions::expression_ptr& match) {
        collection_full_name_t collection;
        collection.database = static_cast<const std::string&>(dbname);
        collection.collection = static_cast<const std::string&>(relname);
        node_match_ptr node = new node_match_t{resource, std::move(dbname), std::move(relname)};
        node->set_collection_full_name(std::move(collection));
        if (match) {
            node->append_expression(match);
        }
        return node;
    }

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const expressions::expression_ptr& match) {
        auto node = make_node_match(
            resource, core::dbname_t{collection.database}, core::relname_t{collection.collection}, match);
        node->set_collection_full_name(collection);
        return node;
    }

} // namespace components::logical_plan

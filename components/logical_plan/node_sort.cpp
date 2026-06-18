#include "node_sort.hpp"

#include <sstream>

namespace components::logical_plan {

    node_sort_t::node_sort_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname)
        : node_t(resource, node_type::sort_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname))) {}

    hash_t node_sort_t::hash_impl() const { return 0; }

    std::string node_sort_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$sort: {";
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

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 const std::vector<expressions::expression_ptr>& expressions) {
        collection_full_name_t collection;
        collection.database = static_cast<const std::string&>(dbname);
        collection.collection = static_cast<const std::string&>(relname);
        auto node = new node_sort_t{resource, std::move(dbname), std::move(relname)};
        node->set_collection_full_name(std::move(collection));
        node->append_expressions(expressions);
        return node;
    }

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::vector<expressions::expression_ptr>& expressions) {
        auto node =
            make_node_sort(resource, core::dbname_t{collection.database}, core::relname_t{collection.collection}, expressions);
        node->set_collection_full_name(collection);
        return node;
    }

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions) {
        collection_full_name_t collection;
        collection.database = static_cast<const std::string&>(dbname);
        collection.collection = static_cast<const std::string&>(relname);
        auto node = new node_sort_t{resource, std::move(dbname), std::move(relname)};
        node->set_collection_full_name(std::move(collection));
        node->append_expressions(expressions);
        return node;
    }

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions) {
        auto node =
            make_node_sort(resource, core::dbname_t{collection.database}, core::relname_t{collection.collection}, expressions);
        node->set_collection_full_name(collection);
        return node;
    }

} // namespace components::logical_plan

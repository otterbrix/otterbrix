#include "node_delete.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <sstream>

namespace components::logical_plan {

    node_delete_t::node_delete_t(std::pmr::memory_resource* resource,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit)
        : node_t(resource, node_type::delete_t)
        , returning_(resource) {
        append_child(match);
        append_child(limit);
    }

    std::pmr::vector<expressions::expression_ptr>& node_delete_t::returning() { return returning_; }
    const std::pmr::vector<expressions::expression_ptr>& node_delete_t::returning() const { return returning_; }

    hash_t node_delete_t::hash_impl() const { return 0; }

    std::string node_delete_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$delete: <oid:" << static_cast<std::uint64_t>(table_oid()) << "> {";
        bool is_first = true;
        for (const auto& child : children()) {
            if (!is_first) {
                stream << ", ";
            } else {
                is_first = false;
            }
            stream << child->to_string();
        }
        stream << "}";
        return stream.str();
    }

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource, const node_match_ptr& match) {
        auto limit = make_node_limit(resource, core::dbname_t{}, core::relname_t{}, limit_t::unlimit());
        return {new node_delete_t{resource, match, limit}};
    }

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match) {
        auto limit = make_node_limit(resource, collection, limit_t::unlimit());
        return make_node_delete(resource, collection, match, limit);
    }

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const collection_full_name_t& collection_from,
                                          const node_match_ptr& match) {
        auto limit = make_node_limit(resource, collection, limit_t::unlimit());
        return make_node_delete(resource, collection, collection_from, match, limit);
    }

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource, const node_match_ptr& match) {
        auto limit = make_node_limit(resource, core::dbname_t{}, core::relname_t{}, limit_t::limit_one());
        return {new node_delete_t{resource, match, limit}};
    }

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match) {
        auto limit = make_node_limit(resource, collection, limit_t::limit_one());
        return make_node_delete(resource, collection, match, limit);
    }

    node_delete_ptr
    make_node_delete(std::pmr::memory_resource* resource, const node_match_ptr& match, const node_limit_ptr& limit) {
        return {new node_delete_t{resource, match, limit}};
    }

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit) {
        auto node = make_node_delete(resource, match, limit);
        node->set_collection_full_name(collection);
        return node;
    }

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const collection_full_name_t& collection_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit) {
        auto node = make_node_delete(resource, collection, match, limit);
        node->set_collection_from(collection_from);
        return node;
    }

} // namespace components::logical_plan

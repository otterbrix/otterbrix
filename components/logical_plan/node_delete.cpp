#include "node_delete.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <sstream>

namespace components::logical_plan {

    node_delete_t::node_delete_t(std::pmr::memory_resource* resource,
                                 std::string dbname_to,
                                 std::string relname_to,
                                 std::string dbname_from,
                                 std::string relname_from,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit)
        : node_t(resource, node_type::delete_t)
        , dbname_(std::move(dbname_to))
        , relname_(std::move(relname_to))
        , dbname_from_(std::move(dbname_from))
        , relname_from_(std::move(relname_from)) {
        append_child(match);
        append_child(limit);
    }

    hash_t node_delete_t::hash_impl() const { return 0; }

    std::string node_delete_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$delete: {";
        bool is_first = true;
        for (auto child : children()) {
            if (!is_first) {
                stream << ", ";
            } else {
                is_first = false;
            }
            stream << child;
        }
        stream << "}";
        return stream.str();
    }

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          std::string dbname,
                                          std::string relname,
                                          const node_match_ptr& match) {
        auto limit = make_node_limit(resource, dbname, relname, limit_t::unlimit());
        return {new node_delete_t{resource, std::move(dbname), std::move(relname), {}, {}, match, limit}};
    }

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          std::string dbname_to,
                                          std::string relname_to,
                                          std::string dbname_from,
                                          std::string relname_from,
                                          const node_match_ptr& match) {
        auto limit = make_node_limit(resource, dbname_to, relname_to, limit_t::unlimit());
        return {new node_delete_t{resource,
                                  std::move(dbname_to),
                                  std::move(relname_to),
                                  std::move(dbname_from),
                                  std::move(relname_from),
                                  match,
                                  limit}};
    }

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         std::string dbname,
                                         std::string relname,
                                         const node_match_ptr& match) {
        auto limit = make_node_limit(resource, dbname, relname, limit_t::limit_one());
        return {new node_delete_t{resource, std::move(dbname), std::move(relname), {}, {}, match, limit}};
    }

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         std::string dbname_to,
                                         std::string relname_to,
                                         std::string dbname_from,
                                         std::string relname_from,
                                         const node_match_ptr& match) {
        auto limit = make_node_limit(resource, dbname_to, relname_to, limit_t::limit_one());
        return {new node_delete_t{resource,
                                  std::move(dbname_to),
                                  std::move(relname_to),
                                  std::move(dbname_from),
                                  std::move(relname_from),
                                  match,
                                  limit}};
    }

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit) {
        return {new node_delete_t{resource, std::move(dbname), std::move(relname), {}, {}, match, limit}};
    }

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     std::string dbname_to,
                                     std::string relname_to,
                                     std::string dbname_from,
                                     std::string relname_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit) {
        return {new node_delete_t{resource,
                                  std::move(dbname_to),
                                  std::move(relname_to),
                                  std::move(dbname_from),
                                  std::move(relname_from),
                                  match,
                                  limit}};
    }

} // namespace components::logical_plan

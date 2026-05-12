#include "node_update.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"
#include <sstream>

namespace components::logical_plan {

    node_update_t::node_update_t(std::pmr::memory_resource* resource,
                                 std::string dbname_to,
                                 std::string relname_to,
                                 std::string dbname_from,
                                 std::string relname_from,
                                 const node_match_ptr& match,
                                 const node_limit_ptr& limit,
                                 const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                 bool upsert)
        : node_t(resource, node_type::update_t)
        , dbname_(std::move(dbname_to))
        , relname_(std::move(relname_to))
        , dbname_from_(std::move(dbname_from))
        , relname_from_(std::move(relname_from))
        , update_expressions_(updates)
        , upsert_(upsert) {
        append_child(match);
        append_child(limit);
    }

    const std::pmr::vector<expressions::update_expr_ptr>& node_update_t::updates() const { return update_expressions_; }

    bool node_update_t::upsert() const { return upsert_; }

    hash_t node_update_t::hash_impl() const { return 0; }

    std::string node_update_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$update: {";
        stream << "$upsert: " << upsert_ << ", ";
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

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          std::string dbname,
                                          std::string relname,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert) {
        auto limit = make_node_limit(resource, dbname, relname, limit_t::unlimit());
        return {new node_update_t{resource,
                                  std::move(dbname),
                                  std::move(relname),
                                  {},
                                  {},
                                  match,
                                  limit,
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update_many(std::pmr::memory_resource* resource,
                                          std::string dbname_to,
                                          std::string relname_to,
                                          std::string dbname_from,
                                          std::string relname_from,
                                          const node_match_ptr& match,
                                          const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                          bool upsert) {
        auto limit = make_node_limit(resource, dbname_to, relname_to, limit_t::unlimit());
        return {new node_update_t{resource,
                                  std::move(dbname_to),
                                  std::move(relname_to),
                                  std::move(dbname_from),
                                  std::move(relname_from),
                                  match,
                                  limit,
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         std::string dbname,
                                         std::string relname,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert) {
        auto limit = make_node_limit(resource, dbname, relname, limit_t::limit_one());
        return {new node_update_t{resource,
                                  std::move(dbname),
                                  std::move(relname),
                                  {},
                                  {},
                                  match,
                                  limit,
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update_one(std::pmr::memory_resource* resource,
                                         std::string dbname_to,
                                         std::string relname_to,
                                         std::string dbname_from,
                                         std::string relname_from,
                                         const node_match_ptr& match,
                                         const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                         bool upsert) {
        auto limit = make_node_limit(resource, dbname_to, relname_to, limit_t::limit_one());
        return {new node_update_t{resource,
                                  std::move(dbname_to),
                                  std::move(relname_to),
                                  std::move(dbname_from),
                                  std::move(relname_from),
                                  match,
                                  limit,
                                  updates,
                                  upsert}};
    }

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert) {
        return {new node_update_t{resource, std::move(dbname), std::move(relname), {}, {}, match, limit, updates, upsert}};
    }

    node_update_ptr make_node_update(std::pmr::memory_resource* resource,
                                     std::string dbname_to,
                                     std::string relname_to,
                                     std::string dbname_from,
                                     std::string relname_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit,
                                     const std::pmr::vector<expressions::update_expr_ptr>& updates,
                                     bool upsert) {
        return {new node_update_t{resource,
                                  std::move(dbname_to),
                                  std::move(relname_to),
                                  std::move(dbname_from),
                                  std::move(relname_from),
                                  match,
                                  limit,
                                  updates,
                                  upsert}};
    }

} // namespace components::logical_plan

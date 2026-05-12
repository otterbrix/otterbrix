#include "node_create_view.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_view_t::node_create_view_t(std::pmr::memory_resource* resource,
                                           std::string dbname,
                                           std::string viewname,
                                           std::string query_sql)
        : node_t(resource, node_type::create_view_t)
        , dbname_(std::move(dbname))
        , viewname_(std::move(viewname))
        , query_sql_(std::move(query_sql)) {}

    hash_t node_create_view_t::hash_impl() const { return 0; }

    std::string node_create_view_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_view: " << dbname_ << "." << viewname_;
        return stream.str();
    }

    node_create_view_ptr make_node_create_view(std::pmr::memory_resource* resource,
                                               std::string dbname,
                                               std::string viewname,
                                               std::string query_sql) {
        return {new node_create_view_t{resource, std::move(dbname), std::move(viewname), std::move(query_sql)}};
    }

} // namespace components::logical_plan

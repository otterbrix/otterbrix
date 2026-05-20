#include "node_create_view.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_view_t::node_create_view_t(std::pmr::memory_resource* resource,
                                           core::viewname_t viewname,
                                           core::query_sql_t query_sql)
        : node_t(resource, node_type::create_view_t)
        , viewname_(std::move(static_cast<std::string&>(viewname)))
        , query_sql_(std::move(static_cast<std::string&>(query_sql))) {}

    hash_t node_create_view_t::hash_impl() const { return 0; }

    std::string node_create_view_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_view: " << viewname_;
        return stream.str();
    }

    node_create_view_ptr
    make_node_create_view(std::pmr::memory_resource* resource, core::viewname_t viewname, core::query_sql_t query_sql) {
        return {new node_create_view_t{resource, std::move(viewname), std::move(query_sql)}};
    }

} // namespace components::logical_plan

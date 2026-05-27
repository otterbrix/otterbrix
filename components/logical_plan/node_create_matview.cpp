#include "node_create_matview.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_matview_t::node_create_matview_t(std::pmr::memory_resource* resource,
                                                 core::matviewname_t matviewname,
                                                 core::body_sql_t body_sql)
        : node_t(resource, node_type::create_matview_t)
        , matviewname_(std::move(static_cast<std::string&>(matviewname)))
        , body_sql_(std::move(static_cast<std::string&>(body_sql))) {}

    void node_create_matview_t::set_body_plan(node_ptr plan) {
        children_.clear();
        if (plan) {
            append_child(plan);
        }
    }

    hash_t node_create_matview_t::hash_impl() const { return 0; }

    std::string node_create_matview_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_matview: " << matviewname_;
        return stream.str();
    }

    node_create_matview_ptr make_node_create_matview(std::pmr::memory_resource* resource,
                                                     core::matviewname_t matviewname,
                                                     core::body_sql_t body_sql) {
        return {new node_create_matview_t{resource, std::move(matviewname), std::move(body_sql)}};
    }

} // namespace components::logical_plan

#include "node_create_matview.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_matview_t::node_create_matview_t(std::pmr::memory_resource* resource, core::matviewname_t matviewname)
        : node_t(resource, node_type::create_matview_t)
        , matviewname_(std::move(static_cast<std::string&>(matviewname))) {}

    hash_t node_create_matview_t::hash_impl() const { return 0; }

    std::string node_create_matview_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_matview: " << matviewname_;
        return stream.str();
    }

    node_create_matview_ptr make_node_create_matview(std::pmr::memory_resource* resource,
                                                     core::matviewname_t matviewname) {
        return {new node_create_matview_t{resource, std::move(matviewname)}};
    }

} // namespace components::logical_plan

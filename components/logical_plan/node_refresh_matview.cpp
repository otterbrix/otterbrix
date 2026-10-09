#include "node_refresh_matview.hpp"

#include <sstream>

namespace components::logical_plan {

    node_refresh_matview_t::node_refresh_matview_t(std::pmr::memory_resource* resource, bool with_data)
        : node_t(resource, node_type::refresh_matview_t)
        , with_data_(with_data) {}

    hash_t node_refresh_matview_t::hash_impl() const { return 0; }

    std::string node_refresh_matview_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$refresh_matview: " << target_.collection;
        return stream.str();
    }

    node_refresh_matview_ptr make_node_refresh_matview(std::pmr::memory_resource* resource, bool with_data) {
        return {new node_refresh_matview_t{resource, with_data}};
    }

} // namespace components::logical_plan

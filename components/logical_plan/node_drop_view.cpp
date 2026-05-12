#include "node_drop_view.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_view_t::node_drop_view_t(std::pmr::memory_resource* resource, std::string dbname, std::string viewname)
        : node_t(resource, node_type::drop_view_t)
        , dbname_(std::move(dbname))
        , viewname_(std::move(viewname)) {}

    hash_t node_drop_view_t::hash_impl() const { return 0; }

    std::string node_drop_view_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_view: " << dbname_ << "." << viewname_;
        return stream.str();
    }

    node_drop_view_ptr make_node_drop_view(std::pmr::memory_resource* resource, std::string dbname, std::string viewname) {
        return {new node_drop_view_t{resource, std::move(dbname), std::move(viewname)}};
    }

} // namespace components::logical_plan

#include "node_drop_view.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_view_t::node_drop_view_t(std::pmr::memory_resource* resource)
        : node_t(resource, node_type::drop_view_t) {}

    hash_t node_drop_view_t::hash_impl() const { return static_cast<hash_t>(relation_oid_); }

    std::string node_drop_view_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_view: <oid:" << static_cast<std::uint64_t>(relation_oid_) << ">";
        return stream.str();
    }

    node_drop_view_ptr make_node_drop_view(std::pmr::memory_resource* resource) {
        return {new node_drop_view_t{resource}};
    }

} // namespace components::logical_plan

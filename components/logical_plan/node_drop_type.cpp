#include "node_drop_type.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_type_t::node_drop_type_t(std::pmr::memory_resource* resource, std::string&& name)
        : node_t(resource, node_type::drop_type_t, {})
        , name_(std::move(name)) {}

    const std::string& node_drop_type_t::name() const noexcept { return name_; }

    hash_t node_drop_type_t::hash_impl() const { return 0; }

    std::string node_drop_type_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_type: name: " << name_;
        return stream.str();
    }

    node_drop_type_ptr make_node_drop_type(std::pmr::memory_resource* resource, std::string&& name) {
        return {new node_drop_type_t{resource, std::move(name)}};
    }

} // namespace components::logical_plan
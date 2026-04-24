#include "node_set_timezone.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_set_timezone_t::node_set_timezone_t(std::pmr::memory_resource* resource, std::string timezone_name)
        : node_t(resource, node_type::set_timezone_t, collection_full_name_t{})
        , timezone_name_(std::move(timezone_name)) {}

    const std::string& node_set_timezone_t::timezone_name() const noexcept { return timezone_name_; }

    hash_t node_set_timezone_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, timezone_name_);
        return hash_value;
    }

    std::string node_set_timezone_t::to_string_impl() const { return "$set_timezone: " + timezone_name_; }

    node_set_timezone_ptr make_node_set_timezone(std::pmr::memory_resource* resource, std::string timezone_name) {
        return {new node_set_timezone_t{resource, std::move(timezone_name)}};
    }

} // namespace components::logical_plan

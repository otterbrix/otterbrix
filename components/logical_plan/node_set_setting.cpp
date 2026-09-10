#include "node_set_setting.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_set_setting_t::node_set_setting_t(std::pmr::memory_resource* resource,
                                           catalog::setting_id id,
                                           std::string value)
        : node_t(resource, node_type::set_setting_t)
        , setting_(id)
        , value_(std::move(value)) {}

    catalog::setting_id node_set_setting_t::setting() const noexcept { return setting_; }

    const std::string& node_set_setting_t::value() const noexcept { return value_; }

    hash_t node_set_setting_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(setting_));
        boost::hash_combine(hash_value, value_);
        return hash_value;
    }

    std::string node_set_setting_t::to_string_impl() const {
        const auto& def = catalog::find_setting_by_id(setting_);
        return "$set_setting: " + std::string(def.sql_name) + " = " + value_;
    }

    node_set_setting_ptr
    make_node_set_setting(std::pmr::memory_resource* resource, catalog::setting_id id, std::string value) {
        return {new node_set_setting_t{resource, id, std::move(value)}};
    }

} // namespace components::logical_plan

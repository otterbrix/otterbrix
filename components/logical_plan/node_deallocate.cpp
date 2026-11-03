#include "node_deallocate.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_deallocate_t::node_deallocate_t(std::pmr::memory_resource* resource, std::string name, bool all)
        : node_t(resource, node_type::deallocate_t)
        , name_(std::move(name))
        , all_(all) {}

    const std::string& node_deallocate_t::name() const noexcept { return name_; }

    bool node_deallocate_t::all() const noexcept { return all_; }

    void node_deallocate_t::set_exists(bool exists) noexcept { exists_ = exists; }

    bool node_deallocate_t::exists() const noexcept { return exists_; }

    hash_t node_deallocate_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, name_);
        boost::hash_combine(hash_value, all_);
        return hash_value;
    }

    std::string node_deallocate_t::to_string_impl() const { return "$deallocate: " + (all_ ? "ALL" : name_); }

    node_deallocate_ptr make_node_deallocate(std::pmr::memory_resource* resource, std::string name) {
        return {new node_deallocate_t{resource, std::move(name), false}};
    }

    node_deallocate_ptr make_node_deallocate_all(std::pmr::memory_resource* resource) {
        return {new node_deallocate_t{resource, std::string{}, true}};
    }

} // namespace components::logical_plan

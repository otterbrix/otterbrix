#include "node_remote.hpp"

namespace components::logical_plan {

    node_remote_t::node_remote_t(std::pmr::memory_resource* resource,
                                 kind_t kind,
                                 table::storage_address_t address,
                                 std::pmr::string request,
                                 std::pmr::vector<std::pmr::string> explain)
        : node_t(resource, node_type::remote_t)
        , kind_(kind)
        , address_(std::move(address))
        , request_(std::move(request), resource)
        , explain_(std::move(explain), resource)
        , tables_(resource)
        , returning_(resource) {}

    hash_t node_remote_t::hash_impl() const { return 0; }

    std::string node_remote_t::to_string_impl() const {
        return std::string{kind_ == kind_t::read ? "$remote_read: " : "$remote_write: "} + address_.connection() +
               " {" + std::string{request_} + "}";
    }

} // namespace components::logical_plan

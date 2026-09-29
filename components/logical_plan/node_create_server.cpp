#include "node_create_server.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::logical_plan {

    node_create_server_t::node_create_server_t(std::pmr::memory_resource* resource,
                                               std::string servername,
                                               std::string servertype,
                                               components::catalog::generic_options_t options)
        : node_t(resource, node_type::create_server_t)
        , servername_(std::move(servername))
        , servertype_(std::move(servertype))
        , options_(std::move(options)) {}

    hash_t node_create_server_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, servername_);
        boost::hash_combine(hash_value, servertype_);
        return hash_value;
    }

    std::string node_create_server_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_server: " << servername_ << " type " << servertype_;
        return stream.str();
    }

    node_create_server_ptr make_node_create_server(std::pmr::memory_resource* resource,
                                                   std::string servername,
                                                   std::string servertype,
                                                   components::catalog::generic_options_t options) {
        return {new node_create_server_t{resource, std::move(servername), std::move(servertype), std::move(options)}};
    }

} // namespace components::logical_plan

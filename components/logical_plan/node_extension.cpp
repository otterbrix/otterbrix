#include "node_extension.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::logical_plan {

    node_extension_t::node_extension_t(std::pmr::memory_resource* resource,
                                       core::dbname_t dbname,
                                       core::relname_t relname)
        : node_t(resource, node_type::extension_t)
        , dbname_(std::move(static_cast<std::string&>(dbname)))
        , relname_(std::move(static_cast<std::string&>(relname))) {}

    std::string node_extension_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$extension: " << dbname_ << "." << relname_;
        return stream.str();
    }

    node_extension_ptr
    make_node_extension(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname) {
        return {new node_extension_t{resource, std::move(dbname), std::move(relname)}};
    }

} // namespace components::logical_plan

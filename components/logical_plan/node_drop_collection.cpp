#include "node_drop_collection.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_collection_t::node_drop_collection_t(std::pmr::memory_resource* resource,
                                                   std::string dbname,
                                                   std::string relname,
                                                   std::string schemaname,
                                                   std::string uuid)
        : node_t(resource, node_type::drop_collection_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , schemaname_(std::move(schemaname))
        , uuid_(std::move(uuid)) {}

    hash_t node_drop_collection_t::hash_impl() const { return 0; }

    std::string node_drop_collection_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_collection: " << dbname_ << "." << relname_;
        return stream.str();
    }

    node_drop_collection_ptr make_node_drop_collection(std::pmr::memory_resource* resource,
                                                       std::string dbname,
                                                       std::string relname,
                                                       std::string schemaname,
                                                       std::string uuid) {
        return {new node_drop_collection_t{resource,
                                            std::move(dbname),
                                            std::move(relname),
                                            std::move(schemaname),
                                            std::move(uuid)}};
    }

} // namespace components::logical_plan

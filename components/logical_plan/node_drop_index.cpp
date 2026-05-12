#include "node_drop_index.hpp"

#include <sstream>

namespace components::logical_plan {

    node_drop_index_t::node_drop_index_t(std::pmr::memory_resource* resource,
                                         std::string dbname,
                                         std::string relname,
                                         std::string indexname,
                                         std::string schemaname,
                                         std::string uuid)
        : node_t(resource, node_type::drop_index_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , indexname_(std::move(indexname))
        , schemaname_(std::move(schemaname))
        , uuid_(std::move(uuid)) {}

    const std::string& node_drop_index_t::name() const noexcept { return indexname_; }

    hash_t node_drop_index_t::hash_impl() const { return 0; }

    std::string node_drop_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_index: " << dbname_ << "." << relname_ << " name:" << indexname_;
        return stream.str();
    }

    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             std::string dbname,
                                             std::string relname,
                                             std::string indexname,
                                             std::string schemaname,
                                             std::string uuid) {
        return {new node_drop_index_t{resource,
                                       std::move(dbname),
                                       std::move(relname),
                                       std::move(indexname),
                                       std::move(schemaname),
                                       std::move(uuid)}};
    }

} // namespace components::logical_plan

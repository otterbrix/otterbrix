#include "node_create_index.hpp"

#include <sstream>

namespace components::logical_plan {

    node_create_index_t::node_create_index_t(std::pmr::memory_resource* resource,
                                             std::string dbname,
                                             std::string relname,
                                             std::string indexname,
                                             index_type type,
                                             std::string schemaname,
                                             std::string uuid)
        : node_t(resource, node_type::create_index_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , indexname_(std::move(indexname))
        , schemaname_(std::move(schemaname))
        , uuid_(std::move(uuid))
        , keys_(resource)
        , index_type_(type) {}

    const std::string& node_create_index_t::name() const noexcept { return indexname_; }

    index_type node_create_index_t::type() const noexcept { return index_type_; }

    keys_base_storage_t& node_create_index_t::keys() noexcept { return keys_; }

    hash_t node_create_index_t::hash_impl() const { return 0; }

    inline std::string name_index_type(index_type type) {
        switch (type) {
            case index_type::single:
                return "single";
            case index_type::composite:
                return "composite";
            case index_type::multikey:
                return "multikey";
            case index_type::hashed:
                return "hashed";
            case index_type::wildcard:
                return "wildcard";
            case index_type::no_valid:
                return "no_valid";
        }
        return "default";
    }

    std::string node_create_index_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_index: " << dbname_ << "." << relname_ << " name:" << indexname_ << "[ ";
        for (const auto& key : keys_) {
            stream << key.as_string() << ' ';
        }
        stream << "] type:" << name_index_type(index_type_);
        return stream.str();
    }

    node_create_index_ptr make_node_create_index(std::pmr::memory_resource* resource,
                                                 std::string dbname,
                                                 std::string relname,
                                                 std::string indexname,
                                                 index_type type,
                                                 std::string schemaname,
                                                 std::string uuid) {
        return {new node_create_index_t{resource,
                                        std::move(dbname),
                                        std::move(relname),
                                        std::move(indexname),
                                        type,
                                        std::move(schemaname),
                                        std::move(uuid)}};
    }

} // namespace components::logical_plan

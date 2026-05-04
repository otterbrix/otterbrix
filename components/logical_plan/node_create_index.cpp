#include "node_create_index.hpp"

#include <sstream>

namespace components::logical_plan {

    // serialize/deserialize retired together with INDEXES_METADATA — index DDL
    // now persists through pg_index rows and replays via WAL physical_inserts on
    // pg_catalog.pg_index, not through this plan-node serialization path.

    node_create_index_t::node_create_index_t(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name,
                                             index_type type)
        : node_t(resource, node_type::create_index_t, collection)
        , name_(name)
        , index_type_(type) {}

    const std::string& node_create_index_t::name() const noexcept { return name_; }

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
        stream << "$create_index: " << database_name() << "." << collection_name() << " name:" << name() << "[ ";
        for (const auto& key : keys_) {
            stream << key.as_string() << ' ';
        }
        stream << "] type:" << name_index_type(index_type_);
        return stream.str();
    }

    node_create_index_ptr make_node_create_index(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& collection,
                                                 const std::string& name,
                                                 index_type type) {
        return {new node_create_index_t{resource, collection, name, type}};
    }

} // namespace components::logical_plan

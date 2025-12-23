#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_create_database_t final : public node_t {
    public:
        explicit node_create_database_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        static boost::intrusive_ptr<node_create_database_t>
        deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    using node_create_database_ptr = boost::intrusive_ptr<node_create_database_t>;
    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& collection);

} // namespace components::logical_plan

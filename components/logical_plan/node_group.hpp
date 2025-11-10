#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        static node_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;
    };

    using node_group_ptr = boost::intrusive_ptr<node_group_t>;

    node_group_ptr make_node_group(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::vector<expression_ptr>& expressions);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::pmr::vector<expression_ptr>& expressions);

} // namespace components::logical_plan

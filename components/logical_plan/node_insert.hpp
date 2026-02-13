#pragma once

#include "node.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>& key_translation();
        const std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>& key_translation() const;

        static boost::intrusive_ptr<node_insert_t> deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;

        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>> key_translation_;
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const components::vector::data_chunk_t& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk);

    node_insert_ptr
    make_node_insert(std::pmr::memory_resource* resource,
                     const collection_full_name_t& collection,
                     components::vector::data_chunk_t&& chunk,
                     std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>&& key_translation);

} // namespace components::logical_plan

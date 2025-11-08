#pragma once

#include "node.hpp"

#include <components/document/document.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    using data_t = std::variant<std::pmr::vector<components::document::document_ptr>, components::vector::data_chunk_t>;

    class node_data_t final : public node_t {
    public:
        explicit node_data_t(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::document::document_ptr>&& documents);

        explicit node_data_t(std::pmr::memory_resource* resource,
                             const std::pmr::vector<components::document::document_ptr>& documents);

        explicit node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

        explicit node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk);

        const std::pmr::vector<components::document::document_ptr>& documents() const;
        const components::vector::data_chunk_t& data_chunk() const;
        bool uses_data_chunk() const;
        bool uses_documents() const;

        size_t size() const;

        static node_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        data_t data_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;
    };

    using node_data_ptr = boost::intrusive_ptr<node_data_t>;

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::document::document_ptr>&& documents);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<components::document::document_ptr>& documents);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk);

} // namespace components::logical_plan

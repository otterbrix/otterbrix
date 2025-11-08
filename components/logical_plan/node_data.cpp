#include "node_data.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_data_t::node_data_t(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::document::document_ptr>&& documents)
        : node_t(resource, node_type::data_t, {})
        , data_(std::move(documents)) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource,
                             const std::pmr::vector<components::document::document_ptr>& documents)
        : node_t(resource, node_type::data_t, {})
        , data_(documents) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk)
        : node_t(resource, node_type::data_t, {})
        , data_(std::move(chunk)) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk)
        : node_t(resource, node_type::data_t, {})
        , data_(vector::data_chunk_t(resource, chunk.types(), chunk.size())) {
        chunk.copy(std::get<components::vector::data_chunk_t>(data_), 0);
    }

    const std::pmr::vector<document::document_ptr>& node_data_t::documents() const {
        return std::get<std::pmr::vector<document::document_ptr>>(data_);
    }

    const components::vector::data_chunk_t& node_data_t::data_chunk() const {
        return std::get<components::vector::data_chunk_t>(data_);
    }

    bool node_data_t::uses_data_chunk() const {
        return std::holds_alternative<components::vector::data_chunk_t>(data_);
    }

    bool node_data_t::uses_documents() const {
        return std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_);
    }

    size_t node_data_t::size() const {
        if (std::holds_alternative<std::pmr::vector<document::document_ptr>>(data_)) {
            return std::get<std::pmr::vector<document::document_ptr>>(data_).size();
        } else {
            return std::get<components::vector::data_chunk_t>(data_).size();
        }
    }

    node_ptr node_data_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        bool uses_data_chunk = deserializer->deserialize_bool(1);
        node_ptr result;
        if (uses_data_chunk) {
            deserializer->advance_array(2);
            result = make_node_raw_data(deserializer->resource(), vector::data_chunk_t::deserialize(deserializer));
            deserializer->pop_array();
        } else {
            result = make_node_raw_data(deserializer->resource(), deserializer->deserialize_documents(2));
        }
        return result;
    }

    hash_t node_data_t::hash_impl() const { return 0; }

    std::string node_data_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$raw_data: {";
        //todo: all rows
        stream << "$rows: " << size();
        stream << "}";
        return stream.str();
    }

    void node_data_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        // TODO: serializer data chunk
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_data);
        if (uses_data_chunk()) {
            serializer->append(true);
            data_chunk().serialize(serializer);
        } else {
            serializer->append(false);
            serializer->append(documents());
        }
        serializer->end_array();
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     std::pmr::vector<components::document::document_ptr>&& documents) {
        return {new node_data_t{resource, std::move(documents)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<components::document::document_ptr>& documents) {
        return {new node_data_t{resource, documents}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk) {
        return {new node_data_t{resource, std::move(chunk)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk) {
        return {new node_data_t{resource, chunk}};
    }

} // namespace components::logical_plan

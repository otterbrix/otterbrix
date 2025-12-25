#include "node_insert.hpp"

#include "node_data.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_insert_t::node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::insert_t, collection)
        , key_translation_(resource) {}

    std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>& node_insert_t::key_translation() {
        return key_translation_;
    }

    const std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>& node_insert_t::key_translation() const {
        return key_translation_;
    }

    node_insert_ptr node_insert_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto collection = deserializer->deserialize_collection(1);

        auto res = make_node_insert(deserializer->resource(), collection);
        deserializer->advance_array(2);
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            deserializer->advance_array(i);
            res->append_child(node_t::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();

        std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>> key_translation(deserializer->resource());
        deserializer->advance_array(3);
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            deserializer->advance_array(i);
            key_translation.emplace_back(deserializer->deserialize_key(0), deserializer->deserialize_key(1));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        res->key_translation() = key_translation;
        return res;
    }

    hash_t node_insert_t::hash_impl() const { return 0; }

    std::string node_insert_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$insert: {";
        stream << children_.front()->to_string();
        stream << "}";
        return stream.str();
    }

    void node_insert_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(serializer::serialization_type::logical_node_insert);
        serializer->append(collection_);
        serializer->start_array(children_.size());
        for (const auto& n : children_) {
            n->serialize(serializer);
        }
        serializer->end_array();
        serializer->start_array(key_translation_.size());
        for (const auto& k_pair : key_translation_) {
            serializer->start_array(2);
            serializer->append(k_pair.first);
            serializer->append(k_pair.second);
            serializer->end_array();
        }
        serializer->end_array();
        serializer->end_array();
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, const collection_full_name_t& collection) {
        return {new node_insert_t{resource, collection}};
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::pmr::vector<components::document::document_ptr>& documents) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, documents));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     std::pmr::vector<components::document::document_ptr>&& documents) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(documents)));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const components::vector::data_chunk_t& chunk) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, chunk));
        return res;
    }

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(chunk)));
        return res;
    }

    node_insert_ptr
    make_node_insert(std::pmr::memory_resource* resource,
                     const collection_full_name_t& collection,
                     std::pmr::vector<components::document::document_ptr>&& documents,
                     std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>&& key_translation) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(documents)));
        res->key_translation() = key_translation;
        return res;
    }

    node_insert_ptr
    make_node_insert(std::pmr::memory_resource* resource,
                     const collection_full_name_t& collection,
                     components::vector::data_chunk_t&& chunk,
                     std::pmr::vector<std::pair<expressions::key_t, expressions::key_t>>&& key_translation) {
        auto res = make_node_insert(resource, collection);
        res->append_child(make_node_raw_data(resource, std::move(chunk)));
        res->key_translation() = key_translation;
        return res;
    }

} // namespace components::logical_plan

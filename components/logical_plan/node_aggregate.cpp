#include "node_aggregate.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_aggregate_t::node_aggregate_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::aggregate_t, collection) {}

    node_aggregate_ptr node_aggregate_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        collection_full_name_t collection = deserializer->deserialize_collection(1);
        auto res = make_node_aggregate(deserializer->resource(), collection);

        deserializer->advance_array(2);
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            deserializer->advance_array(i);
            res->append_child(node_t::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        return res;
    }

    hash_t node_aggregate_t::hash_impl() const { return 0; }

    std::string node_aggregate_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$aggregate: {";
        bool is_first = true;
        for (const auto& child : children_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << child->to_string();
        }
        stream << "}";
        return stream.str();
    }

    void node_aggregate_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_aggregate);
        serializer->append(collection_);
        serializer->start_array(children_.size());
        for (const auto& n : children_) {
            n->serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& collection) {
        return {new node_aggregate_t(resource, collection)};
    }

} // namespace components::logical_plan
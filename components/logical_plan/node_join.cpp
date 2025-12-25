#include "node_join.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_join_t::node_join_t(std::pmr::memory_resource* resource,
                             const collection_full_name_t& collection,
                             join_type type)
        : node_t(resource, node_type::join_t, collection)
        , type_(type) {}

    join_type node_join_t::type() const { return type_; }

    node_join_ptr node_join_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto type = deserializer->deserialize_enum<join_type>(1);
        auto collection = deserializer->deserialize_collection(2);
        auto res = make_node_join(deserializer->resource(), collection, type);
        deserializer->advance_array(3);
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            deserializer->advance_array(i);
            res->append_child(node_t::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();

        return res;
    }

    hash_t node_join_t::hash_impl() const { return 0; }

    std::string node_join_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$join: {";
        stream << "$type: " << magic_enum::enum_name(type_);
        for (const auto& child : children_) {
            stream << ", " << child->to_string();
        }
        for (const auto& expr : expressions()) {
            stream << ", " << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    void node_join_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(4);
        serializer->append_enum(serializer::serialization_type::logical_node_join);
        serializer->append_enum(type_);
        serializer->append(collection_);
        serializer->start_array(children_.size());
        for (const auto& n : children_) {
            n->serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    node_join_ptr
    make_node_join(std::pmr::memory_resource* resource, const collection_full_name_t& collection, join_type type) {
        return {new node_join_t{resource, collection, type}};
    }

} // namespace components::logical_plan

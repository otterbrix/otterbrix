#include "node_match.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_match_t::node_match_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::match_t, collection) {}

    node_match_ptr node_match_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto collection = deserializer->deserialize_collection(1);
        deserializer->advance_array(2);
        auto expr = expressions::expression_i::deserialize(deserializer);
        deserializer->pop_array();
        return make_node_match(deserializer->resource(), collection, expr);
    }

    hash_t node_match_t::hash_impl() const { return 0; }

    std::string node_match_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$match: {";
        bool is_first = true;
        for (const auto& expr : expressions_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << expr->to_string();
        }
        stream << "}";
        return stream.str();
    }

    void node_match_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_match);
        serializer->append(collection_);
        expressions_.front()->serialize(serializer);
        serializer->end_array();
    }

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const expressions::expression_ptr& match) {
        node_match_ptr node = new node_match_t{resource, collection};
        if (match) {
            node->append_expression(match);
        }
        return node;
    }

} // namespace components::logical_plan
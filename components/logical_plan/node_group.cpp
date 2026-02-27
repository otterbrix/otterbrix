#include "node_group.hpp"

#include <components/serialization/deserializer.hpp>

#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_group_t::node_group_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection,
                               expression_ptr having)
        : node_t(resource, node_type::group_t, collection)
        , having_(std::move(having)) {}

    node_group_ptr node_group_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto collection = deserializer->deserialize_collection(1);
        deserializer->advance_array(2);
        std::pmr::vector<logical_plan::expression_ptr> exprs(deserializer->resource());
        exprs.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < deserializer->current_array_size(); i++) {
            deserializer->advance_array(i);
            exprs.emplace_back(expressions::expression_i::deserialize(deserializer));
            deserializer->pop_array();
        }
        deserializer->pop_array();
        return make_node_group(deserializer->resource(), collection, exprs);
    }

    hash_t node_group_t::hash_impl() const { return 0; }

    std::string node_group_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$group: {";
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

    void node_group_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_group);
        serializer->append(collection_);
        serializer->start_array(expressions_.size());
        for (const auto& expr : expressions_) {
            expr->serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   expression_ptr having) {
        return {new node_group_t{resource, collection, std::move(having)}};
    }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::vector<expression_ptr>& expressions,
                                   expression_ptr having) {
        auto node = new node_group_t{resource, collection, std::move(having)};
        node->append_expressions(expressions);
        return node;
    }

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::pmr::vector<expression_ptr>& expressions,
                                   expression_ptr having) {
        auto node = new node_group_t{resource, collection, std::move(having)};
        node->append_expressions(expressions);
        return node;
    }

} // namespace components::logical_plan
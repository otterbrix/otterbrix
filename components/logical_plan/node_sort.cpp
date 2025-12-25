#include "node_sort.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_sort_t::node_sort_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection)
        : node_t(resource, node_type::sort_t, collection) {}

    node_sort_ptr node_sort_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
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
        return make_node_sort(deserializer->resource(), collection, exprs);
    }

    hash_t node_sort_t::hash_impl() const { return 0; }

    std::string node_sort_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$sort: {";
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

    void node_sort_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_sort);
        serializer->append(collection_);
        serializer->start_array(expressions_.size());
        for (const auto& expr : expressions_) {
            expr->serialize(serializer);
        }
        serializer->end_array();
        serializer->end_array();
    }

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::vector<expressions::expression_ptr>& expressions) {
        auto node = new node_sort_t{resource, collection};
        node->append_expressions(expressions);
        return node;
    }

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions) {
        auto node = new node_sort_t{resource, collection};
        node->append_expressions(expressions);
        return node;
    }

} // namespace components::logical_plan
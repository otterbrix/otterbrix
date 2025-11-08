#pragma once

#include "node.hpp"

#include <components/expressions/compare_expression.hpp>

namespace components::logical_plan {

    class node_sort_t final : public node_t {
    public:
        explicit node_sort_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        static node_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;
    };

    using node_sort_ptr = boost::intrusive_ptr<node_sort_t>;

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::vector<expressions::expression_ptr>& expressions);

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions);

} // namespace components::logical_plan
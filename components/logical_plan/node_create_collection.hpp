#pragma once

#include "node.hpp"

#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          std::pmr::vector<types::complex_logical_type> schema = {});

        static node_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

        std::pmr::vector<types::complex_logical_type>& schema();
        const std::pmr::vector<types::complex_logical_type>& schema() const;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;

        std::pmr::vector<types::complex_logical_type> schema_;
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema = {});

} // namespace components::logical_plan

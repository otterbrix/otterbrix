#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource,
                              const collection_full_name_t& collection,
                              expression_ptr having = nullptr);

        const expression_ptr& having() const { return having_; }

        static boost::intrusive_ptr<node_group_t> deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        expression_ptr having_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    using node_group_ptr = boost::intrusive_ptr<node_group_t>;

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   expression_ptr having = nullptr);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::vector<expression_ptr>& expressions,
                                   expression_ptr having = nullptr);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::pmr::vector<expression_ptr>& expressions,
                                   expression_ptr having = nullptr);

} // namespace components::logical_plan

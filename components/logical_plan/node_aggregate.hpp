#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_aggregate_t final : public node_t {
    public:
        explicit node_aggregate_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        static boost::intrusive_ptr<node_aggregate_t> deserialize(serializer::msgpack_deserializer_t* deserializer);

        void set_distinct(bool d) { distinct_ = d; }
        bool is_distinct() const { return distinct_; }

    private:
        bool distinct_{false};
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    using node_aggregate_ptr = boost::intrusive_ptr<node_aggregate_t>;

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           const collection_full_name_t& collection);

} // namespace components::logical_plan

#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_drop_sequence_t final : public node_t {
    public:
        explicit node_drop_sequence_t(std::pmr::memory_resource* resource,
                                      const collection_full_name_t& name);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    using node_drop_sequence_ptr = boost::intrusive_ptr<node_drop_sequence_t>;
    node_drop_sequence_ptr make_node_drop_sequence(std::pmr::memory_resource* resource,
                                                   const collection_full_name_t& name);

} // namespace components::logical_plan

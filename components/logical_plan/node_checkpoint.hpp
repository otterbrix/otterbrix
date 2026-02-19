#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_checkpoint_t final : public node_t {
    public:
        explicit node_checkpoint_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    using node_checkpoint_ptr = boost::intrusive_ptr<node_checkpoint_t>;
    node_checkpoint_ptr make_node_checkpoint(std::pmr::memory_resource* resource);

} // namespace components::logical_plan

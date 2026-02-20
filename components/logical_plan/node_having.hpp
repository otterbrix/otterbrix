#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_having_t final : public node_t {
    public:
        explicit node_having_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    using node_having_ptr = boost::intrusive_ptr<node_having_t>;

    node_having_ptr make_node_having(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const expressions::expression_ptr& expr);

} // namespace components::logical_plan

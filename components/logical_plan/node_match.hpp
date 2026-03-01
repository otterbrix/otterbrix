#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_match_t final : public node_t {
    public:
        explicit node_match_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_match_ptr = boost::intrusive_ptr<node_match_t>;

    node_match_ptr make_node_match(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const expressions::expression_ptr& match);

} // namespace components::logical_plan
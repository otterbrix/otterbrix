#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_drop_macro_t final : public node_t {
    public:
        explicit node_drop_macro_t(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& name);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_drop_macro_ptr = boost::intrusive_ptr<node_drop_macro_t>;
    node_drop_macro_ptr make_node_drop_macro(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& name);

} // namespace components::logical_plan

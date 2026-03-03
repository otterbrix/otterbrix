#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_drop_index_t final : public node_t {
    public:
        explicit node_drop_index_t(std::pmr::memory_resource* resource,
                                   const collection_full_name_t& collection,
                                   const std::string& name);

        const std::string& name() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
    };

    using node_drop_index_ptr = boost::intrusive_ptr<node_drop_index_t>;
    node_drop_index_ptr make_node_drop_index(std::pmr::memory_resource* resource,
                                             const collection_full_name_t& collection,
                                             const std::string& name);

} // namespace components::logical_plan
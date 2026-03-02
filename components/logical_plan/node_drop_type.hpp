#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_drop_type_t final : public node_t {
    public:
        explicit node_drop_type_t(std::pmr::memory_resource* resource, std::string&& name);

        const std::string& name() const noexcept;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        std::string name_;
    };

    using node_drop_type_ptr = boost::intrusive_ptr<node_drop_type_t>;

    node_drop_type_ptr make_node_drop_type(std::pmr::memory_resource* resource, std::string&& name);

} // namespace components::logical_plan

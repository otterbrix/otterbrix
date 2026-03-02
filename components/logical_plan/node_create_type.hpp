#pragma once

#include "node.hpp"
#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_type_t final : public node_t {
    public:
        explicit node_create_type_t(std::pmr::memory_resource* resource, types::complex_logical_type&& type);

        types::complex_logical_type& type() noexcept;
        const types::complex_logical_type& type() const noexcept;

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        types::complex_logical_type type_;
    };

    using node_create_type_ptr = boost::intrusive_ptr<node_create_type_t>;

    node_create_type_ptr make_node_create_type(std::pmr::memory_resource* resource, types::complex_logical_type&& type);

} // namespace components::logical_plan

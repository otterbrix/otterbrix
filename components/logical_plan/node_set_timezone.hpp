#pragma once

#include "node.hpp"

#include <string>

namespace components::logical_plan {

    class node_set_timezone_t final : public node_t {
    public:
        node_set_timezone_t(std::pmr::memory_resource* resource, std::string timezone_name);

        const std::string& timezone_name() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string timezone_name_;
    };

    using node_set_timezone_ptr = boost::intrusive_ptr<node_set_timezone_t>;
    node_set_timezone_ptr make_node_set_timezone(std::pmr::memory_resource* resource, std::string timezone_name);

} // namespace components::logical_plan

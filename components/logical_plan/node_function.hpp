#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_function_t final : public node_t {
    public:
        explicit node_function_t(std::pmr::memory_resource* resource, std::string&& name);
        explicit node_function_t(std::pmr::memory_resource* resource,
                                 std::string&& name,
                                 std::pmr::vector<expressions::param_storage>&& args);

        const std::string& name() const noexcept;
        const std::pmr::vector<expressions::param_storage>& args() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        std::pmr::vector<expressions::param_storage> args_;
    };

    using node_function_ptr = boost::intrusive_ptr<node_function_t>;

    node_function_ptr make_node_function(std::pmr::memory_resource* resource, std::string&& name);
    node_function_ptr make_node_function(std::pmr::memory_resource* resource,
                                         std::string&& name,
                                         std::pmr::vector<expressions::param_storage>&& args);

} // namespace components::logical_plan

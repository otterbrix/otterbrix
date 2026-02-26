#include "node_function.hpp"

#include <sstream>

namespace components::logical_plan {

    node_function_t::node_function_t(std::pmr::memory_resource* resource, std::string&& name)
        : node_t(resource, node_type::function_t, {})
        , name_(std::move(name)) {}

    node_function_t::node_function_t(std::pmr::memory_resource* resource,
                                     std::string&& name,
                                     std::pmr::vector<expressions::param_storage>&& args)
        : node_t(resource, node_type::function_t, {})
        , name_(std::move(name))
        , args_(std::move(args)) {}

    const std::string& node_function_t::name() const noexcept { return name_; }

    const std::pmr::vector<expressions::param_storage>& node_function_t::args() const noexcept { return args_; }

    hash_t node_function_t::hash_impl() const { return 0; }

    std::string node_function_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$function: {";
        stream << "name: {\"" << name_ << "\"}, ";
        stream << "args: {";
        bool is_first = true;
        for (const auto& arg : args_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << arg;
        }
        stream << "}}";
        return stream.str();
    }

    node_function_ptr make_node_function(std::pmr::memory_resource* resource, std::string&& name) {
        return {new node_function_t(resource, std::move(name))};
    }

    node_function_ptr make_node_function(std::pmr::memory_resource* resource,
                                         std::string&& name,
                                         std::pmr::vector<expressions::param_storage>&& args) {
        return {new node_function_t(resource, std::move(name), std::move(args))};
    }

} // namespace components::logical_plan

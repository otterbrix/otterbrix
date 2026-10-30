#include "node_execute.hpp"

#include <boost/container_hash/hash.hpp>

namespace components::logical_plan {

    node_execute_t::node_execute_t(std::pmr::memory_resource* resource,
                                   std::string name,
                                   std::pmr::vector<execute_argument_t> arguments,
                                   core::error_t refusal)
        : node_t(resource, node_type::execute_t)
        , name_(std::move(name))
        , arguments_(std::move(arguments))
        , refusal_(std::move(refusal)) {}

    const std::string& node_execute_t::name() const noexcept { return name_; }

    const std::pmr::vector<execute_argument_t>& node_execute_t::arguments() const noexcept { return arguments_; }

    const core::error_t& node_execute_t::refusal() const noexcept { return refusal_; }

    hash_t node_execute_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, name_);
        for (const auto& argument : arguments_) {
            boost::hash_combine(hash_value, static_cast<uint64_t>(argument.slot));
            boost::hash_combine(hash_value, argument.quoted_literal);
        }
        return hash_value;
    }

    std::string node_execute_t::to_string_impl() const {
        std::string out = "$execute: " + name_ + "(";
        for (std::size_t i = 0; i < arguments_.size(); ++i) {
            out += i == 0 ? "#" : ", #";
            out += std::to_string(static_cast<uint64_t>(arguments_[i].slot));
        }
        return out + ")";
    }

    node_execute_ptr make_node_execute(std::pmr::memory_resource* resource,
                                       std::string name,
                                       std::pmr::vector<execute_argument_t> arguments,
                                       const core::error_t& refusal) {
        return {new node_execute_t{resource, std::move(name), std::move(arguments), core::error_on(resource, refusal)}};
    }

    node_execute_ptr make_node_execute(std::pmr::memory_resource* resource,
                                       std::string name,
                                       const std::pmr::vector<core::parameter_id_t>& slots) {
        std::pmr::vector<execute_argument_t> arguments{resource};
        arguments.reserve(slots.size());
        for (const auto slot : slots) {
            arguments.push_back(execute_argument_t{slot, false});
        }
        return make_node_execute(resource, std::move(name), std::move(arguments), core::error_t::no_error());
    }

} // namespace components::logical_plan

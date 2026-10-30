#pragma once

#include "node.hpp"

#include <core/parameter_id.hpp>
#include <core/result_wrapper.hpp>

#include <string>

namespace components::logical_plan {

    // An argument of EXECUTE: the slot of the plan's parameters that holds its value (a host's value, or what the
    // statement's sub-plan SELECT <argument> computed), and whether it was written as a bare string literal, which
    // PostgreSQL 18 types as unknown: it takes its parameter's type as a quoted VALUES cell of an INSERT does.
    struct execute_argument_t {
        core::parameter_id_t slot{};
        bool quoted_literal{false};
    };

    // EXECUTE name [(arguments)]: the planner expands the statement the session stored under `name`, as a view's
    // body is expanded. `refusal` is the first argument the transformer refused (a sub-query, a $n, a window
    // function): PostgreSQL 18 checks the number of arguments first and reports it after.
    class node_execute_t final : public node_t {
    public:
        node_execute_t(std::pmr::memory_resource* resource,
                       std::string name,
                       std::pmr::vector<execute_argument_t> arguments,
                       core::error_t refusal);

        const std::string& name() const noexcept;
        const std::pmr::vector<execute_argument_t>& arguments() const noexcept;
        const core::error_t& refusal() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        std::pmr::vector<execute_argument_t> arguments_;
        core::error_t refusal_;
    };

    using node_execute_ptr = boost::intrusive_ptr<node_execute_t>;
    node_execute_ptr make_node_execute(std::pmr::memory_resource* resource,
                                       std::string name,
                                       std::pmr::vector<execute_argument_t> arguments,
                                       const core::error_t& refusal);
    // A host's EXECUTE: the values are in the plan's parameters at `slots`.
    node_execute_ptr make_node_execute(std::pmr::memory_resource* resource,
                                       std::string name,
                                       const std::pmr::vector<core::parameter_id_t>& slots);

} // namespace components::logical_plan

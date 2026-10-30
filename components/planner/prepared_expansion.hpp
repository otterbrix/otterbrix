#pragma once

// EXECUTE runs a prepared statement as PostgreSQL 18 runs one with a custom plan every time: the text PREPARE stored
// is parsed and transformed again, without the host's grammar, as a view's body is, and $1..$n take the EXECUTE's
// values. A whole plan comes out: the body is not spliced into another, so a sub-query in it is its own.

#include <components/logical_plan/execution_plan.hpp>
#include <components/vector/vector.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <span>
#include <string>

namespace components::planner {

    enum class prepared_use_t
    {
        // The values are the EXECUTE's, each of its parameter's type.
        execute,
        // PREPARE's describe of the body: the values are typed NULLs of the declared types; the $n past them are
        // typed by their context.
        describe
    };

    core::result_wrapper_t<logical_plan::execution_plan_t> expand_prepared(std::pmr::memory_resource* resource,
                                                                           const std::string& text,
                                                                           std::span<const vector::vector_t> values,
                                                                           prepared_use_t use);

} // namespace components::planner

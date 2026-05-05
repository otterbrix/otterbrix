#pragma once

// Compile a SQL CHECK constraint expression into an evaluable predicate.
// The predicate is returned as a row_predicate_fn (see constraint_evaluator.hpp)
// so catalog does not take a link dependency on components/physical_plan.
//
// Implementation (in .cpp) uses sql::transform::transformer to parse conexpr
// and components::operators::predicates::create_predicate to compile the AST.
// Completed in Etap 3.5 when the planner is rewritten.

#include <components/catalog/constraint_evaluator.hpp>
#include <components/compute/function.hpp>
#include <components/types/complex_logical_type.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace components::catalog {

    // Parse and compile a SQL CHECK expression for the given column types.
    // Returns a predicate that returns true for rows that pass the CHECK.
    // Returns an always-true predicate if conexpr is empty or parsing fails.
    row_predicate_fn compile_check(
        std::pmr::memory_resource*                      resource,
        const compute::function_registry_t&             functions,
        const std::string&                              conexpr,
        const std::vector<types::complex_logical_type>& col_types);

} // namespace components::catalog

#pragma once

// Shared by the aggregate-pushdown POD-spec tests: the disk-side reduce test (services/disk/tests)
// and the planner-side spec-build test (components/planner/test) are different test targets, so
// the one definition lives here where both reach it via the repo-root include path.

#include <components/compute/function.hpp>
#include <components/expressions/aggregate_expression.hpp>

#include <memory_resource>

namespace pushdown_test {

    // Stamps the builtin "sum" into `aggregate` as validation does: its pin and a copy of the function.
    inline void stamp_sum(components::expressions::aggregate_expression_t& aggregate, std::pmr::memory_resource* r) {
        components::compute::function_registry_t reg{r};
        components::compute::register_default_functions(reg);
        const auto uid = reg.find_functions("sum").front();
        aggregate.set_pin(components::compute::function_pin_t{uid});
        aggregate.set_function(reg.get_function(uid)->get_copy(r));
    }

} // namespace pushdown_test

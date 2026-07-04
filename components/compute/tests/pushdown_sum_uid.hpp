#pragma once

// Shared test helper for the aggregate-pushdown POD-spec tests. Resolving the
// builtin "sum" function uid by NAME (from a fresh default registry) is duplicated
// by the disk-side reduce test (services/disk/tests) and the planner-side
// spec-build test (components/planner/test) — different test targets, so the one
// definition lives here where both reach it via the repo-root include path.

#include <components/compute/function.hpp>

#include <memory_resource>

namespace pushdown_test {

    // register_default_functions assigns deterministic uids; resolve "sum" by name
    // so an agent-side reduce (which rebuilds its OWN registry) and the spec-build
    // uid gate (uid < DEFAULT_FUNCTIONS.size()) both resolve the same function.
    inline components::compute::function_uid sum_uid(std::pmr::memory_resource* r) {
        components::compute::function_registry_t reg{r};
        components::compute::register_default_functions(reg);
        for (auto& [name, uid] : reg.get_functions()) {
            if (name == "sum") {
                return uid;
            }
        }
        return components::compute::invalid_function_uid;
    }

} // namespace pushdown_test

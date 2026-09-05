#pragma once

#include <components/types/types.hpp>
#include <memory_resource>
#include <vector>

namespace components::compute {
    class compute_kernel;
    class function_registry_t;
    class function_options;

    class exec_context_t {
    public:
        explicit exec_context_t(std::pmr::memory_resource* resource, function_registry_t* registry = nullptr);

        exec_context_t(const exec_context_t&) = default;
        exec_context_t(exec_context_t&& other) = default;
        exec_context_t& operator=(const exec_context_t&) = default;
        exec_context_t& operator=(exec_context_t&& other) = default;

        std::pmr::memory_resource* resource() const;
        function_registry_t* func_registry() const;

    private:
        std::pmr::memory_resource* resource_;
        function_registry_t* func_registry_;
    };

    // THERE IS NO default_exec_context(), AND THERE MUST NOT BE ONE. The obvious shape --
    // a function-local static exec_context_t on std::pmr::get_default_resource() (rule 14
    // forbids that call outright), handed out as the default argument of every public
    // function::execute / function::make_executor -- makes every caller that does not spell a
    // context out allocate from ONE process-wide arena it never named: 134 allocations /
    // 29 860 bytes for a single one-row execute(), measured in
    // components/compute/tests/test_exec_context_resource.cpp. A compute caller names its
    // resource; there is no default to fall back to (rule 6).

    struct kernel_init_args {
        const compute_kernel& kernel;
        const std::pmr::vector<types::complex_logical_type>& inputs;
        const function_options* options;
    };
} // namespace components::compute

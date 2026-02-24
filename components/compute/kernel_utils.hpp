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

    //TODO: remove default version, because it requires a static initialization of memory_resource
    exec_context_t& default_exec_context();

    struct kernel_init_args {
        const compute_kernel& kernel;
        const std::pmr::vector<types::complex_logical_type>& inputs;
        const function_options* options;
    };
} // namespace components::compute

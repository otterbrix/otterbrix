#pragma once

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <memory_resource>
#include <vector>

namespace components::compute {
    class compute_kernel;
    class function_registry_t;
    class function_options;

    bool all_inputs_valid(const vector::data_chunk_t& inputs);
    bool row_contains_null(const vector::data_chunk_t& inputs, uint64_t row);

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

    // No default_exec_context(): a defaulted `ctx` backed by get_default_resource() (banned)
    // made every unnamed-context caller share one process-wide arena -- measured at
    // 134 allocations / 29 860 bytes for a single one-row execute(). Every caller names its
    // resource explicitly instead.

    struct kernel_init_args {
        const compute_kernel& kernel;
        const std::pmr::vector<types::complex_logical_type>& inputs;
        const function_options* options;
    };
} // namespace components::compute

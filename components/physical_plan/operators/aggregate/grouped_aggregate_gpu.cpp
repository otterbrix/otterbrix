#include "grouped_aggregate_gpu_internal.hpp"

#include <stdexcept>
#include <string>

#include <fmt/format.h>

namespace components::operators::aggregate {

    void update_all_gpu(builtin_agg agg,
                        const vector::vector_t& vec,
                        const uint32_t* group_ids,
                        uint64_t count,
                        std::pmr::vector<raw_agg_state_t>& states) {
        if (count == 0 || states.empty()) {
            return;
        }

        if (gpu::try_run_update_all_gpu(agg, vec, group_ids, count, states)) {
            gpu::log_info_if_available(
                fmt::format("grouped_aggregate GPU execution: agg={}, rows={}, groups={}",
                            gpu::agg_name(agg),
                            count,
                            states.size()));
            return;
        }

#if defined(__linux__)
        const auto reason = gpu::opencl_runtime().failure_reason();
#else
        const auto reason = std::string{"OpenCL GPU runtime is unavailable on this platform"};
#endif

        if (gpu::gpu_group_aggregate_strict_enabled()) {
            throw std::runtime_error(
                fmt::format("OTTERBRIX_GROUP_AGG_GPU_STRICT=1: grouped aggregate GPU path is unavailable ({})", reason));
        }

        gpu::log_warn_if_available(
            fmt::format("grouped_aggregate GPU fallback to CPU: agg={}, reason={}", gpu::agg_name(agg), reason));
        update_all(agg, vec, group_ids, count, states);
    }

    void update_count_star_gpu(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states) {
        if (count == 0 || states.empty()) {
            return;
        }

        if (gpu::try_run_count_star_gpu(group_ids, count, states)) {
            gpu::log_info_if_available(
                fmt::format("grouped_aggregate GPU execution: agg=count(*), rows={}, groups={}", count, states.size()));
            return;
        }

#if defined(__linux__)
        const auto reason = gpu::opencl_runtime().failure_reason();
#else
        const auto reason = std::string{"OpenCL GPU runtime is unavailable on this platform"};
#endif

        if (gpu::gpu_group_aggregate_strict_enabled()) {
            throw std::runtime_error(
                fmt::format("OTTERBRIX_GROUP_AGG_GPU_STRICT=1: grouped count(*) GPU path is unavailable ({})", reason));
        }

        gpu::log_warn_if_available(fmt::format("grouped_aggregate GPU fallback to CPU: agg=count(*), reason={}", reason));
        update_count_star(group_ids, count, states);
    }
    bool gpu_group_aggregate_test_enabled() { return gpu::read_bool_env("OTTERBRIX_GROUP_AGG_GPU_TEST"); }

} // namespace components::operators::aggregate

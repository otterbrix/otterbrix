#include "grouped_aggregate_gpu_internal.hpp"

#include <stdexcept>
#include <string>

#include <fmt/format.h>

namespace components::operators::aggregate {

    namespace {

        [[noreturn]] void throw_gpu_strict_error(const std::string& operation, const std::string& reason) {
            throw std::runtime_error(
                fmt::format("OTTERBRIX_GROUP_AGG_GPU_STRICT=1: grouped aggregate GPU {} is unavailable ({})",
                            operation,
                            reason));
        }

        std::string gpu_failure_reason() {
#if defined(__linux__)
            return gpu::opencl_runtime().failure_reason();
#else
            return "OpenCL GPU runtime is unavailable on this platform";
#endif
        }

    } // namespace

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

        const auto reason = gpu_failure_reason();

        if (gpu::gpu_group_aggregate_strict_enabled()) {
            throw_gpu_strict_error("path", reason);
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

        const auto reason = gpu_failure_reason();

        if (gpu::gpu_group_aggregate_strict_enabled()) {
            throw_gpu_strict_error("count(*) path", reason);
        }

        gpu::log_warn_if_available(fmt::format("grouped_aggregate GPU fallback to CPU: agg=count(*), reason={}", reason));
        update_count_star(group_ids, count, states);
    }

    void update_batch_gpu(const gpu_update_request_t* requests,
                          size_t request_count,
                          const uint32_t* group_ids,
                          uint64_t count) {
        if (request_count == 0 || count == 0) {
            return;
        }

        if (gpu::try_run_batch_gpu(requests, request_count, group_ids, count)) {
            gpu::log_info_if_available(
                fmt::format("grouped_aggregate GPU batch execution: requests={}, rows={}", request_count, count));
            return;
        }

        const auto reason = gpu_failure_reason();
        if (gpu::gpu_group_aggregate_strict_enabled()) {
            throw_gpu_strict_error("batch path", reason);
        }

        gpu::log_warn_if_available(fmt::format("grouped_aggregate GPU batch fallback to CPU: requests={}, reason={}",
                                               request_count,
                                               reason));

        for (size_t index = 0; index < request_count; index++) {
            const auto& request = requests[index];
            if (request.states == nullptr) {
                continue;
            }
            if (request.is_count_star) {
                update_count_star(group_ids, count, *request.states);
            } else if (request.vec != nullptr) {
                update_all(request.kind, *request.vec, group_ids, count, *request.states);
            }
        }
    }

    bool gpu_group_aggregate_test_enabled() { return gpu::read_bool_env("OTTERBRIX_GROUP_AGG_GPU_TEST"); }

} // namespace components::operators::aggregate

#pragma once

#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <core/result_wrapper.hpp>

#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Every refusable step precedes the one mutating step (mirroring into
    // function_registry_t::get_default()), since the catalog write it depends on can still refuse.
    class operator_register_udf_t final : public read_only_operator_t {
    public:
        // One non-invalid, mutually-equal uid per executor on success; empty when there are none.
        using executor_uids_t = std::pmr::vector<components::compute::function_uid>;

        operator_register_udf_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::compute::function_ptr function,
                                executor_uids_t executor_uids);

        // True iff EVERY executor registered AND the pg_proc/pg_depend rows were appended.
        bool success() const noexcept { return success_; }

        // Sourceless SINK leaf: all work runs in the single await_async_and_resume the dispatcher drives directly.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::compute::function_ptr function_;
        executor_uids_t executor_uids_;
        bool success_{false};
    };

} // namespace components::operators

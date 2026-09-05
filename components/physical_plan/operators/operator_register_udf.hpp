#pragma once

#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <core/result_wrapper.hpp>

#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Operator implementation of manager_dispatcher_t::register_udf.
    //
    // Executor fan-out is done by the DISPATCHER (owns the executor addresses/scheduler), which co_awaits
    // each register_udf send and hands this operator a plain pre-collected uid vector — keeping
    // std::function/std::shared_ptr out of the operator.
    //
    // Every step that can refuse comes before the one step that mutates (5), so a refusal leaves the
    // process untouched: 1) resolve_function_by_name (cross-namespace conflict). 2) validate the
    // pre-collected per-executor uids agree. 3) allocate the pg_proc OID. 4) write pg_proc + pg_depend.
    // 5) mirror into function_registry_t::get_default() — LAST because step 4's catalog read can still
    // refuse, and mirroring first would answer for a function the catalog has no row for.
    //
    // The function payload is owned here as the canonical function_ptr (unique): the operator deep-copies it
    // via get_copy() for the default-registry mirror and reads name()/get_signatures() for the pg_proc
    // encode step.
    class operator_register_udf_t final : public read_only_operator_t {
    public:
        // Pre-collected per-executor registration uids gathered by the dispatcher.
        // One non-invalid, mutually-equal uid per executor on success; an empty
        // vector when there are no executors to mirror by uid.
        using executor_uids_t = std::pmr::vector<components::compute::function_uid>;

        operator_register_udf_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::compute::function_ptr function,
                                executor_uids_t executor_uids);

        // True iff the registration succeeded across every executor and the
        // pg_proc/pg_depend rows were appended. Caller (dispatcher) reads this
        // to fulfil the bool unique_future<> the public API exposes.
        bool success() const noexcept { return success_; }

        // Sourceless SINK leaf (no data pipeline, no children): all work — the
        // cross-namespace conflict read, the default-registry mirror and the
        // pg_proc/pg_depend writes — runs in await_async_and_resume. The dispatcher
        // drives this operator's async finalize directly (a single
        // await_async_and_resume).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::compute::function_ptr function_;
        executor_uids_t executor_uids_;
        bool success_{false};
    };

} // namespace components::operators
